package cbcli

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/codingbeard/cbutil"
	"github.com/robfig/cron/v3"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

type Task interface {
	GetGroup() string
	GetName() string
	Run() error
}

type ScheduledTask interface {
	GetSchedule() string
}

type GoroutineConfigurableTask interface {
	ExecuteInGoroutine() bool
}

type ErrorAfterDurationTask interface {
	GetErrorAfterDuration() time.Duration
}

type Logger interface {
	InfoF(category string, message string, args ...interface{})
	Write(message []byte) (n int, e error)
}

type defaultLogger struct{}

func (d defaultLogger) Write(p []byte) (n int, err error) {
	d.InfoF("ERROR", string(p))
	return 0, nil
}

func (d defaultLogger) InfoF(category string, message string, args ...interface{}) {
	log.Println(category+":", fmt.Sprintf(message, args...))
}

type ErrorHandler interface {
	Error(e error)
	Recover()
}

type defaultErrorHandler struct{}

func (d defaultErrorHandler) Error(e error) {
	buf := make([]byte, 1000000)
	runtime.Stack(buf, false)
	buf = bytes.Trim(buf, "\x00")
	stack := string(buf)
	stackParts := strings.Split(stack, "\n")
	newStackParts := []string{stackParts[0]}
	newStackParts = append(newStackParts, stackParts[3:]...)
	stack = strings.Join(newStackParts, "\n")
	log.Println("ERROR", e.Error()+"\n"+stack)
}

func (d defaultErrorHandler) Recover() {
	e := recover()
	if err, ok := e.(error); ok {
		d.Error(err)
	} else {
		d.Error(errors.New(fmt.Sprint(e)))
	}
}

type Config interface {
	/**
	first return type is whether the config is enabled
	second return type is an error when the config path is not defined
	the path passed in will be in the format: cbcli.group.name
	where group and name are replaced with the task's returned values
	*/
	GetRequiredBool(path string) (bool, error)
}

var TaskNotFound = errors.New("task not found")

type TaskContainer struct {
	tasks        []Task
	logger       Logger
	errors       ErrorHandler
	config       Config
	dispatchEnvs []string
}

func New() *TaskContainer {
	return &TaskContainer{
		logger: defaultLogger{},
		errors: defaultErrorHandler{},
	}
}

func (t *TaskContainer) AddTask(task Task) {
	t.tasks = append(t.tasks, task)
}

func (t *TaskContainer) SetLogger(logger Logger) {
	t.logger = logger
}

func (t *TaskContainer) SetErrorHandler(handler ErrorHandler) {
	t.errors = handler
}

func (t *TaskContainer) SetConfig(config Config) {
	t.config = config
}

func (t *TaskContainer) SetDispatchEnvironment(envs []string) {
	t.dispatchEnvs = envs
}

func (t *TaskContainer) Execute() error {
	if len(os.Args) > 2 && os.Args != nil {
		group := os.Args[1]
		name := os.Args[2]

		if t.config != nil {
			enabled, e := t.config.GetRequiredBool(fmt.Sprintf("cbcli.%s.%s", group, name))
			if e == nil && !enabled {
				e = fmt.Errorf("Task %s:%s is not enabled", group, name)
				t.logger.InfoF("CLI", e.Error())
				return e
			}
		}

		e := t.RunTask(group, name)
		if e != nil {
			if errors.Is(e, TaskNotFound) {
				t.logger.InfoF("CLI", "Task %s:%s not found", group, name)
			} else {
				t.errors.Error(e)
				return e
			}
		}
	} else {
		t.logger.InfoF("CLI", "Not enough arguments, expecting: taskGroup taskName")
	}

	return nil
}

func (t *TaskContainer) RunTask(group, name string) error {
	for _, task := range t.tasks {
		if task.GetGroup() == group && task.GetName() == name {

			trueVariable := true
			falseVariable := false
			var running *bool
			if errorAfterTask, ok := task.(ErrorAfterDurationTask); ok {
				go func() {
					elapsed := time.Duration(0)
					for true {
						cbutil.Sleep(time.Second)
						elapsed += time.Second

						if elapsed >= errorAfterTask.GetErrorAfterDuration() {
							if *running {
								t.errors.Error(fmt.Errorf(
									"task still running after expected duration: %s:%s %ds",
									task.GetGroup(),
									task.GetName(),
									int(errorAfterTask.GetErrorAfterDuration()/time.Second),
								))
							}
							break
						}
					}
				}()
			}

			t.logger.InfoF("CLI", "Running task (%s:%s)", task.GetGroup(), task.GetName())
			running = &trueVariable
			e := task.Run()
			running = &falseVariable
			t.logger.InfoF("CLI", "Finished running task (%s:%s)", task.GetGroup(), task.GetName())
			return e
		}
	}

	return TaskNotFound
}

func (t *TaskContainer) DispatchTasks() {
	crontab := cron.New()

	for taskKey := range t.tasks {
		task := t.tasks[taskKey]
		scheduledTask, isScheduled := task.(ScheduledTask)
		if !isScheduled {
			continue
		}
		if scheduledTask.GetSchedule() == "manual" || scheduledTask.GetSchedule() == "" {
			continue
		}
		if t.config != nil {
			enabled, e := t.config.GetRequiredBool(fmt.Sprintf("cbcli.%s.%s", task.GetGroup(), task.GetName()))
			if e == nil && !enabled {
				continue
			}
		}
		_, e := crontab.AddFunc(scheduledTask.GetSchedule(), func() {
			t.logger.InfoF("CLI", "Dispatching task (%s:%s)", task.GetGroup(), task.GetName())
			isGoroutineTask := false
			goroutineTask, isGoroutineConfigurableTask := task.(GoroutineConfigurableTask)
			if isGoroutineConfigurableTask {
				isGoroutineTask = goroutineTask.ExecuteInGoroutine()
			}
			if isGoroutineTask {
				go func() {
					t.errors.Recover()
					e := t.RunTask(task.GetGroup(), task.GetName())
					if e != nil {
						t.errors.Error(e)
					}
				}()
			} else {
				executable, e := os.Executable()
				if e != nil {
					t.errors.Error(e)
				}
				cmd := exec.Command(executable, task.GetGroup(), task.GetName())
				cmd.Env = t.dispatchEnvs
				cmd.Stderr = t.logger
				cmd.Stderr = t.logger
				e = cmd.Run()
				if e != nil {
					t.errors.Error(e)
				}
			}
		})

		if e != nil {
			t.errors.Error(e)
		}
	}

	crontab.Start()
}
