package cbcli

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/robfig/cron/v3"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

type Task interface {
	GetSchedule() string
	GetGroup() string
	GetName() string
	Run() error
}

type Logger interface {
	InfoF(category string, message string, args ...interface{})
}

type defaultLogger struct{}

func (d defaultLogger) InfoF(category string, message string, args ...interface{}) {
	log.Println(category+":", fmt.Sprintf(message, args...))
}

type ErrorHandler interface {
	Error(e error)
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

var TaskNotFound = errors.New("task not found")

type TaskContainer struct {
	tasks  []Task
	logger Logger
	errors ErrorHandler
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

func (t *TaskContainer) Execute() error {
	if len(os.Args) > 2 && os.Args != nil {
		group := os.Args[1]
		name := os.Args[2]

		e := t.RunTask(group, name)
		if e != nil {
			if errors.Is(e, TaskNotFound) {
				t.logger.InfoF("CLI", "Task %s:%s not found", group, name)
			} else {
				t.errors.Error(e)
			}
			os.Exit(1)
		}
		os.Exit(0)
	} else {
		t.logger.InfoF("CLI", "Not enough arguments, expecting: taskGroup taskName")
	}

	return nil
}

func (t *TaskContainer) RunTask(group, name string) error {
	for _, task := range t.tasks {
		if task.GetGroup() == group && task.GetName() == name {
			t.logger.InfoF("CLI", "Running task (%s:%s)", task.GetGroup(), task.GetName())
			e := task.Run()
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
		if task.GetSchedule() == "manual" || task.GetSchedule() == "" {
			continue
		}
		_, e := crontab.AddFunc(task.GetSchedule(), func() {
			t.logger.InfoF("CLI", "Dispatching task (%s:%s)", task.GetGroup(), task.GetName())
			executable, e := os.Executable()
			if e != nil {
				t.errors.Error(e)
			}
			e = exec.Command(executable, "start-task", task.GetGroup(), task.GetName()).Run()
			if e != nil {
				t.errors.Error(e)
			}
		})

		if e != nil {
			t.errors.Error(e)
		}
	}

	crontab.Start()
}
