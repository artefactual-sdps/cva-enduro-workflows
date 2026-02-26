package tasks

import (
	"time"

	"github.com/artefactual-sdps/cva-enduro-workflows/internal/enums"
)

type Task struct {
	Name        string
	Outcome     enums.TaskOutcome
	Message     string
	StartedAt   time.Time
	CompletedAt time.Time
}

// New returns a Task with the given name and start time.
func New(start time.Time, name string) *Task {
	return &Task{
		Name:      name,
		StartedAt: start,
	}
}

// Complete records the task completion time, outcome and message.
func (t *Task) Complete(end time.Time, outcome enums.TaskOutcome, msg string) {
	t.Outcome = outcome
	t.Message = msg
	t.CompletedAt = end
}
