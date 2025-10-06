// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/sync/errgroup"
)

type Spinner struct {
	msgTemplate  syncutil.AtomicString
	startMsg     string
	tickCallback func()
	quiet, term  bool
	out          io.Writer
	waitGroup    sync.WaitGroup
	stopChan     chan struct{}
	started      bool
	mu           syncutil.Mutex
}

type CountingSpinner struct {
	*Spinner
	count         int32
	expectedTotal int
}

type TaskSpinner struct {
	*Spinner
	tasks map[interface{}]struct {
		msg  string
		done bool
	}
	taskOrder []interface{}
	tasksMu   syncutil.Mutex
}

type SpinnerGroup struct {
	errgroup.Group
	spinner   *CountingSpinner
	completed int32
}

var spinnerLoop = []string{"|", "/", "-", "\\"}

const spinnerTemplate = "{%}"

// NewSpinner creates a new Spinner. If quiet is true, the spinner will only
// print the message and not the spinner itself, but will still print dots every
// second to indicate progress. If term is false the spinner will not print dots
// to indicate progress (usually false when the output is a file). The msg
// parameter is the initial message of the spinner and can be left empty if no
// initial message is desired.
func NewSpinner(msg string, out io.Writer, quiet, term bool) *Spinner {
	msgTemplate := syncutil.AtomicString{}
	if msg != "" {
		msgTemplate.Set(msg + " " + spinnerTemplate)
	}
	return &Spinner{
		msgTemplate: msgTemplate,
		startMsg:    msg,
		quiet:       quiet,
		term:        term,
		out:         out,
		stopChan:    make(chan struct{}),
	}
}

// NewDefaultSpinner creates a new Spinner with the default configuration. It
// will use the quiet settings from the roachprod config package and use the
// logger's stdout, and file to determine if term should be true.
func NewDefaultSpinner(l *logger.Logger, msg string) *Spinner {
	return NewSpinner(msg, l.Stdout, config.Quiet, l.File == nil)
}

// SetTickCallback sets a callback function that will be called every time
// before the spinner ticks. This can be used to update the message of the
// spinner before it ticks.
func (s *Spinner) SetTickCallback(cb func()) {
	s.tickCallback = cb
}

func (s *Spinner) renderTemplate(spinnerIdx int, done bool) string {
	msg := s.msgTemplate.Get()
	spinner := ""
	if !done {
		spinner = spinnerLoop[spinnerIdx%len(spinnerLoop)]
	}
	return strings.Replace(msg, spinnerTemplate, spinner, -1)
}

// Start starts the spinner. It returns a function that can be called to stop
// the spinner. If the spinner is already started, it does nothing.
func (s *Spinner) Start() func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return s.Stop
	}
	s.started = true

	if s.startMsg != "" && s.quiet {
		fmt.Fprintf(s.out, "%s", s.startMsg)
		// Print a newline since we won't be printing dots.
		if !s.term {
			fmt.Fprintf(s.out, "\n")
		}
	}
	go func() {
		defer s.waitGroup.Done()
		s.waitGroup.Add(1)

		var writer Writer
		tickerDuration := 100 * time.Millisecond
		if s.quiet {
			tickerDuration = 1000 * time.Millisecond
		}
		ticker := time.NewTicker(tickerDuration)
		defer ticker.Stop()
		done := false
		spinnerIdx := 0

		for !done {
			select {
			case <-ticker.C:
				if s.tickCallback != nil {
					s.tickCallback()
				}
				if s.quiet && s.term {
					fmt.Fprintf(s.out, ".")
				}
			case <-s.stopChan:
				done = true
			}
			if !s.quiet {
				fmt.Fprint(&writer, s.renderTemplate(spinnerIdx, done))
				fmt.Fprintf(&writer, "\n")
				_ = writer.Flush(s.out)
				spinnerIdx++
			}
		}
		// This newline is only required if we have been printing dots.
		if s.quiet && s.term {
			fmt.Fprintf(s.out, "\n")
		}
	}()
	return s.Stop
}

// Stop stops the spinner. If the spinner is not started, it does nothing.
func (s *Spinner) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.started {
		return
	}
	close(s.stopChan)
	s.waitGroup.Wait()
	s.stopChan = make(chan struct{})
	s.waitGroup = sync.WaitGroup{}
}

// Status sets the message of the spinner atomically. If the spinner is started,
// the updated message will be displayed on the next tick. In quiet mode, this
// will not have any effect and only the initial message will be displayed.
func (s *Spinner) Status(msg string) {
	msg = msg + " " + spinnerTemplate
	s.statusTemplate(msg)
}

// statusTemplate sets the internal template message of the spinner atomically.
func (s *Spinner) statusTemplate(msg string) {
	s.msgTemplate.Set(msg)
}

// NewCountingSpinner creates a new CountingSpinner. It is a wrapper around
// Spinner, but also keeps count of completed items and displays it in the
// message. The expectedTotal parameter is the total number of expected items
// that will be processed.
func NewCountingSpinner(spinner *Spinner, expectedTotal int) *CountingSpinner {
	return &CountingSpinner{
		Spinner:       spinner,
		expectedTotal: expectedTotal,
	}
}

// NewDefaultCountingSpinner creates a new CountingSpinner with the default
// configuration. See NewDefaultSpinner for more information.
func NewDefaultCountingSpinner(l *logger.Logger, msg string, expectedTotal int) *CountingSpinner {
	return NewCountingSpinner(NewDefaultSpinner(l, msg), expectedTotal)
}

// Start starts the embedded spinner, but updates the initial message to include
// the count of completed items. It returns a function that can be called to
// stop the spinner.
func (s *CountingSpinner) Start() func() {
	if !s.quiet {
		s.CountStatus(0)
	}
	return s.Spinner.Start()
}

// updateStatus sets the embedded spinner's message to the base message with the
// count appended to it.
func (s *CountingSpinner) updateStatus() {
	count := atomic.LoadInt32(&s.count)
	s.Spinner.Status(fmt.Sprintf("%s %d/%d", s.startMsg, count, s.expectedTotal))
}

// CountStatus sets the number of completed items atomically. This will update
// the message of the embedded spinner to reflect the new count.
func (s *CountingSpinner) CountStatus(count int) {
	atomic.StoreInt32(&s.count, int32(count))
	s.updateStatus()
}

// NewSpinnerGroup creates a new SpinnerGroup from the given CountingSpinner.
// SpinnerGroup is a wrapper around an `errgroup.Group` that also keeps track
// of the number of completed tasks and displays it in the spinner.
func NewSpinnerGroup(spinner *CountingSpinner) *SpinnerGroup {
	return &SpinnerGroup{
		spinner: spinner,
	}
}

// NewDefaultSpinnerGroup creates a new SpinnerGroup with the default configuration.
func NewDefaultSpinnerGroup(l *logger.Logger, msg string, expectedTotal int) *SpinnerGroup {
	return NewSpinnerGroup(NewDefaultCountingSpinner(l, msg, expectedTotal))
}

// Go adds a new task to the SpinnerGroup. The task should be a function that
// returns an error. The task will be executed in a separate goroutine and the
// SpinnerGroup will keep track of the number of completed tasks.
func (g *SpinnerGroup) Go(f func() error) {
	g.Group.Go(func() error {
		defer func() {
			atomic.AddInt32(&g.completed, 1)
		}()
		return f()
	})
}

// Wait waits for all tasks in the SpinnerGroup to complete. It will update the
// status of the embedded counting spinner to reflect the number of completed
// tasks.
func (g *SpinnerGroup) Wait() error {
	updateStatus := func() {
		count := atomic.LoadInt32(&g.completed)
		if !g.spinner.quiet {
			g.spinner.CountStatus(int(count))
		}
	}
	g.spinner.SetTickCallback(updateStatus)

	defer g.spinner.Start()()
	err := g.Group.Wait()
	updateStatus()
	return err
}

// NewTaskSpinner creates a new TaskSpinner. It is a wrapper around Spinner that
// keeps track of the status of multiple tasks.
func NewTaskSpinner(spinner *Spinner) *TaskSpinner {
	return &TaskSpinner{
		Spinner: spinner,
		tasks: make(map[interface{}]struct {
			msg  string
			done bool
		}),
	}
}

// NewDefaultTaskSpinner creates a new TaskSpinner with the default configuration.
func NewDefaultTaskSpinner(l *logger.Logger, msg string) *TaskSpinner {
	return NewTaskSpinner(NewDefaultSpinner(l, msg))
}

func (t *TaskSpinner) TaskStatus(taskKey interface{}, msg string, done bool) {
	t.tasksMu.Lock()
	defer t.tasksMu.Unlock()
	if _, ok := t.tasks[taskKey]; !ok {
		t.taskOrder = append(t.taskOrder, taskKey)
	}
	t.tasks[taskKey] = struct {
		msg  string
		done bool
	}{
		msg:  msg,
		done: done,
	}
	t.updateStatus()
}

func (t *TaskSpinner) updateStatus() {
	var status strings.Builder
	if t.startMsg != "" {
		status.WriteString(t.startMsg + "\n")
	}
	for idx, taskKey := range t.taskOrder {
		if idx > 0 {
			status.WriteString("\n")
		}
		status.WriteString(t.tasks[taskKey].msg)
		if !t.tasks[taskKey].done {
			status.WriteString(" " + spinnerTemplate)
		}
	}
	t.Spinner.statusTemplate(status.String())
}

// MaybeLogTasks logs the status of all tasks if the spinner is in quiet mode
// and not in a terminal.
func (t *TaskSpinner) MaybeLogTasks(l *logger.Logger) {
	t.tasksMu.Lock()
	defer t.tasksMu.Unlock()
	if t.quiet && !t.term {
		for _, taskKey := range t.taskOrder {
			l.Printf("%s\n", t.tasks[taskKey].msg)
		}
	}
}
