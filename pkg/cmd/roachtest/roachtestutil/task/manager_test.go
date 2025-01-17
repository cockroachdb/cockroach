// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package task

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPanicHandler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	m := NewManager(ctx, nilLogger())

	panicErr := errors.New("panic")
	panicHandlerFn := func(_ context.Context, n string, l *logger.Logger, r interface{}) (err error) {
		return r.(error)
	}
	m.Go(func(ctx context.Context, l *logger.Logger) error {
		panic(panicErr)
	}, PanicHandler(panicHandlerFn))

	e := <-m.CompletedEvents()
	require.ErrorIs(t, e.Err, panicErr)
	require.Equal(t, "task-1", e.Name)

	m.Terminate(nilLogger())
}

func TestErrorHandler(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	m := NewManager(ctx, nilLogger())

	var wrapErr error
	errorHandlerFn := func(_ context.Context, n string, l *logger.Logger, err error) error {
		wrapErr = errors.Wrapf(err, "wrapped")
		return wrapErr
	}

	m.Go(func(ctx context.Context, l *logger.Logger) error {
		return errors.New("error")
	}, ErrorHandler(errorHandlerFn), Name("def"))

	e := <-m.CompletedEvents()
	require.ErrorIs(t, e.Err, wrapErr)
	require.Equal(t, "def", e.Name)
	m.Terminate(nilLogger())
}

func TestContextCancel(t *testing.T) {
	t.Run("cancel main context", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		ctx, cancel := context.WithCancel(context.Background())
		m := NewManager(ctx, nilLogger())

		wg := sync.WaitGroup{}
		wg.Add(1)
		m.Go(func(ctx context.Context, l *logger.Logger) error {
			defer wg.Done()
			<-ctx.Done()
			return ctx.Err()
		})
		cancel()
		wg.Wait()
		m.Terminate(nilLogger())
	})

	t.Run("cancel task context", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		ctx := context.Background()
		m := NewManager(ctx, nilLogger())

		wg := sync.WaitGroup{}
		wg.Add(1)
		cancel := m.GoWithCancel(func(ctx context.Context, l *logger.Logger) error {
			defer wg.Done()
			<-ctx.Done()
			return ctx.Err()
		})
		cancel()
		wg.Wait()

		e := <-m.CompletedEvents()
		require.ErrorIs(t, e.Err, context.Canceled)
		m.Terminate(nilLogger())
	})
}

func TestTerminate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	m := NewManager(ctx, nilLogger())
	numTasks := 10
	var counter atomic.Uint32
	for i := 0; i < numTasks; i++ {
		m.Go(func(ctx context.Context, l *logger.Logger) error {
			defer func() {
				counter.Add(1)
			}()
			<-ctx.Done()
			return nil
		})
	}
	go func() {
		for i := 0; i < numTasks; i++ {
			e := <-m.CompletedEvents()
			require.NoError(t, e.Err)
		}
	}()
	m.Terminate(nilLogger())
	require.Equal(t, uint32(numTasks), counter.Load())
}

func TestGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	m := NewManager(ctx, nilLogger())

	numTasks := 10
	g := m.NewGroup()
	channels := make([]chan struct{}, numTasks)

	// Start tasks.
	for i := 0; i < numTasks; i++ {
		channels[i] = make(chan struct{})
		g.Go(func(ctx context.Context, l *logger.Logger) error {
			<-channels[i]
			return nil
		})
	}

	// Start a goroutine that waits for all tasks in the group to complete.
	done := make(chan struct{})
	go func() {
		g.Wait()
		close(done)
	}()

	// Close channels one by one to complete all tasks, and ensure the group is
	// not done yet.
	for i := 0; i < numTasks; i++ {
		select {
		case <-done:
			t.Fatal("group should not be done yet")
		default:
		}
		// Close the channel and wait for the completed event.
		close(channels[i])
		<-m.CompletedEvents()
	}

	// Ensure the group is done.
	<-done
	m.Terminate(nilLogger())
}

func TestTerminateGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	m := NewManager(ctx, nilLogger())

	numTasks := 3
	g := m.NewGroup()

	// Start tasks.
	for i := 0; i < numTasks; i++ {
		g.Go(func(ctx context.Context, l *logger.Logger) error {
			<-ctx.Done()
			return nil
		})
	}

	// Start a goroutine that waits for all tasks in the group to complete.
	done := make(chan struct{})
	go func() {
		g.Wait()
		close(done)
	}()

	// Consume all completed events.
	go func() {
		for i := 0; i < numTasks; i++ {
			e := <-m.CompletedEvents()
			require.NoError(t, e.Err)
		}
	}()

	m.Terminate(nilLogger())
	<-done
}

func TestErrorGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	m := NewManager(ctx, nilLogger())
	g := m.NewErrorGroup()

	err := errors.New("test error")
	g.Go(func(ctx context.Context, l *logger.Logger) error {
		return err
	})

	actualErr := g.WaitE()
	require.ErrorIs(t, actualErr, err)

	select {
	case <-m.CompletedEvents():
		t.Fatal("expected no events (errors handled by WaitE)")
	default:
	}

	m.Terminate(nilLogger())
}

func nilLogger() *logger.Logger {
	lcfg := logger.Config{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	l, err := lcfg.NewLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}
