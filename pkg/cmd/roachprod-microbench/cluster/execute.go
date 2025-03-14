// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// RemoteCommand is a command to be executed on a remote node. The Metadata field is used to
// store additional information from the original caller.
type RemoteCommand struct {
	Args     []string
	GroupID  string // GroupID identifies commands that are part of the same logical group
	Metadata any
}

// RemoteResponse is the response to a RemoteCommand.
// A Duration of -1 indicates that the command was cancelled.
type RemoteResponse struct {
	RemoteCommand
	Stdout        string
	Stderr        string
	Err           error
	ExitStatus    int
	Duration      time.Duration
	commandStatus CommandStatus
}

type CommandStatus int8

const (
	Completed CommandStatus = iota
	Cancelled
	Terminated
)

type CancelledGroups struct {
	*syncutil.Map[string, chan struct{}]
}

var ErrAllNodesTerminated = errors.New("all nodes terminated")

type RemoteExecutionFunc func(
	ctx context.Context,
	l *logger.Logger,
	clusterName, SSHOptions, processTag string,
	secure bool,
	cmdArray []string,
	options install.RunOptions,
) ([]install.RunResultDetails, error)

func NewCancelledGroups() *CancelledGroups {
	return &CancelledGroups{Map: &syncutil.Map[string, chan struct{}]{}}
}

// getChannel lazily creates a channel for a groupID.
func (c *CancelledGroups) getChannel(groupID string) chan struct{} {
	ch := make(chan struct{})
	val, _ := c.LoadOrStore(groupID, &ch)
	return *val
}

// withGroupCancellation returns a new context that is cancelled when either:
// 1. The parent context is cancelled
// 2. The group's channel is closed
// If groupID is empty, returns the parent context unchanged.
func (c *CancelledGroups) withGroupCancellation(
	ctx context.Context, groupID string,
) context.Context {
	if groupID == "" {
		return ctx
	}
	groupCh := c.getChannel(groupID)
	cmdCtx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-groupCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return cmdCtx
}

// cancelGroup closes the channel for a groupID if it is not already closed.
func (c *CancelledGroups) cancelGroup(groupID string) {
	ch := c.getChannel(groupID)
	select {
	case <-ch:
		// Channel already closed
	default:
		close(ch)
	}
}

// isCancelled checks if the channel for a groupID is closed.
func (c *CancelledGroups) isCancelled(groupID string) bool {
	ch := c.getChannel(groupID)
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// remoteWorker is a worker that executes commands on a remote node.
func remoteWorker(
	ctx context.Context,
	log *logger.Logger,
	execFunc RemoteExecutionFunc,
	clusterNode string,
	runOptions install.RunOptions,
	workChan chan []RemoteCommand,
	responseChan chan RemoteResponse,
	cancelledGroups *CancelledGroups,
) {
	for {
		commands := <-workChan
		if commands == nil {
			return
		}
		for index, command := range commands {
			if errors.Is(ctx.Err(), context.Canceled) {
				for _, cancelCommand := range commands[index:] {
					responseChan <- RemoteResponse{RemoteCommand: cancelCommand, commandStatus: Cancelled}
				}
				break
			}

			// Check if the command's group has already been cancelled
			if command.GroupID != "" {
				if cancelledGroups.isCancelled(command.GroupID) {
					responseChan <- RemoteResponse{RemoteCommand: command, commandStatus: Cancelled}
					continue
				}
			}

			start := timeutil.Now()

			// Create a context that's cancelled either by parent ctx or group cancellation
			cmdCtx := cancelledGroups.withGroupCancellation(ctx, command.GroupID)

			runResult, err := execFunc(
				cmdCtx, log, clusterNode, "" /* SSHOptions */, "", /* processTag */
				false /* secure */, command.Args, runOptions,
			)
			duration := timeutil.Since(start)

			// Check for context cancellation after execution
			if errors.Is(cmdCtx.Err(), context.Canceled) {
				responseChan <- RemoteResponse{RemoteCommand: command, commandStatus: Cancelled}
				continue
			}

			// Reschedule on another worker when a `roachprod` or SSH error occurs. A
			// `roachprod` error is when `result.Err == nil`.
			if len(runResult) > 0 {
				err = errors.CombineErrors(err, runResult[0].Err)
			}
			if rperrors.IsTransient(err) {
				if log != nil {
					log.Printf("SSH error executing command %v on %s: %v, terminating worker.", command.Args, clusterNode, err)
				}
				responseChan <- RemoteResponse{commandStatus: Terminated}
				// Requeue all the remaining commands, so that it can be executed by
				// another worker.
				commandGroup := commands[index:]
				workChan <- commandGroup
				return
			}

			var stdout, stderr string
			var exitStatus int
			if len(runResult) > 0 {
				stdout = runResult[0].Stdout
				stderr = runResult[0].Stderr
				exitStatus = runResult[0].RemoteExitStatus
			}

			response := RemoteResponse{
				command,
				stdout,
				stderr,
				err,
				exitStatus,
				duration,
				Completed,
			}

			responseChan <- response
		}
	}
}

// ExecuteRemoteCommands distributes the commands to the cluster nodes and waits
// for the responses. Commands can be grouped as a sub-array to be executed
// serially on the same node. Only one command is executed per node at a time.
// The commands are executed in the order they are provided. The failFast
// parameter indicates whether the execution should stop on the first error.
func ExecuteRemoteCommands(
	log *logger.Logger,
	execFunc RemoteExecutionFunc,
	cluster string,
	commandGroups [][]RemoteCommand,
	numNodes int,
	failFast bool,
	runOptions install.RunOptions,
	callback func(response RemoteResponse),
) error {
	workChannel := make(chan []RemoteCommand, numNodes)
	responseChannel := make(chan RemoteResponse, numNodes)
	cancelledGroups := NewCancelledGroups()

	ctx, cancelCtx := context.WithCancelCause(context.Background())

	for idx := 1; idx <= numNodes; idx++ {
		go remoteWorker(ctx, log, execFunc, fmt.Sprintf("%s:%d", cluster, idx),
			runOptions, workChannel, responseChannel, cancelledGroups)
	}

	var wg sync.WaitGroup
	// Receive responses.
	go func() {
		nodesAlive := numNodes
		for {
			response := <-responseChannel
			// Check if the channel has been closed.
			if response.Args == nil && response.commandStatus == Completed {
				return
			}
			switch response.commandStatus {
			case Completed:
				callback(response)
				if response.Err != nil {
					if failFast {
						cancelCtx(errors.Wrap(response.Err, "failed to execute command, cancelling execution"))
					}
					// If the command has a GroupID and the error is not transient,
					// mark all commands in this group as cancelled
					if response.GroupID != "" && !rperrors.IsTransient(response.Err) {
						cancelledGroups.cancelGroup(response.GroupID)
					}
				}
				wg.Done()
			case Cancelled:
				wg.Done()
			case Terminated:
				nodesAlive--
				if nodesAlive == 0 {
					// Cancel the context and drain the workChannel.
					cancelCtx(ErrAllNodesTerminated)
					go func() {
						for {
							commands := <-workChannel
							if commands == nil {
								return
							}
							for _, cancelCommand := range commands {
								responseChannel <- RemoteResponse{RemoteCommand: cancelCommand, commandStatus: Cancelled}
							}
						}
					}()
				}
			}
		}
	}()

	// Send commands to workers.
done:
	for _, commands := range commandGroups {
		wg.Add(len(commands))
	outer:
		for {
			select {
			case <-ctx.Done():
				wg.Add(-len(commands))
				break done
			case workChannel <- commands:
				break outer
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}

	wg.Wait()

	close(workChannel)
	close(responseChannel)

	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Cause(ctx)
	}
	return ctx.Err()
}
