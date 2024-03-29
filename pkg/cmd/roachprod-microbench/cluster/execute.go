// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cluster

import (
	"context"
	"fmt"
	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// RemoteCommand is a command to be executed on a remote node. The Metadata field is used to
// store additional information from the original caller.
type RemoteCommand struct {
	Args     []string
	Metadata interface{}
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

type RemoteExecutionFunc func(
	ctx context.Context,
	l *logger.Logger,
	clusterName, SSHOptions, processTag string,
	secure bool,
	cmdArray []string,
	options install.RunOptions,
) ([]install.RunResultDetails, error)

func remoteWorker(
	ctx context.Context,
	log *logger.Logger,
	execFunc RemoteExecutionFunc,
	clusterNode string,
	workChan chan []RemoteCommand,
	responseChan chan RemoteResponse,
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
			start := timeutil.Now()
			runResult, err := execFunc(
				context.Background(), log, clusterNode, "" /* SSHOptions */, "", /* processTag */
				false /* secure */, command.Args, install.DefaultRunOptions().WithRetryDisabled(),
			)
			duration := timeutil.Since(start)

			// Reschedule on another worker when a roachprod or SSH error occurs. A
			// roachprod error is when result.Err==nil.
			if err != nil && (runResult[0].Err == nil || errors.Is(err, rperrors.ErrSSH255)) {
				log.Printf("SSH error executing command %v on %s: %v, terminating worker.", command.Args, clusterNode, err)
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
				err = errors.CombineErrors(err, runResult[0].Err)
			}
			responseChan <- RemoteResponse{
				command,
				stdout,
				stderr,
				err,
				exitStatus,
				duration,
				Completed,
			}
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
	callback func(response RemoteResponse),
) error {
	workChannel := make(chan []RemoteCommand, numNodes)
	responseChannel := make(chan RemoteResponse, numNodes)

	ctx, cancelCtx := context.WithCancelCause(context.Background())

	for idx := 1; idx <= numNodes; idx++ {
		go remoteWorker(ctx, log, execFunc, fmt.Sprintf("%s:%d", cluster, idx), workChannel, responseChannel)
	}

	var wg sync.WaitGroup
	// Receive responses.
	go func() {
		nodesAlive := numNodes
		for {
			response := <-responseChannel
			if response.Args == nil && response.commandStatus == 0 { // channel closed
				return
			}
			switch response.commandStatus {
			case Completed:
				callback(response)
				wg.Done()
			case Cancelled:
				wg.Done()
			case Terminated:
				nodesAlive--
				if nodesAlive == 0 {
					cancelCtx(errors.New("all workers terminated"))
				}
			}
			if response.Err != nil && failFast {
				cancelCtx(errors.Wrap(response.Err, "failed to execute command, cancelling execution"))
			}
		}
	}()

	// Send commands to workers.
done:
	for _, commands := range commandGroups {
	outer:
		for {
			select {
			case <-ctx.Done():
				break done
			case workChannel <- commands:
				wg.Add(len(commands))
				break outer
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	// Wait for all commands to be executed. If the context is cancelled, the
	// workers will be terminated and the workChannel will be closed.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}

	close(workChannel)
	close(responseChannel)

	if errors.Is(ctx.Err(), context.Canceled) {
		return context.Cause(ctx)
	}
	return ctx.Err()
}
