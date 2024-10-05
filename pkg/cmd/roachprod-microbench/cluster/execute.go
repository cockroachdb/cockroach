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

	"github.com/cockroachdb/cockroach/pkg/roachprod"
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
	Stdout     string
	Stderr     string
	Err        error
	ExitStatus int
	Duration   time.Duration
}

func remoteWorker(
	ctx context.Context,
	log *logger.Logger,
	clusterNode string,
	receive chan []RemoteCommand,
	response chan RemoteResponse,
) {
	for {
		commands := <-receive
		if commands == nil {
			return
		}
		for index, command := range commands {
			if errors.Is(ctx.Err(), context.Canceled) {
				for _, cancelCommand := range commands[index:] {
					response <- RemoteResponse{cancelCommand, "", "", nil, -1, -1}
				}
				break
			}
			start := timeutil.Now()
			runResult, err := roachprod.RunWithDetails(
				context.Background(), log, clusterNode, "" /* SSHOptions */, "", /* processTag */
				false /* secure */, command.Args, install.RunOptions{},
			)
			duration := timeutil.Since(start)
			var stdout, stderr string
			var exitStatus int
			if len(runResult) > 0 {
				stdout = runResult[0].Stdout
				stderr = runResult[0].Stderr
				exitStatus = runResult[0].RemoteExitStatus
				err = errors.CombineErrors(err, runResult[0].Err)
			}
			response <- RemoteResponse{command, stdout, stderr, err, exitStatus, duration}
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
	cluster string,
	commandGroups [][]RemoteCommand,
	numNodes int,
	failFast bool,
	callback func(response RemoteResponse),
) {
	workChannel := make(chan []RemoteCommand, numNodes)
	responseChannel := make(chan RemoteResponse, numNodes)

	ctx, cancelCtx := context.WithCancel(context.Background())

	for idx := 1; idx <= numNodes; idx++ {
		go remoteWorker(ctx, log, fmt.Sprintf("%s:%d", cluster, idx), workChannel, responseChannel)
	}

	var wg sync.WaitGroup
	// Receive responses.
	go func() {
		for {
			response := <-responseChannel
			if response.Args == nil {
				return
			}
			if response.Duration != -1 {
				callback(response)
			}
			if response.Err != nil && failFast {
				cancelCtx()
			}
			wg.Done()
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

	wg.Wait()

	close(workChannel)
	close(responseChannel)
}
