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
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// RemoteCommand is a command to be executed on a remote node. The Metadata field is used to
// store additional information from the original caller.
type RemoteCommand struct {
	Args     []string
	Metadata interface{}
}

// RemoteResponse is the response to a RemoteCommand.
type RemoteResponse struct {
	RemoteCommand
	Stdout   string
	Stderr   string
	Err      error
	Duration time.Duration
}

func roachprodRunWithOutput(
	log *logger.Logger, clusterName string, cmdArray []string,
) (string, string, error) {
	bufOut, bufErr := new(bytes.Buffer), new(bytes.Buffer)
	err := roachprod.Run(context.Background(), log, clusterName, "", "", false, bufOut, bufErr, cmdArray)
	return bufOut.String(), bufErr.String(), err
}

func remoteWorker(
	log *logger.Logger, clusterNode string, receive chan RemoteCommand, response chan RemoteResponse,
) {
	for {
		command := <-receive
		if command.Args == nil {
			return
		}
		start := timeutil.Now()
		stdout, stderr, err := roachprodRunWithOutput(log, clusterNode, command.Args)
		duration := timeutil.Since(start)
		response <- RemoteResponse{command, stdout, stderr, err, duration}
	}
}

// ExecuteRemoteCommands distributes the commands to the cluster nodes and waits for the responses.
// Only one command is executed per node at a time. The commands are executed in the order they are
// provided. The failFast parameter indicates whether the execution should stop on the first error.
func ExecuteRemoteCommands(
	log *logger.Logger,
	cluster string,
	commands []RemoteCommand,
	numNodes int,
	failFast bool,
	callback func(response RemoteResponse),
) {
	workChannel := make(chan RemoteCommand, numNodes)
	responseChannel := make(chan RemoteResponse, numNodes)

	for idx := 1; idx <= numNodes; idx++ {
		go remoteWorker(log, fmt.Sprintf("%s:%d", cluster, idx), workChannel, responseChannel)
	}

	responsesRemaining := 0
outer:
	for _, command := range commands {
		workChannel <- command
		responsesRemaining++

		select {
		case response := <-responseChannel:
			responsesRemaining--
			callback(response)
			if response.Err != nil && failFast {
				break outer
			}
		default:
		}
	}

	for responsesRemaining > 0 {
		response := <-responseChannel
		responsesRemaining--
		callback(response)
	}

	close(workChannel)
	close(responseChannel)
}
