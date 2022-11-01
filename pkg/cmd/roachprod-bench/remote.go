package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod"
)

type RemoteCommand struct {
	command  []string
	metadata interface{}
}

type RemoteResponse struct {
	command  []string
	metadata interface{}
	stdout   string
	stderr   string
	err      error
	duration time.Duration
}

func roachprodRunWithOutput(clusterName string, cmdArray []string) (string, string, error) {
	bufOut, bufErr := new(bytes.Buffer), new(bytes.Buffer)
	var stdout, stderr io.Writer
	if *flagVerbose {
		stdout = io.MultiWriter(os.Stdout, bufOut)
		stderr = io.MultiWriter(os.Stderr, bufErr)
	} else {
		stdout = bufOut
		stderr = bufErr
	}
	err := roachprod.Run(context.Background(), l, clusterName, "", "", false, stdout, stderr, cmdArray)
	return bufOut.String(), bufErr.String(), err
}

func remoteWorker(clusterNode string, receive chan RemoteCommand, response chan RemoteResponse) {
	for {
		command := <-receive
		if command.command == nil {
			return
		}
		start := time.Now()
		stdout, stderr, err := roachprodRunWithOutput(clusterNode, command.command)
		duration := time.Since(start)
		response <- RemoteResponse{command.command, command.metadata, stdout, stderr, err, duration}
	}
}

func executeRemoteCommands(cluster string, commands []RemoteCommand, numNodes int, failFast bool, callback func(response RemoteResponse)) {
	workChannel := make(chan RemoteCommand, numNodes)
	responseChannel := make(chan RemoteResponse, numNodes)

	for idx := 1; idx <= numNodes; idx++ {
		go remoteWorker(fmt.Sprintf("%s:%d", cluster, idx), workChannel, responseChannel)
	}

	responsesRemaining := 0
out:
	for _, command := range commands {
		workChannel <- command
		responsesRemaining++

		select {
		case response := <-responseChannel:
			responsesRemaining--
			callback(response)
			if response.err != nil && failFast {
				break out
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
