// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync/atomic"
	"testing"
	"testing/quick"
	"time"

	rperrors "github.com/cockroachdb/cockroach/pkg/roachprod/errors"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestExecutionScheduling(t *testing.T) {
	rng, _ := randutil.NewTestRand()

	// `makeGen` generates random parameter values for the test.
	// The first parameter is the number of nodes in the cluster.
	// The second parameter is the number of commands to distribute and execute.
	// The third parameter is the number of node terminations to simulate. if
	// `terminateAll` is true, all nodes will be terminated.
	makeGen := func(terminateAll bool) func(values []reflect.Value, rng *rand.Rand) {
		return func(values []reflect.Value, rng *rand.Rand) {
			minNodes := 3
			numNodes := rng.Intn(3) + minNodes
			numCommands := rng.Intn(30) + 5
			numTerminates := rng.Intn(numNodes - 1)
			if terminateAll {
				numTerminates = numNodes
			}
			if numTerminates > numCommands {
				numTerminates = numCommands
			}
			values[0], values[1], values[2] =
				reflect.ValueOf(numNodes), reflect.ValueOf(numCommands), reflect.ValueOf(numTerminates)
		}
	}

	// `makeVerify` generates a function that verifies the correctness of the
	// execution scheduling. `expectAllNodesTerminatedError` is used to determine
	// if the test should expect an ErrAllNodesTerminated error, when all nodes
	// will be terminated.
	makeVerify := func(expectAllNodesTerminatedError bool) func(numNodes, numCommands, terminateCount int) bool {
		return func(numNodes, numCommands, terminateCount int) bool {
			// Determine random points at which to terminate the nodes.
			terminatePoints := make(map[int]struct{})
			for len(terminatePoints) < terminateCount {
				terminatePoints[rng.Intn(numCommands)+1] = struct{}{}
			}

			// `cmdIdx` is used to keep track of the current command index being
			// executed, in order to determine when to terminate the worker.
			cmdIdx := atomic.Int32{}

			// `execFunc` is a mock function that simulates the execution of a command.
			execFunc := func(
				ctx context.Context,
				l *logger.Logger,
				clusterName, SSHOptions, processTag string,
				secure bool,
				cmdArray []string,
				options install.RunOptions,
			) ([]install.RunResultDetails, error) {
				curVal := cmdIdx.Add(1)
				if _, ok := terminatePoints[int(curVal)]; ok {
					// Terminate the worker.
					return []install.RunResultDetails{
						{Err: rperrors.TransientError{}},
					}, nil
				}
				// Command succeeded.
				return []install.RunResultDetails{
					{},
				}, nil
			}

			// Generate a list of random commands to execute.
			commands := make([][]RemoteCommand, 0)
			addCommand := func(command RemoteCommand) {
				commands = append(commands, []RemoteCommand{command})
			}
			commandMap := make(map[string]struct{})
			for len(commandMap) < numCommands {
				commandName := randutil.RandString(rng, 8, randutil.PrintableKeyAlphabet)
				commandMap[commandName] = struct{}{}
				addCommand(RemoteCommand{Args: []string{commandName}})
			}

			// Execute the commands. The callback function will remove the executed
			// command from the `commandMap`, in order to keep track of the remaining
			// commands.
			err := ExecuteRemoteCommands(
				nil, execFunc, "test", commands, numNodes, false, install.DefaultRunOptions().WithRetryDisabled(),
				func(response RemoteResponse) {
					delete(commandMap, response.Args[0])
				},
			)

			// Verify the correctness of the execution scheduling.
			switch {
			case expectAllNodesTerminatedError:
				// Expect an `ErrAllNodesTerminated` error when all nodes are terminated.
				if !errors.Is(err, ErrAllNodesTerminated) {
					t.Errorf("expected: %v", ErrAllNodesTerminated)
					return false
				}
			default:
				// Expect no error when not all nodes are terminated.
				if err != nil {
					t.Errorf("unexpected error: %v", err)
					return false
				}
				// Expect all commands to be executed, because at least one node
				// should have remained alive.
				if len(commandMap) != 0 {
					t.Errorf("expected all commands to be executed")
					return false
				}
			}
			return true
		}
	}

	for _, terminateAllNodes := range []bool{true, false} {
		t.Run(fmt.Sprintf("terminateAllNodes=%t", terminateAllNodes), func(t *testing.T) {
			require.NoError(t, quick.Check(makeVerify(terminateAllNodes), &quick.Config{
				MaxCount: 100,
				Rand:     rng,
				Values:   makeGen(terminateAllNodes),
			}))
		})
	}
}

// TestCancelGroupsBeforeExecution tests that upcoming commands in a group that
// has been cancelled will not execute.
func TestCancelGroupsBeforeExecution(t *testing.T) {
	commands := make([][]RemoteCommand, 0)
	addCommand := func(command RemoteCommand) {
		commands = append(commands, []RemoteCommand{command})
	}

	// Use a checkpoint to synchronize the execution of cmd4. This ensures that the
	// error response for cmd2 is received before cmd4 is executed.
	checkpoint := make(chan struct{})

	addCommand(RemoteCommand{Args: []string{"cmd1"}, GroupID: "A"})
	addCommand(RemoteCommand{Args: []string{"cmd2"}, GroupID: "A"})
	addCommand(RemoteCommand{Args: []string{"cmd3"}, GroupID: "B"})
	addCommand(RemoteCommand{Args: []string{"cmd4"}, GroupID: "B"})
	addCommand(RemoteCommand{Args: []string{"cmd5"}, GroupID: "A"})
	addCommand(RemoteCommand{Args: []string{"cmd6"}, GroupID: "A"})

	// `execFunc` is a mock function that simulates the execution of a command.
	execFunc := func(
		ctx context.Context,
		l *logger.Logger,
		clusterName, SSHOptions, processTag string,
		secure bool,
		cmdArray []string,
		options install.RunOptions,
	) ([]install.RunResultDetails, error) {
		// Simulate a an error for cmd2. This should cancel the group.
		if cmdArray[0] == "cmd2" {
			return []install.RunResultDetails{
				{Err: errors.New("error")},
			}, nil
		}
		// Wait for cmd3's response to be received (checkpoint).
		if cmdArray[0] == "cmd4" {
			<-checkpoint
		}
		// Command succeeded.
		return []install.RunResultDetails{
			{},
		}, nil
	}

	group1Count := 0
	group2Count := 0
	callback := func(response RemoteResponse) {
		switch response.GroupID {
		case "A":
			group1Count++
		case "B":
			group2Count++
		}
		// Signal that cmd3's response has been received (checkpoint).
		if response.Args[0] == "cmd3" {
			checkpoint <- struct{}{}
		}
	}

	err := ExecuteRemoteCommands(nil, execFunc, "test", commands, 1, false, install.DefaultRunOptions(), callback)
	require.NoError(t, err)

	// The first error will cause the other commands in Group A to be cancelled,
	// thus we expect only 2 commands to be executed for Group A.
	require.Equal(t, 2, group1Count, "expected 2 commands to be executed for group A")
	require.Equal(t, 2, group2Count, "expected 1 command to be executed for group B")
}

func TestCancelGroupsInFlight(t *testing.T) {
	commands := make([][]RemoteCommand, 0)
	addCommand := func(command RemoteCommand) {
		commands = append(commands, []RemoteCommand{command})
	}

	addCommand(RemoteCommand{Args: []string{"cmd1"}, GroupID: "A"})
	addCommand(RemoteCommand{Args: []string{"cmd2"}, GroupID: "A"})

	// `execFunc` is a mock function that simulates the execution of a command.
	execFunc := func(
		ctx context.Context,
		l *logger.Logger,
		clusterName, SSHOptions, processTag string,
		secure bool,
		cmdArray []string,
		options install.RunOptions,
	) ([]install.RunResultDetails, error) {
		// Simulate a an error for cmd2. This should cancel the group.
		if cmdArray[0] == "cmd2" {
			return []install.RunResultDetails{
				{Err: errors.New("error")},
			}, nil
		}
		// Simulate a long running command.
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
	}

	errors := 0
	callback := func(response RemoteResponse) {
		if response.Err != nil {
			errors++
		}
	}

	err := ExecuteRemoteCommands(nil, execFunc, "test", commands, 2, false, install.DefaultRunOptions(), callback)
	require.NoError(t, err)

	// The first error will cause the other in-flight command to be cancelled,
	// therefore we expect only one error.
	require.Equal(t, 1, errors, "expected only one error")
}
