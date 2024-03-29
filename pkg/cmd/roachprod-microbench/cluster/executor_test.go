// Copyright 2024 The Cockroach Authors.
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
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

func TestExecuteRemoteCommands(t *testing.T) {
	execFunc := func(
		ctx context.Context,
		l *logger.Logger,
		clusterName, SSHOptions, processTag string,
		secure bool,
		cmdArray []string,
		options install.RunOptions,
	) ([]install.RunResultDetails, error) {
		if clusterName == "test:1" {
			return []install.RunResultDetails{
				{},
			}, rperrors.ErrSSH255
		}
		return []install.RunResultDetails{
			{},
		}, nil
	}
	callback := func(response RemoteResponse) {
		fmt.Println(response.Args)
	}

	commands := make([][]RemoteCommand, 0)
	addCommand := func(command RemoteCommand) {
		commands = append(commands, []RemoteCommand{command})
	}
	addCommand(RemoteCommand{Args: []string{"echo", "hello"}})

	err := ExecuteRemoteCommands(
		nil, execFunc, "test", commands, 2, false, callback,
	)
	require.NoError(t, err)
}
