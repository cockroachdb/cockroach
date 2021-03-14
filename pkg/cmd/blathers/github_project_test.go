// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blathers

import (
	"context"
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
)

var flagContactGithubForTest = flag.Bool(
	"contact-github",
	false,
	"whether to contact GitHub for tests",
)

func TestProjectForTeams(t *testing.T) {
	flag.Parse()

	if !*flagContactGithubForTest {
		return
	}

	ctx := context.Background()
	ghClient := srv.getGithubClientFromInstallation(
		ctx,
		installationID(7754752),
	)
	// Ensure all teams point to a valid board.
	for k := range teamToBoards {
		t.Run(k, func(t *testing.T) {
			ret, err := findProjectsForTeams(ctx, ghClient, []string{k})
			require.NoError(t, err)
			require.Greater(t, len(ret), 0)
		})
	}
}
