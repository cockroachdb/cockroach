// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engineupgrade

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createEngine creates an engine for testing.
// It should be closed when it is no longer needed via the Close() method.
func createEngine(t *testing.T, state *EngineState) engine.Engine {
	ctx := context.Background()
	eng := engine.NewInMem(
		ctx,
		engine.DefaultStorageEngine,
		roachpb.Attributes{},
		1<<20,
	)
	if state != nil {
		err := setEngineState(ctx, eng, state)
		require.NoError(t, err)
	}
	return eng
}

// assertStatus ensures the status from serv matches the expected status.
func assertNodeUpgraderStatus(t *testing.T, serv *nodeUpgrader, expected []*EngineState) {
	statusResp, err := serv.Status(context.Background(), &StatusRequest{})
	require.NoError(t, err)
	assert.Equal(t, &StatusResponse{EngineStates: expected}, statusResp)
}

func TestUpgradeEngineToTarget(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		desc string

		engInitState *EngineState
		target       Version

		expectedNumUpgradesCalled map[int]int
		expectedState             *EngineState
		expectedErrorRegex        string
	}{
		{
			desc:                      "errors if the current version is higher than target version",
			engInitState:              &EngineState{CurrentVersion: 2, TargetVersion: 1},
			target:                    Version(3),
			expectedErrorRegex:        "current version .* > target version .*",
			expectedNumUpgradesCalled: map[int]int{},
			expectedState:             &EngineState{CurrentVersion: 2, TargetVersion: 1},
		},
		{
			desc:                      "errors if target version is higher than the given target",
			engInitState:              &EngineState{CurrentVersion: 2, TargetVersion: 2},
			target:                    Version(1),
			expectedErrorRegex:        "attempted downgrade: target version .* > target version .*",
			expectedNumUpgradesCalled: map[int]int{},
			expectedState:             &EngineState{CurrentVersion: 2, TargetVersion: 2},
		},
		{
			desc:                      "succeeds if version already matches",
			engInitState:              &EngineState{CurrentVersion: 2, TargetVersion: 2},
			target:                    Version(2),
			expectedNumUpgradesCalled: map[int]int{},
			expectedState:             &EngineState{CurrentVersion: 2, TargetVersion: 2},
		},
		{
			desc:                      "errors if version upgrade is expected, but not found",
			engInitState:              &EngineState{CurrentVersion: 4, TargetVersion: 4},
			target:                    Version(5),
			expectedErrorRegex:        "could not find upgrade for key",
			expectedNumUpgradesCalled: map[int]int{},
			expectedState:             &EngineState{CurrentVersion: 4, TargetVersion: 5},
		},
		{
			desc:                      "errors propagate from inside upgrade functions",
			engInitState:              &EngineState{CurrentVersion: 3, TargetVersion: 3},
			target:                    Version(4),
			expectedErrorRegex:        "running upgrade:.*error",
			expectedNumUpgradesCalled: map[int]int{},
			expectedState:             &EngineState{CurrentVersion: 3, TargetVersion: 4},
		},
		{
			desc:                      "successfully upgrades up new version",
			engInitState:              &EngineState{},
			target:                    Version(3),
			expectedNumUpgradesCalled: map[int]int{1: 1, 2: 1, 3: 1},
			expectedState:             &EngineState{CurrentVersion: 3, TargetVersion: 3},
		},
		{
			desc:                      "successfully upgrades to new version if no upgrades have ever been performed",
			engInitState:              nil,
			target:                    Version(3),
			expectedNumUpgradesCalled: map[int]int{1: 1, 2: 1, 3: 1},
			expectedState:             &EngineState{CurrentVersion: 3, TargetVersion: 3},
		},
		{
			desc:                      "successfully upgrades up new version if dropped halfway",
			engInitState:              &EngineState{CurrentVersion: 1, TargetVersion: 3},
			target:                    Version(3),
			expectedNumUpgradesCalled: map[int]int{2: 1, 3: 1},
			expectedState:             &EngineState{CurrentVersion: 3, TargetVersion: 3},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			reg := &fnRegistry{}
			numUpgradesCalled := make(map[int]int)
			reg.mu.versionToFn = map[Version]Fn{
				Version(1): func(context.Context, engine.Engine) error {
					numUpgradesCalled[1]++
					return nil
				},
				Version(2): func(context.Context, engine.Engine) error {
					numUpgradesCalled[2]++
					return nil
				},
				Version(3): func(context.Context, engine.Engine) error {
					numUpgradesCalled[3]++
					return nil
				},
				Version(4): func(context.Context, engine.Engine) error {
					return errors.New("error")
				},
			}

			eng := createEngine(t, tc.engInitState)
			err := upgradeEngineToTarget(ctx, eng, reg, tc.target)
			if tc.expectedErrorRegex == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Regexp(t, tc.expectedErrorRegex, err.Error())
			}
			state, err := getEngineState(ctx, eng)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedState, state)
			assert.Equal(t, tc.expectedNumUpgradesCalled, numUpgradesCalled)
		})
	}
}

func TestNodeUpgraderUpgradeEngines(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		desc string

		engInitStates []*EngineState
		targetVersion Version

		expectedErrorRegex        string
		expectedState             []*EngineState
		expectedNumUpgradesCalled map[int]int
	}{
		{
			desc: "propagates errors from bad engines",
			engInitStates: []*EngineState{
				{CurrentVersion: 1},
				{CurrentVersion: 1, TargetVersion: 1},
			},
			targetVersion:      Version(1),
			expectedErrorRegex: "current version .* > target version .*",
			expectedState: []*EngineState{
				{CurrentVersion: 1},
				{CurrentVersion: 1, TargetVersion: 1},
			},
			expectedNumUpgradesCalled: map[int]int{},
		},
		{
			desc: "succeeds",
			engInitStates: []*EngineState{
				{CurrentVersion: 1, TargetVersion: 3},
				{CurrentVersion: 0, TargetVersion: 0},
			},
			targetVersion: Version(3),
			expectedState: []*EngineState{
				{CurrentVersion: 3, TargetVersion: 3},
				{CurrentVersion: 3, TargetVersion: 3},
			},
			expectedNumUpgradesCalled: map[int]int{1: 1, 2: 2, 3: 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			reg := &fnRegistry{}
			writeMu := struct {
				syncutil.Mutex
				numUpgradesCalled map[int]int
			}{
				numUpgradesCalled: make(map[int]int),
			}
			reg.mu.versionToFn = map[Version]Fn{
				Version(1): func(context.Context, engine.Engine) error {
					writeMu.Lock()
					defer writeMu.Unlock()
					writeMu.numUpgradesCalled[1]++
					return nil
				},
				Version(2): func(context.Context, engine.Engine) error {
					writeMu.Lock()
					defer writeMu.Unlock()
					writeMu.numUpgradesCalled[2]++
					return nil
				},
				Version(3): func(context.Context, engine.Engine) error {
					writeMu.Lock()
					defer writeMu.Unlock()
					writeMu.numUpgradesCalled[3]++
					return nil
				},
			}

			engs := make([]engine.Engine, len(tc.engInitStates))
			for i, s := range tc.engInitStates {
				engs[i] = createEngine(t, s)
			}
			serv := &nodeUpgrader{engs: engs, fnRegistry: reg}
			for _, eng := range serv.engs {
				defer eng.Close()
			}
			resp, err := serv.UpgradeEngines(
				ctx,
				&UpgradeEnginesRequest{TargetVersion: tc.targetVersion},
			)
			require.NoError(t, err)
			if tc.expectedErrorRegex == "" {
				require.Nil(t, resp.Err)
			} else {
				err = errors.DecodeError(ctx, *resp.Err)
				assert.Regexp(t, tc.expectedErrorRegex, err.Error())
			}
			assert.Equal(t, tc.expectedNumUpgradesCalled, writeMu.numUpgradesCalled)

			assertNodeUpgraderStatus(t, serv, tc.expectedState)
		})
	}

	t.Run("test mutual exclusion of UpgradeEngines", func(t *testing.T) {
		// Upgrade to version 0->1 in a background node, and 1->2 in the foreground.
		// If upgrade 0->1 succeeds, then shouldError should equal false.
		// If this flakes, we have an error.
		var shouldError bool
		reg := &fnRegistry{}
		readyCh := make(chan struct{})
		reg.mu.versionToFn = map[Version]Fn{
			Version(1): func(context.Context, engine.Engine) error {
				readyCh <- struct{}{}
				shouldError = false
				return nil
			},
			Version(2): func(context.Context, engine.Engine) error {
				if shouldError {
					return errors.Newf("error")
				}
				return nil
			},
		}

		serv := &nodeUpgrader{
			engs:       []engine.Engine{createEngine(t, &EngineState{})},
			fnRegistry: reg,
		}
		for _, eng := range serv.engs {
			defer eng.Close()
		}

		// This upgrade should run in the background, locking any other upgrade requests.
		go func() {
			_, err := serv.UpgradeEngines(ctx, &UpgradeEnginesRequest{TargetVersion: 1})
			require.NoError(t, err)
		}()

		// Now that we now we're inside the upgrade of version 1, we can start the second
		// which should hit the lock. If it flakes at this point, something is up.
		<-readyCh
		_, err := serv.UpgradeEngines(ctx, &UpgradeEnginesRequest{TargetVersion: 2})
		require.NoError(t, err)
		assertNodeUpgraderStatus(t, serv, []*EngineState{{CurrentVersion: 2, TargetVersion: 2}})
	})
}

func TestNodeUpgradeStatus(t *testing.T) {
	t.Run("tests status is non-blocking and can be called mid-upgrade", func(t *testing.T) {
		reg := &fnRegistry{}
		cancelCh := make(chan struct{})
		readyCh := make(chan struct{})
		reg.mu.versionToFn = map[Version]Fn{
			Version(1): func(context.Context, engine.Engine) error {
				readyCh <- struct{}{}
				<-cancelCh
				return nil
			},
		}

		serv := &nodeUpgrader{
			engs:       []engine.Engine{createEngine(t, &EngineState{})},
			fnRegistry: reg,
		}
		for _, eng := range serv.engs {
			defer eng.Close()
		}

		var upgradeErr error
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, upgradeErr = serv.UpgradeEngines(
				context.Background(),
				&UpgradeEnginesRequest{TargetVersion: Version(1)},
			)
		}()

		// Wait for the upgrade function to be hit.
		<-readyCh

		// Now check status still works.
		assertNodeUpgraderStatus(t, serv, []*EngineState{{CurrentVersion: 0, TargetVersion: 1}})

		// Cleanup and wait for upgrade to return, and check no error.
		cancelCh <- struct{}{}
		wg.Wait()
		assert.NoError(t, upgradeErr)
		assertNodeUpgraderStatus(t, serv, []*EngineState{{CurrentVersion: 1, TargetVersion: 1}})
	})
}
