// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessionmutator

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/stretchr/testify/require"
)

// makeTestIterator creates a SessionDataMutatorIterator with the given stack
// for testing the Apply* methods.
func makeTestIterator(sds *sessiondata.Stack) *SessionDataMutatorIterator {
	return &SessionDataMutatorIterator{
		Sds: sds,
		SessionDataMutatorBase: SessionDataMutatorBase{
			Defaults: SessionDefaults{},
			Settings: cluster.MakeTestingClusterSettings(),
		},
	}
}

func TestApplyMutators(t *testing.T) {
	// setAppName returns a mutator function that sets ApplicationName on the
	// targeted SessionData.
	setAppName := func(name string) func(m SessionDataMutator) error {
		return func(m SessionDataMutator) error {
			m.SetApplicationName(name)
			return nil
		}
	}

	t.Run("ApplyOnEachMutatorError", func(t *testing.T) {
		base := &sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{ApplicationName: "base"},
		}
		sds := sessiondata.NewStack(base.Clone())
		sds.Push(
			&sessiondata.SessionData{
				SessionData: sessiondatapb.SessionData{ApplicationName: "txn"},
			},
		)
		it := makeTestIterator(sds)

		require.NoError(t, it.ApplyOnEachMutatorError(setAppName("all")))
		for _, elem := range sds.Elems() {
			require.Equal(t, "all", elem.ApplicationName)
		}
	})

	t.Run("ApplyOnEachMutatorError/with_stmt_level", func(t *testing.T) {
		base := &sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{ApplicationName: "base"},
		}
		sds := sessiondata.NewStack(base.Clone())
		sds.Push(
			&sessiondata.SessionData{
				SessionData: sessiondatapb.SessionData{ApplicationName: "txn"},
			},
		)
		sds.PushStmtLevel()
		it := makeTestIterator(sds)

		require.NoError(t, it.ApplyOnEachMutatorError(setAppName("all")))
		// All stack frames updated.
		for _, elem := range sds.Elems() {
			require.Equal(t, "all", elem.ApplicationName)
		}
		// Stmt level also updated.
		require.Equal(t, "all", sds.StmtLevel().ApplicationName)
		sds.PopStmtLevel()
	})

	t.Run("ApplyOnStmtScopedMutator", func(t *testing.T) {
		base := &sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{ApplicationName: "base"},
		}
		sds := sessiondata.NewStack(base.Clone())
		sds.Push(
			&sessiondata.SessionData{
				SessionData: sessiondatapb.SessionData{ApplicationName: "txn"},
			},
		)
		sds.PushStmtLevel()
		it := makeTestIterator(sds)

		// ApplyOnStmtScopedMutator only writes to the stmt level.
		require.NoError(t, it.ApplyOnStmtScopedMutator(setAppName("hint")))
		require.Equal(t, "hint", sds.StmtLevel().ApplicationName)
		// The stack frames should be untouched.
		require.Equal(t, "txn", sds.Elems()[1].ApplicationName)
		require.Equal(t, "base", sds.Elems()[0].ApplicationName)
		sds.PopStmtLevel()
	})

	t.Run("ApplyOnStmtScopedMutator/no_stmt_level", func(t *testing.T) {
		base := &sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{ApplicationName: "base"},
		}
		sds := sessiondata.NewStack(base.Clone())
		it := makeTestIterator(sds)

		// Should return an error when no stmt level is active.
		err := it.ApplyOnStmtScopedMutator(setAppName("hint"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "without active statement-level session data")
	})

	t.Run("ApplyOnTopMutator/no_stmt_level", func(t *testing.T) {
		// Without stmt level, it applies to the stack top only.
		base := &sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{ApplicationName: "base"},
		}
		sds := sessiondata.NewStack(base.Clone())
		sds.Push(
			&sessiondata.SessionData{
				SessionData: sessiondatapb.SessionData{ApplicationName: "txn"},
			},
		)
		it := makeTestIterator(sds)

		require.NoError(t, it.ApplyOnTopMutator(setAppName("local")))
		require.Equal(t, "local", sds.Top().ApplicationName)
		// Base untouched.
		require.Equal(t, "base", sds.Base().ApplicationName)
	})

	t.Run("ApplyOnTopMutator/with_stmt_level", func(t *testing.T) {
		// With stmt level active, both the stack top and the stmt level
		// are updated so the current statement sees the change.
		base := &sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{ApplicationName: "base"},
		}
		sds := sessiondata.NewStack(base.Clone())
		sds.Push(
			&sessiondata.SessionData{
				SessionData: sessiondatapb.SessionData{ApplicationName: "txn"},
			},
		)
		sds.PushStmtLevel()
		require.Equal(t, "txn", sds.Top().ApplicationName) // cloned from txn
		it := makeTestIterator(sds)

		require.NoError(t, it.ApplyOnTopMutator(setAppName("local")))
		// Both the stmt level (top) and the stack top should be updated.
		require.Equal(t, "local", sds.StmtLevel().ApplicationName)
		require.Equal(t, "local", sds.Elems()[1].ApplicationName)
		// Base untouched.
		require.Equal(t, "base", sds.Base().ApplicationName)
		sds.PopStmtLevel()
	})

	t.Run("ApplyOnTopMutator/session_only", func(t *testing.T) {
		// With only a session base and stmt level, it applies to the
		// session frame (stack top) and the stmt level.
		base := &sessiondata.SessionData{
			SessionData: sessiondatapb.SessionData{ApplicationName: "base"},
		}
		sds := sessiondata.NewStack(base.Clone())
		sds.PushStmtLevel()
		it := makeTestIterator(sds)

		require.NoError(t, it.ApplyOnTopMutator(setAppName("local")))
		// Both session base and stmt level should be updated.
		require.Equal(t, "local", sds.StmtLevel().ApplicationName)
		require.Equal(t, "local", sds.Base().ApplicationName)
		sds.PopStmtLevel()
	})
}
