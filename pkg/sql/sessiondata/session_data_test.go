// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessiondata

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/stretchr/testify/require"
)

func TestStack(t *testing.T) {
	initialElem := &SessionData{
		SessionData: sessiondatapb.SessionData{
			ApplicationName: "bob",
		},
	}
	secondElem := &SessionData{
		SessionData: sessiondatapb.SessionData{
			ApplicationName: "jane",
		},
	}
	thirdElem := &SessionData{
		SessionData: sessiondatapb.SessionData{
			ApplicationName: "t-marts",
		},
	}
	s := NewStack(initialElem.Clone())
	require.Equal(t, s.Top(), initialElem)
	require.EqualError(t, s.Pop(), "there must always be at least one element in the SessionData stack")
	require.Equal(t, s.Elems(), []*SessionData{initialElem})

	s.Push(secondElem.Clone())
	require.Equal(t, s.Top(), secondElem.Clone())
	s.Push(thirdElem.Clone())
	require.Equal(t, s.Top(), thirdElem.Clone())
	require.Equal(t, s.Elems(), []*SessionData{initialElem, secondElem, thirdElem})

	require.NoError(t, s.Pop())
	require.Equal(t, s.Top(), secondElem.Clone())
	require.NoError(t, s.Pop())
	require.Equal(t, s.Top(), initialElem.Clone())
	require.EqualError(t, s.Pop(), "there must always be at least one element in the SessionData stack")
	require.Equal(t, s.Elems(), []*SessionData{initialElem})

	s.Push(secondElem.Clone())
	s.Push(thirdElem.Clone())
	s.PushTopClone()
	require.Equal(t, s.Elems(), []*SessionData{initialElem, secondElem, thirdElem, thirdElem})
	c := s.Clone()
	s.PopAll()
	require.Equal(t, s.Elems(), []*SessionData{initialElem})
	s.PopAll()
	require.Equal(t, s.Elems(), []*SessionData{initialElem})
	require.Equal(t, c.Elems(), []*SessionData{initialElem, secondElem, thirdElem, thirdElem})

	s.Push(secondElem.Clone())
	s.Push(thirdElem.Clone())
	copyStack := s.Clone()
	require.Error(t, s.PopN(3), "there must always be at least one element in the SessionData stack")
	require.NoError(t, s.PopN(2))
	require.Equal(t, s.Elems(), []*SessionData{initialElem})

	require.Equal(t, s.Base(), initialElem)

	s.Top().Database = "some other value"
	s.Replace(copyStack)
	require.Equal(t, s.Elems(), []*SessionData{initialElem, secondElem, thirdElem})
	require.Equal(t, s.Base(), initialElem)

	copyStack.Top().Database = "some other value"
	s.Replace(copyStack)
	copiedElem := thirdElem
	copiedElem.Database = "some other value"
	require.Equal(t, s.Elems(), []*SessionData{initialElem, secondElem, copiedElem})
	require.Equal(t, s.Base(), initialElem)
}

func TestStmtLevel(t *testing.T) {
	sd := &SessionData{
		SessionData: sessiondatapb.SessionData{ApplicationName: "base"},
	}
	s := NewStack(sd)

	// Initially no stmtLevel.
	require.False(t, s.HasStmtLevel())
	require.Nil(t, s.StmtLevel())
	require.Equal(t, "base", s.Top().ApplicationName)

	// Push a txn frame, then push stmt level.
	s.PushTopClone()
	s.Top().ApplicationName = "txn"
	s.PushStmtLevel()
	require.True(t, s.HasStmtLevel())
	require.Equal(t, "txn", s.Top().ApplicationName) // cloned from txn frame

	// Modifying the stmt level does not affect the stack top.
	s.Top().ApplicationName = "stmt"
	require.Equal(t, "stmt", s.StmtLevel().ApplicationName)
	require.Equal(t, "txn", s.Elems()[1].ApplicationName) // stack top unchanged

	// Pop stmt level.
	s.PopStmtLevel()
	require.False(t, s.HasStmtLevel())
	require.Equal(t, "txn", s.Top().ApplicationName) // back to stack top

	// Double push panics.
	s.PushStmtLevel()
	require.Panics(t, func() { s.PushStmtLevel() })
	s.PopStmtLevel()

	// Pop without push panics.
	require.Panics(t, func() { s.PopStmtLevel() })

	// Stack ops while stmtLevel is set panic.
	s.PushStmtLevel()
	require.Panics(t, func() { s.Push(sd.Clone()) })
	require.Panics(t, func() { s.PushTopClone() })
	require.Panics(t, func() { _ = s.Pop() })
	require.Panics(t, func() { _ = s.PopN(1) })
	require.Panics(t, func() { s.PopAll() })
	s.PopStmtLevel()

	// Clone preserves stmtLevel.
	s.PushStmtLevel()
	s.Top().ApplicationName = "cloned-stmt"
	c := s.Clone()
	require.True(t, c.HasStmtLevel())
	require.Equal(t, "cloned-stmt", c.Top().ApplicationName)
	// Cloned stmtLevel is independent.
	c.Top().ApplicationName = "modified"
	require.Equal(t, "cloned-stmt", s.Top().ApplicationName)
	s.PopStmtLevel()
}

func TestUpdateSessionData(t *testing.T) {
	var sd SessionData
	unchangedLocal := sd.LocalOnlySessionData.String()
	unchangedRemote := sd.SessionData.String()
	for _, tc := range []struct {
		variable  string
		value     string
		localOnly bool
		// If set, then the update must have succeeded.
		expectedSubstring string
	}{
		// string type.
		{variable: "Database", value: "foo", localOnly: false, expectedSubstring: `database:"foo"`},
		// SQLUsernameProto custom type.
		{variable: "UserProto", value: "foo", localOnly: false, expectedSubstring: `user_proto:"foo"`},
		// This variable has a custom type that isn't handled, so the update
		// should be a no-op.
		{variable: "DataConversionConfig", value: "foo", localOnly: false},
		// This variable has a custom type that is handled.
		{variable: "VectorizeMode", value: "on", localOnly: false, expectedSubstring: `vectorize_mode:on`},
		// duration type.
		{variable: "LockTimeout", value: "42s", localOnly: false, expectedSubstring: `lock_timeout:<seconds:42 >`},
		// duration type.
		{variable: "DeadlockTimeout", value: "42s", localOnly: false, expectedSubstring: `deadlock_timeout:<seconds:42 >`},
		// This variable has a custom name, int64 type.
		{variable: "OptimizerFKCascadesLimit", value: "123", localOnly: true, expectedSubstring: `optimizer_fk_cascades_limit:123`},
		// bool type.
		{variable: "DefaultTxnReadOnly", value: "true", localOnly: true, expectedSubstring: `default_txn_read_only:true`},
		// float64 type.
		{variable: "LargeFullScanRows", value: "12.34", localOnly: true, expectedSubstring: `large_full_scan_rows:12.34`},
		// int32 type.
		{variable: "OptSplitScanLimit", value: "123", localOnly: true, expectedSubstring: `opt_split_scan_limit:123`},
		// DistSQLExecMode custom type.
		{variable: "DistSQLMode", value: "on", localOnly: true, expectedSubstring: `dist_sql_mode:2`},
		// NewSchemaChangerMode custom type.
		{variable: "NewSchemaChangerMode", value: "unsafe_always", localOnly: true, expectedSubstring: `new_schema_changer_mode:3`},
	} {
		sd = SessionData{}
		ok := sd.Update(tc.variable, tc.value)
		local, remote := sd.LocalOnlySessionData.String(), sd.SessionData.String()
		if tc.expectedSubstring != "" {
			require.True(t, ok)
			if tc.localOnly {
				require.True(t, strings.Contains(local, tc.expectedSubstring))
				require.Equal(t, unchangedRemote, remote)
			} else {
				require.True(t, strings.Contains(remote, tc.expectedSubstring))
				require.Equal(t, unchangedLocal, local)
			}
		} else {
			require.False(t, ok)
			require.Equal(t, unchangedLocal, local)
			require.Equal(t, unchangedRemote, remote)
		}
	}
}

func TestValidateMultiOverride(t *testing.T) {
	for _, tc := range []struct {
		name        string
		input       string
		expectedErr string
	}{
		{name: "empty", input: ""},
		{name: "valid bool", input: "DefaultTxnReadOnly=true"},
		{name: "valid string", input: "Database=foo"},
		{name: "valid multiple", input: "Database=foo,DefaultTxnReadOnly=true"},
		{name: "valid DistSQLMode", input: "DistSQLMode=off"},
		{name: "valid NewSchemaChangerMode", input: "NewSchemaChangerMode=unsafe_always"},
		{
			name:        "bad format",
			input:       "no_equals_sign",
			expectedErr: "invalid override format",
		},
		{
			name:        "unknown field name",
			input:       "distsql=off",
			expectedErr: `unknown or unsupported session variable "distsql"`,
		},
		{
			name:        "bad value for type",
			input:       "DefaultTxnReadOnly=notabool",
			expectedErr: `unknown or unsupported session variable "DefaultTxnReadOnly"`,
		},
		{
			name:        "second override invalid",
			input:       "Database=foo,bogus=bar",
			expectedErr: `unknown or unsupported session variable "bogus"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateMultiOverride(tc.input)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
