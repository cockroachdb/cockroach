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
		// This variable uses string type under the hood but has the custom
		// cast type which isn't handled, so the update should be a no-op.
		{variable: "UserProto", value: "foo", localOnly: false},
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
