// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sessiondata

import (
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
	s := NewStack(initialElem)
	require.Equal(t, s.Top(), initialElem)
	require.EqualError(t, s.Pop(), "there must always be at least one element in the SessionData stack")
	require.Equal(t, s.Elems(), []*SessionData{initialElem})

	s.Push(secondElem)
	require.Equal(t, s.Top(), secondElem)
	s.Push(thirdElem)
	require.Equal(t, s.Top(), thirdElem)
	require.Equal(t, s.Elems(), []*SessionData{initialElem, secondElem, thirdElem})

	require.NoError(t, s.Pop())
	require.Equal(t, s.Top(), secondElem)
	require.NoError(t, s.Pop())
	require.Equal(t, s.Top(), initialElem)
	require.EqualError(t, s.Pop(), "there must always be at least one element in the SessionData stack")
	require.Equal(t, s.Elems(), []*SessionData{initialElem})

	s.Push(secondElem)
	s.Push(thirdElem)
	s.PushTopClone()
	require.Equal(t, s.Elems(), []*SessionData{initialElem, secondElem, thirdElem, thirdElem})
	c := s.Clone()
	s.PopAll()
	require.Equal(t, s.Elems(), []*SessionData{initialElem})
	s.PopAll()
	require.Equal(t, s.Elems(), []*SessionData{initialElem})
	require.Equal(t, c.Elems(), []*SessionData{initialElem, secondElem, thirdElem, thirdElem})

	s.Push(secondElem)
	s.Push(thirdElem)
	require.Error(t, s.PopN(3), "there must always be at least one element in the SessionData stack")
	require.NoError(t, s.PopN(2))
	require.Equal(t, s.Elems(), []*SessionData{initialElem})

	require.Equal(t, s.Base(), initialElem)
}
