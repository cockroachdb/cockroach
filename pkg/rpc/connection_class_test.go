// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestConnectionClassOverrides(t *testing.T) {
	defer leaktest.AfterTest(t)()
	type classMap map[ConnectionClass]ConnectionClass
	for _, tc := range []struct {
		input string
		want  classMap
	}{{
		input: "",
		want:  classMap{},
	}, {
		input: "def", // technically, default class can be overridden too
		want:  classMap{DefaultClass: DefaultClass},
	}, {
		input: "raft", // single class
		want:  classMap{RaftClass: DefaultClass},
	}, {
		input: "def", // technically, default class can be overridden too
		want:  classMap{DefaultClass: DefaultClass},
	}, {
		input: "rf,raft", // two classes
		want:  classMap{RangefeedClass: DefaultClass, RaftClass: DefaultClass},
	}, {
		input: "sys,raft,rf,raft", // multiple classes, also with duplicates
		want: classMap{
			SystemClass:    DefaultClass,
			RangefeedClass: DefaultClass,
			RaftClass:      DefaultClass,
		},
	}, {
		input: "raft,custom-class", // custom classes are ignored
		want:  classMap{RaftClass: DefaultClass},
	}, {
		input: "raft,sys,custom-class,,raft,", // weird inputs handled gracefully
		want:  classMap{SystemClass: DefaultClass, RaftClass: DefaultClass},
	}} {
		t.Run("", func(t *testing.T) {
			got := parseClassOverrides(tc.input)
			require.Equal(t, tc.want, classMap(got))
		})
	}
}
