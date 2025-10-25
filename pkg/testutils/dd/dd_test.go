// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package dd

import (
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestParseCommand(t *testing.T) {
	type RangeID int64
	type ReplicaID int32
	type testCommand struct {
		RangeID     RangeID   `name:"range-id"`
		ReplicaID   ReplicaID `name:"repl-id"`
		Initialized bool      `name:"init" opt:"true"`
		Keys        []string  `name:"keys" min:"2"`
	}

	d := datadriven.TestData{Cmd: "test", CmdArgs: []datadriven.CmdArg{
		{Key: "range-id", Vals: []string{"123"}},
		{Key: "repl-id", Vals: []string{"4"}},
		{Key: "keys", Vals: []string{"a", "b"}},
	}}

	var cmd testCommand
	ParseCommand(t, &d, &cmd)

	require.Equal(t, testCommand{
		RangeID:     123,
		ReplicaID:   4,
		Initialized: false,
		Keys:        []string{"a", "b"},
	}, cmd)
}
