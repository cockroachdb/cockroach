// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestflags

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

type testValues struct {
	intVal    int
	boolVal   bool
	stringVal string
}

func initTest() (*manager, *testValues) {
	m := &manager{}
	tv := &testValues{}
	m.RegisterFlag(listCmdID, &tv.intVal, FlagInfo{
		Name: "some-int",
		Usage: `usage usage usage
usage usage`,
	})
	m.RegisterFlag(runCmdID, &tv.boolVal, FlagInfo{
		Name:      "some-bool",
		Shorthand: "b",
		Usage:     `usage for the bool`,
	})
	m.RegisterFlag(listCmdID, &tv.stringVal, FlagInfo{
		Name:  "some-string",
		Usage: `usage for the string in list`,
	})
	m.RegisterFlag(runCmdID, &tv.stringVal, FlagInfo{
		Name:      "some-string",
		Shorthand: "s",
		Usage:     `usage for the string in run`,
	})
	return m, tv
}

func TestManager(t *testing.T) {
	t.Run("list1", func(t *testing.T) {
		m, tv := initTest()
		listCmd := &cobra.Command{}
		m.AddFlagsToCommand(listCmdID, listCmd.Flags())
		require.NoError(t, listCmd.ParseFlags([]string{"--some-int", "123"}))
		require.Equal(t, 123, tv.intVal)
		require.True(t, m.Changed(&tv.intVal))
		require.False(t, m.Changed(&tv.boolVal))
		require.False(t, m.Changed(&tv.stringVal))
	})

	t.Run("list2", func(t *testing.T) {
		m, tv := initTest()
		listCmd := &cobra.Command{}
		m.AddFlagsToCommand(listCmdID, listCmd.Flags())
		require.NoError(t, listCmd.ParseFlags([]string{"--some-int", "123", "--some-string", "foo"}))
		require.Equal(t, 123, tv.intVal)
		require.Equal(t, "foo", tv.stringVal)
		require.True(t, m.Changed(&tv.intVal))
		require.False(t, m.Changed(&tv.boolVal))
		require.True(t, m.Changed(&tv.stringVal))
	})

	t.Run("run", func(t *testing.T) {
		m, tv := initTest()
		runCmd := &cobra.Command{}
		m.AddFlagsToCommand(runCmdID, runCmd.Flags())
		require.NoError(t, runCmd.ParseFlags([]string{"-b"}))
		require.True(t, tv.boolVal)
		require.False(t, m.Changed(&tv.intVal))
		require.True(t, m.Changed(&tv.boolVal))
		require.False(t, m.Changed(&tv.stringVal))
	})

	t.Run("run_and_bench", func(t *testing.T) {
		m, tv := initTest()
		runCmd := &cobra.Command{}
		benchCmd := &cobra.Command{}
		m.AddFlagsToCommand(runCmdID, runCmd.Flags())
		m.AddFlagsToCommand(runCmdID, benchCmd.Flags())
		require.NoError(t, runCmd.ParseFlags([]string{"--some-bool=false", "-s", "foo"}))
		require.False(t, tv.boolVal)
		require.Equal(t, "foo", tv.stringVal)
		require.False(t, m.Changed(&tv.intVal))
		require.True(t, m.Changed(&tv.boolVal))
		require.True(t, m.Changed(&tv.stringVal))
	})
}

func TestCleanupString(t *testing.T) {
	in := `
  this is
			a string that has been broken up into many lines,
because why not

`
	out := cleanupString(in)
	require.Equal(t, "this is a string that has been broken up into many lines, because why not", out)
}
