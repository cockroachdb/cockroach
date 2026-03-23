// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		require.True(t, m.Changed(&tv.intVal) != nil)
		require.Equal(t, "some-int", m.Changed(&tv.intVal).Name)
		require.False(t, m.Changed(&tv.boolVal) != nil)
		require.False(t, m.Changed(&tv.stringVal) != nil)
	})

	t.Run("list2", func(t *testing.T) {
		m, tv := initTest()
		listCmd := &cobra.Command{}
		m.AddFlagsToCommand(listCmdID, listCmd.Flags())
		require.NoError(t, listCmd.ParseFlags([]string{"--some-int", "123", "--some-string", "foo"}))
		require.Equal(t, 123, tv.intVal)
		require.Equal(t, "foo", tv.stringVal)
		require.True(t, m.Changed(&tv.intVal) != nil)
		require.Equal(t, "some-int", m.Changed(&tv.intVal).Name)
		require.False(t, m.Changed(&tv.boolVal) != nil)
		require.True(t, m.Changed(&tv.stringVal) != nil)
		require.Equal(t, "some-string", m.Changed(&tv.stringVal).Name)
	})

	t.Run("run", func(t *testing.T) {
		m, tv := initTest()
		runCmd := &cobra.Command{}
		m.AddFlagsToCommand(runCmdID, runCmd.Flags())
		require.NoError(t, runCmd.ParseFlags([]string{"-b"}))
		require.True(t, tv.boolVal)
		require.False(t, m.Changed(&tv.intVal) != nil)
		require.True(t, m.Changed(&tv.boolVal) != nil)
		require.Equal(t, "b", m.Changed(&tv.boolVal).Shorthand)
		require.False(t, m.Changed(&tv.stringVal) != nil)
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
		require.False(t, m.Changed(&tv.intVal) != nil)
		require.True(t, m.Changed(&tv.boolVal) != nil)
		require.Equal(t, "some-bool", m.Changed(&tv.boolVal).Name)
		require.True(t, m.Changed(&tv.stringVal) != nil)
		require.Equal(t, "s", m.Changed(&tv.stringVal).Shorthand)
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

func TestManagerStringArrayAndMap(t *testing.T) {
	t.Run("string_array", func(t *testing.T) {
		m := &manager{}
		var arrayVal []string
		m.RegisterFlag(runCmdID, &arrayVal, FlagInfo{
			Name:  "start-env",
			Usage: "env vars to inject",
		})

		runCmd := &cobra.Command{}
		m.AddFlagsToCommand(runCmdID, runCmd.Flags())

		// Test multiple --start-env flags
		require.NoError(t, runCmd.ParseFlags([]string{
			"--start-env", "FOO=bar",
			"--start-env", "BAZ=qux",
		}))
		require.Equal(t, []string{"FOO=bar", "BAZ=qux"}, arrayVal)
		require.NotNil(t, m.Changed(&arrayVal))
	})

	t.Run("string_to_string_map", func(t *testing.T) {
		m := &manager{}
		var mapVal map[string]string
		m.RegisterFlag(runCmdID, &mapVal, FlagInfo{
			Name:  "start-setting",
			Usage: "cluster settings to apply",
		})

		runCmd := &cobra.Command{}
		m.AddFlagsToCommand(runCmdID, runCmd.Flags())

		// Test key=value format
		require.NoError(t, runCmd.ParseFlags([]string{
			"--start-setting", "foo.bar=baz",
			"--start-setting", "qux.quux=corge",
		}))
		require.Equal(t, map[string]string{
			"foo.bar":  "baz",
			"qux.quux": "corge",
		}, mapVal)
		require.NotNil(t, m.Changed(&mapVal))
	})
}
