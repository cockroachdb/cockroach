// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commands

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_processYaml(t *testing.T) {
	ctx := context.Background()
	// setting to nil as a precaution that the command execution does not invoke an
	// actual command
	commandExecutor = nil
	t.Run("expect unmarshall to fail", func(t *testing.T) {
		err := processYaml(ctx, []byte("invalid"), false, nil)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "cannot unmarshal")
	})
	t.Run("expect failure due to unwanted field", func(t *testing.T) {
		err := processYaml(ctx, []byte(`
unwanted: value
environment:
  NAME_1: name_value1
  NAME_2: name_value2
`), false, nil)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "field unwanted not found in type commands.yamlConfig")
	})
	t.Run("expect no command execution on display-only=true", func(t *testing.T) {
		require.Nil(t, processYaml(ctx, getTestYaml(), true, nil))
	})
	t.Run("expect partial failure and rollback", func(t *testing.T) {
		name1Commands := make([]string, 0)
		name2Commands := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			if strings.HasPrefix(logPrefix, "name_value1") {
				name1Commands = append(name1Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "name_value2") {
				name2Commands = append(name2Commands, (&command{name: cmd, args: args}).String())
			}
			if cmd == "dummy_script1" || cmd == "script33" || args[0] == "rb_dummy2" {
				return fmt.Errorf("error while processing script %s", cmd)
			}
			return nil
		}
		require.Nil(t, processYaml(ctx, getTestYaml(), false, nil))
		require.Equal(t, 8, len(name1Commands))
		require.Equal(t, 1, len(name2Commands))
		// the flags are maintained as map and can be in any sequence
		require.True(t, strings.HasPrefix(name1Commands[0], "drtprod dummy1 name_value1 arg11"))
		require.True(t, strings.Contains(name1Commands[0], "--clouds=gce"))
		require.True(t, strings.Contains(name1Commands[0], "--nodes=1"))
		require.Equal(t, []string{
			"dummy_script1", "dummy_script2 arg11", "drtprod dummy2", "script33",
		}, name1Commands[1:5])
		// rollback
		require.True(t, strings.HasPrefix(name1Commands[5], "drtprod rb_dummy2 arg1 arg2"))
		require.True(t, strings.Contains(name1Commands[5], "--flag1=value1"))
		require.True(t, strings.Contains(name1Commands[5], "--flag2=value2"))
		require.True(t, strings.HasPrefix(name1Commands[6], "dummy_script22"))
		require.True(t, strings.Contains(name1Commands[6], "--f1=\\\"v1 v2\\\""))
		require.Equal(t, "drtprod rb_dummy1", name1Commands[7])
		require.Equal(t, []string{
			"drtprod dummy2 name_value2 arg12",
		}, name2Commands)
	})
	t.Run("expect no failure", func(t *testing.T) {
		name1Commands := make([]string, 0)
		name2Commands := make([]string, 0)
		depN1Commands := make([]string, 0)
		depN1N2Commands := make([]string, 0)
		depNotPresentCommands := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			if strings.HasPrefix(logPrefix, "name_value1") {
				name1Commands = append(name1Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "name_value2") {
				name2Commands = append(name2Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_n1") {
				// expect that "name_value1" is complete by now
				require.Equal(t, 6, len(name1Commands))
				depN1Commands = append(depN1Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_n2_n1") {
				// expect that "name_value1" and "name_value2" is complete by now
				require.Equal(t, 6, len(name1Commands))
				require.Equal(t, 1, len(name2Commands))
				depN1N2Commands = append(depN1N2Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_not_present") {
				depNotPresentCommands = append(depNotPresentCommands, (&command{name: cmd, args: args}).String())
			}
			return nil
		}
		require.Nil(t, processYaml(ctx, getTestYaml(), false, nil))
		require.Equal(t, 6, len(name1Commands))
		require.Equal(t, 1, len(name2Commands))
		require.Equal(t, 1, len(depN1Commands))
		require.Equal(t, 1, len(depN1N2Commands))
		require.Equal(t, 1, len(depNotPresentCommands))
		// the flags are maintained as map and can be in any sequence
		require.True(t, strings.HasPrefix(name1Commands[0], "drtprod dummy1 name_value1 arg11"))
		require.True(t, strings.Contains(name1Commands[0], "--clouds=gce"))
		require.True(t, strings.Contains(name1Commands[0], "--nodes=1"))
		require.Equal(t, []string{
			"dummy_script1", "dummy_script2 arg11", "drtprod dummy2", "script33", "last_script",
		}, name1Commands[1:])
		require.Equal(t, []string{
			"drtprod dummy2 name_value2 arg12",
		}, name2Commands)
	})
}

func getTestYaml() []byte {
	return []byte(`
environment:
  NAME_1: name_value1
  NAME_2: name_value2

targets:
  - target_name: $NAME_1
    steps:
    - command: dummy1
      args:
        - $NAME_1
        - arg11
      flags:
        clouds: gce
        nodes: 1
      on_rollback:
      - command: rb_dummy1
    - script: "dummy_script1"
      continue_on_failure: True
    - script: "dummy_script2"
      args:
      - arg11
    - command: dummy2
      on_rollback:
      - command: rb_dummy2
        flags:
          flag1: value1
          flag2: value2
        args:
          - arg1
          - arg2
      - script: "dummy_script22"
        flags:
          f1: \"v1 v2\"
    - script: "script33"
      on_rollback:
      - command: script33_rb
    - script: "last_script"
      on_rollback:
      - command: rb_last
  - target_name: $NAME_2
    steps:
    - command: dummy2
      args:
        - $NAME_2
        - arg12
  - target_name: dependent_target_n1
    dependent_targets:
      - $NAME_1
    steps:
    - command: dummy2
      args:
        - $NAME_2
        - arg12
  - target_name: dependent_target_n2_n1
    dependent_targets:
      - $NAME_2
      - name_value1
      - name_value1
    steps:
    - command: dummy2
      args:
        - $NAME_2
        - arg12
  - target_name: dependent_target_not_present
    dependent_targets:
      - not_present
    steps:
    - command: dummy2
      args:
        - $NAME_2
        - arg12
`)
}
