// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commands

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

func Test_processYaml(t *testing.T) {
	ctx := context.Background()
	// setting to nil as a precaution that the command execution does not invoke an
	// actual command
	commandExecutor = nil
	drtprodLocation = ""
	t.Run("expect unmarshall to fail", func(t *testing.T) {
		err := processYaml(ctx, "", []byte("invalid"), false, nil, false)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "cannot unmarshal")
	})
	t.Run("expect failure due to unwanted field", func(t *testing.T) {
		err := processYaml(ctx, "", []byte(`
unwanted: value
environment:
  NAME_1: name_value1
  NAME_2: name_value2
`), false, nil, false)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "field unwanted not found in type commands.yamlConfig")
	})
	t.Run("expect no command execution on display-only=true", func(t *testing.T) {
		require.Nil(t, processYaml(ctx, "", getTestYaml(), true, nil, false))
	})
	t.Run("expect dependent file check to fail", func(t *testing.T) {
		existingFile, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		yamlContent := []byte(fmt.Sprintf("%s%s",
			fmt.Sprintf(`dependent_file_locations:
  - file_not_present
  - %[1]s
  - another/missing/file
`, existingFile.Name()), getTestYaml()))
		err = processYaml(ctx, "", yamlContent, false, nil, false)
		require.NotNil(t, err)
		require.Equal(t, "dependent files are missing for the YAML: file_not_present, another/missing/file", err.Error())
	})
	t.Run("expect dependent file check to pass for display only", func(t *testing.T) {
		existingFile, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		yamlContent := []byte(fmt.Sprintf("%s%s",
			fmt.Sprintf(`dependent_file_locations:
  - file_not_present
  - %[1]s
  - another/missing/file
`, existingFile.Name()), getTestYaml()))
		require.Nil(t, processYaml(ctx, "", yamlContent, true, nil, false))
	})
	t.Run("expect dependent file to pass", func(t *testing.T) {
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			return nil
		}
		existingFile, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		yamlContent := []byte(fmt.Sprintf("%s%s",
			fmt.Sprintf(`dependent_file_locations:
  - %[1]s
`, existingFile.Name()), getTestYaml()))
		require.Nil(t, processYaml(ctx, "", yamlContent, false, nil, false))
	})
	t.Run("expect partial failure and rollback", func(t *testing.T) {
		name1Commands := make([]string, 0)
		name2Commands := make([]string, 0)
		depN1Commands := make([]string, 0)
		depN1N2Commands := make([]string, 0)
		depN1N2IgnoreFailureCommands := make([]string, 0)
		depNotPresentCommands := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			if strings.HasPrefix(logPrefix, "name_value1") {
				name1Commands = append(name1Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "name_value2") {
				name2Commands = append(name2Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_n1") {
				// expect that "name_value1" is complete by now
				require.Equal(t, 8, len(name1Commands))
				depN1Commands = append(depN1Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_n2_n1") {
				// expect that "name_value1" and "name_value2" is complete by now
				require.Equal(t, 8, len(name1Commands))
				require.Equal(t, 1, len(name2Commands))
				depN1N2Commands = append(depN1N2Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_ignore_failure_n2_n1") {
				// expect that "name_value1" and "name_value2" is complete by now
				require.Equal(t, 8, len(name1Commands))
				require.Equal(t, 2, len(name2Commands))
				depN1N2IgnoreFailureCommands = append(depN1N2IgnoreFailureCommands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_not_present") {
				depNotPresentCommands = append(depNotPresentCommands, (&command{name: cmd, args: args}).String())
			}
			if cmd == "dummy_script1" || cmd == "path/to/script33" || (len(args) > 0 && args[0] == "rb_dummy2") {
				return fmt.Errorf("error while processing script %s", cmd)
			}
			return nil
		}
		require.Nil(t, processYaml(ctx, "", getTestYaml(), false, nil, false))
		require.Equal(t, 8, len(name1Commands))
		require.Equal(t, 0, len(depN1Commands))
		require.Equal(t, 0, len(depN1N2Commands))
		require.Equal(t, 1, len(depNotPresentCommands))
		require.Equal(t, 1, len(depN1N2IgnoreFailureCommands))
		// the flags are maintained as map and can be in any sequence
		require.True(t, strings.HasPrefix(name1Commands[0], "drtprod dummy1 name_value1 arg11"))
		require.True(t, strings.Contains(name1Commands[0], "--clouds=gce"))
		require.True(t, strings.Contains(name1Commands[0], "--nodes=1"))
		require.Equal(t, []string{
			"dummy_script1", "dummy_script2 arg11", "drtprod dummy2", "path/to/script33",
		}, name1Commands[1:5])
		// rollback
		require.True(t, strings.HasPrefix(name1Commands[5], "drtprod rb_dummy2 arg1 arg2"))
		require.True(t, strings.Contains(name1Commands[5], "--flag1=value1"))
		require.True(t, strings.Contains(name1Commands[5], "--flag2=value2"))
		require.True(t, strings.HasPrefix(name1Commands[6], "dummy_script22"))
		require.True(t, strings.Contains(name1Commands[6], "--f1=\\\"v1 v2\\\""))
		require.Equal(t, "drtprod rb_dummy1", name1Commands[7])
		require.Equal(t, []string{
			"drtprod dummy2 name_value2 arg12", "drtprod put name_value2 path/to/file",
		}, name2Commands)
	})
	t.Run("expect no failure", func(t *testing.T) {
		name1Commands := make([]string, 0)
		name2Commands := make([]string, 0)
		depN1Commands := make([]string, 0)
		depN1N2Commands := make([]string, 0)
		depNotPresentCommands := make([]string, 0)
		depN1N2IgnoreFailureCommands := make([]string, 0)
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
				require.Equal(t, 2, len(name2Commands))
				depN1N2Commands = append(depN1N2Commands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_ignore_failure_n2_n1") {
				// expect that "name_value1" and "name_value2" is complete by now
				require.Equal(t, 6, len(name1Commands))
				require.Equal(t, 2, len(name2Commands))
				depN1N2IgnoreFailureCommands = append(depN1N2IgnoreFailureCommands, (&command{name: cmd, args: args}).String())
			} else if strings.HasPrefix(logPrefix, "dependent_target_not_present") {
				depNotPresentCommands = append(depNotPresentCommands, (&command{name: cmd, args: args}).String())
			}
			return nil
		}
		require.Nil(t, processYaml(ctx, "", getTestYaml(), false, nil, false))
		require.Equal(t, 6, len(name1Commands))
		require.Equal(t, 2, len(name2Commands))
		require.Equal(t, 1, len(depN1Commands))
		require.Equal(t, 1, len(depN1N2Commands))
		require.Equal(t, 1, len(depNotPresentCommands))
		require.Equal(t, 1, len(depN1N2IgnoreFailureCommands))
		// the flags are maintained as map and can be in any sequence
		require.True(t, strings.HasPrefix(name1Commands[0], "drtprod dummy1 name_value1 arg11"))
		require.True(t, strings.Contains(name1Commands[0], "--clouds=gce"))
		require.True(t, strings.Contains(name1Commands[0], "--nodes=1"))
		require.Equal(t, []string{
			"dummy_script1", "dummy_script2 arg11", "drtprod dummy2", "path/to/script33", "last_script",
		}, name1Commands[1:])
		require.Equal(t, []string{
			"drtprod dummy2 name_value2 arg12", "drtprod put name_value2 path/to/file",
		}, name2Commands)
	})
	t.Run("run command remotely with drtprod that does not exist", func(t *testing.T) {
		commandExecutor = nil
		drtprodLocation = "missing_drtprod"
		err := processYaml(ctx, "location/to/yaml", getTestYaml(), false, nil, true)
		require.NotNil(t, err)
		require.Equal(t, "missing_drtprod must be available for executing remotely",
			err.Error())
	})
	t.Run("run command remotely with display option", func(t *testing.T) {
		commandExecutor = nil
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		err = processYaml(ctx, "location/to/yaml", getTestYaml(), true, nil, true)
		require.NotNil(t, err)
		require.Equal(t, "display option is not valid for remote execution",
			err.Error())
	})
	t.Run("run command remotely without deployment_name", func(t *testing.T) {
		commandExecutor = nil
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		err = processYaml(ctx, "location/to/yaml", getTestYaml(), false, nil, true)
		require.NotNil(t, err)
		require.Equal(t, "deployment_name is a required configuration for remote execution",
			err.Error())
	})
	t.Run("run command remotely to fail", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "drt-test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return fmt.Errorf("failure in execution")
		}
		require.Nil(t, processYaml(ctx, "location/to/yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			false, nil, true))
		// for create error is ignored, so, there are 2 commands.
		require.Equal(t, 2, len(executedCmds))
	})
	t.Run("run command remotely successfully", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		lock := syncutil.Mutex{}
		executedCmds := make(map[string][]string)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			lock.Lock()
			defer lock.Unlock()
			require.True(t, strings.HasPrefix(logPrefix, "drt-test-monitor"))
			if _, ok := executedCmds[logPrefix]; !ok {
				executedCmds[logPrefix] = make([]string, 0)
			}
			executedCmds[logPrefix] = append(executedCmds[logPrefix], (&command{name: cmd, args: args}).String())
			if logPrefix == "drt-test-monitor" {
				// this is the first command. So, the only expected entry is itself.
				require.Equal(t, 1, len(executedCmds))
			} else if logPrefix == "drt-test-monitor-execute" {
				// all the commands must be executed before invoking execute. So, the count must be 6
				require.Equal(t, 6, len(executedCmds))
				require.Equal(t, 2, len(executedCmds["drt-test-monitor"]))
			} else {
				// all other targets will be executed after the create and sync commands are complete
				cmds, ok := executedCmds["drt-test-monitor"]
				require.True(t, ok)
				require.Equal(t, 2, len(cmds))
			}
			return nil
		}
		require.Nil(t, processYaml(ctx, "location/to/yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			false, nil, true))
		require.Equal(t, 6, len(executedCmds))
		require.Equal(t, 1, len(executedCmds["drt-test-monitor-execute"]))
		require.Equal(t, 2, len(executedCmds["drt-test-monitor-6-0"]))
		require.Equal(t, 2, len(executedCmds["drt-test-monitor-6-2"]))
		require.Equal(t, 1, len(executedCmds["drt-test-monitor-7-0"]))
	})
	t.Run("run command remotely successfully with specified targets", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		lock := syncutil.Mutex{}
		executedCmds := make(map[string][]string)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			lock.Lock()
			defer lock.Unlock()
			require.True(t, strings.HasPrefix(logPrefix, "drt-test-monitor"))
			if _, ok := executedCmds[logPrefix]; !ok {
				executedCmds[logPrefix] = make([]string, 0)
			}
			executedCmds[logPrefix] = append(executedCmds[logPrefix], (&command{name: cmd, args: args}).String())
			if logPrefix == "drt-test-monitor" {
				require.Equal(t, 1, len(executedCmds))
			} else if logPrefix == "drt-test-monitor-execute" {
				require.Equal(t, 5, len(executedCmds))
				require.Equal(t, 2, len(executedCmds["drt-test-monitor"]))
			} else {
				cmds, ok := executedCmds["drt-test-monitor"]
				require.True(t, ok)
				require.Equal(t, 2, len(cmds))
			}
			return nil
		}
		require.Nil(t, processYaml(ctx, "location/to/yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			false, []string{"name_value2", "remote_test_1"}, true))
		require.Equal(t, 5, len(executedCmds))
		require.Equal(t, 1, len(executedCmds["drt-test-monitor-execute"]))
		require.Equal(t, 2, len(executedCmds["drt-test-monitor-6-0"]))
		require.Equal(t, 2, len(executedCmds["drt-test-monitor-6-2"]))
		_, ok := executedCmds["drt-test-monitor-7-0"]
		require.False(t, ok)
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
    - script: "path/to/script33"
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
    - command: put
      args:
        - $NAME_2
        - path/to/file
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
  - target_name: dependent_target_ignore_failure_n2_n1
    dependent_targets:
      - $NAME_2
      - name_value1
      - name_value1
    ignore_dependent_failure: true
    steps:
    - command: dummy3
      args:
        - $NAME_2
        - arg20
  - target_name: dependent_target_not_present
    dependent_targets:
      - not_present
    steps:
    - command: dummy2
      args:
        - $NAME_2
        - arg12`)
}

func addRemoteConfig(t *testing.T, config []byte, fileDir string) []byte {
	scriptFile, err := os.CreateTemp(fileDir, "script1")
	require.Nil(t, err)
	putFile, err := os.CreateTemp(fileDir, "put1")
	require.Nil(t, err)
	_, err = os.Create("script2")
	require.Nil(t, err)
	return []byte(
		fmt.Sprintf("%s%s%s",
			"deployment_name: test",
			string(config),
			fmt.Sprintf(`
  - target_name: remote_test_1
    steps:
    - script: %[1]s
    - command: put
      args:
      - drt-test-monitor
      - missing_file
    - command: put
      args:
      - drt-test-monitor
      - %[2]s
  - target_name: remote_test_2
    steps:
    - script: script2
`, scriptFile.Name(), putFile.Name(),
			)))
}
