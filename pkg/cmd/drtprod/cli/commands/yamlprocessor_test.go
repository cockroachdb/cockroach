// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/stretchr/testify/require"
)

var cleanupFuncs = make([]func(), 0)

func Test_processYaml(t *testing.T) {
	t.Cleanup(func() {
		// cleanup all once the tests are complete
		for _, f := range cleanupFuncs {
			f()
		}
	})
	ctx := context.Background()
	// setting to nil as a precaution that the command execution does not invoke an
	// actual command
	commandExecutor = nil
	roachprodRun = nil
	roachprodPut = nil
	drtprodLocation = ""
	t.Run("expect unmarshall to fail", func(t *testing.T) {
		err := processYaml(ctx, "", []byte("invalid"), nil, false, nil)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "cannot unmarshal")
	})
	t.Run("expect failure due to unwanted field", func(t *testing.T) {
		err := processYaml(ctx, "", []byte(`
unwanted: value
environment:
  NAME_1: name_value1
  NAME_2: name_value2
`), nil, false, nil)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "field unwanted not found in type commands.yamlConfig")
	})
	t.Run("expect no command execution on display-only=true", func(t *testing.T) {
		require.Nil(t, processYaml(ctx, "", getTestYaml(), nil, true, nil))
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
		err = processYaml(ctx, "", yamlContent, nil, false, nil)
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
		require.Nil(t, processYaml(ctx, "", yamlContent, nil, true, nil))
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
		require.Nil(t, processYaml(ctx, "", yamlContent, nil, false, nil))
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
		err := processYaml(ctx, "", getTestYaml(), nil, false, nil)
		require.NotNil(t, err)
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
		require.Nil(t, processYaml(ctx, "", getTestYaml(), nil, false, nil))
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
	t.Run("run command remotely with missing invalid deployment YAML", func(t *testing.T) {
		commandExecutor = nil
		err := processYaml(ctx, "location/to/test.yaml", getTestYaml(), []byte("invalid content"), false, nil)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "cannot unmarshal")
	})
	t.Run("run command remotely with drtprod that does not exist", func(t *testing.T) {
		commandExecutor = nil
		drtprodLocation = "missing_drtprod"
		err := processYaml(ctx, "location/to/test.yaml", getTestYaml(), getRemoteConfigYaml(), false, nil)
		require.NotNil(t, err)
		require.Equal(t, "missing_drtprod must be available for executing remotely: stat missing_drtprod: no such file or directory",
			err.Error())
	})
	t.Run("run command remotely with display option", func(t *testing.T) {
		commandExecutor = nil
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		err = processYaml(ctx, "location/to/test.yaml", getTestYaml(), getRemoteConfigYaml(), true, nil)
		require.NotNil(t, err)
		require.Equal(t, "display option is not valid for remote execution",
			err.Error())
	})
	t.Run("run command remotely to fail", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return fmt.Errorf("failure in execution")
		}
		err = processYaml(ctx, "location/to/test.yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			getRemoteConfigYaml(), false, nil)
		require.NotNil(t, err)
		// for create error is ignored, so, there are 2 commands.
		require.Equal(t, 1, len(executedCmds))
		require.Equal(t, "drtprod create test-monitor --clouds=gce --gce-image=ubuntu-2204-jammy-v20240319 --gce-machine-type=n2-standard-2 --gce-zones=us-central1-a --lifetime=8760h --nodes=1 --username=drt",
			executedCmds[0])
	})
	t.Run("run command remotely with failure in upload while mkdir", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return nil
		}
		roachprodRun = func(ctx context.Context, l *logger.Logger, clusterName,
			SSHOptions, processTag string, secure bool, stdout, stderr io.Writer,
			cmdArray []string, options install.RunOptions) error {
			require.Equal(t, "test-monitor", clusterName)
			if strings.HasPrefix(cmdArray[0], "mkdir -p") {
				return fmt.Errorf("failed in upload")
			}
			return nil
		}
		roachprodPut = func(ctx context.Context, l *logger.Logger, clusterName, src, dest string, useTreeDist bool) error {
			require.Equal(t, "test-monitor", clusterName)
			return nil
		}
		err = processYaml(ctx, "location/to/test.yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			getRemoteConfigYaml(), false, nil)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "Error while creating directory for file")
		require.Equal(t, 2, len(executedCmds))
	})
	t.Run("run command remotely with failure in upload while put", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return nil
		}
		roachprodRun = func(ctx context.Context, l *logger.Logger, clusterName,
			SSHOptions, processTag string, secure bool, stdout, stderr io.Writer,
			cmdArray []string, options install.RunOptions) error {
			require.Equal(t, "test-monitor", clusterName)
			return nil
		}
		roachprodPut = func(ctx context.Context, l *logger.Logger, clusterName, src, dest string, useTreeDist bool) error {
			require.Equal(t, "test-monitor", clusterName)
			return fmt.Errorf("failed to put the file")
		}
		err = processYaml(ctx, "location/to/test.yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			getRemoteConfigYaml(), false, nil)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "Error while putting file")
		require.Equal(t, 2, len(executedCmds))
	})
	t.Run("run command remotely with failure in mv of drtprod", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return nil
		}
		roachprodRun = func(ctx context.Context, l *logger.Logger, clusterName,
			SSHOptions, processTag string, secure bool, stdout, stderr io.Writer,
			cmdArray []string, options install.RunOptions) error {
			if strings.HasPrefix(cmdArray[0], "sudo mv") {
				return fmt.Errorf("move command failed")
			}
			return nil
		}
		roachprodPut = func(ctx context.Context, l *logger.Logger, clusterName, src, dest string, useTreeDist bool) error {
			require.Equal(t, "test-monitor", clusterName)
			return nil
		}
		err = processYaml(ctx, "location/to/test.yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			getRemoteConfigYaml(), false, nil)
		require.NotNil(t, err)
		require.Equal(t, "move command failed", err.Error())
		require.Equal(t, 2, len(executedCmds))
	})
	t.Run("run command remotely with failure in systemd-run", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		runCmds := make([]string, 0)
		runCmdsLock := syncutil.Mutex{}
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return nil
		}
		roachprodRun = func(ctx context.Context, l *logger.Logger, clusterName,
			SSHOptions, processTag string, secure bool, stdout, stderr io.Writer,
			cmdArray []string, options install.RunOptions) error {
			runCmdsLock.Lock()
			defer runCmdsLock.Unlock()
			runCmds = append(runCmds, cmdArray[0])
			if strings.HasPrefix(cmdArray[0], "sudo systemd-run") {
				return fmt.Errorf("systemd-run command failed")
			}
			return nil
		}
		roachprodPut = func(ctx context.Context, l *logger.Logger, clusterName, src, dest string, useTreeDist bool) error {
			require.Equal(t, "test-monitor", clusterName)
			return nil
		}
		err = processYaml(ctx, "location/to/test.yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			getRemoteConfigYaml(), false, nil)
		require.NotNil(t, err)
		require.Equal(t, "systemd-run command failed", err.Error())
		require.Equal(t, 2, len(executedCmds))
		t.Log(runCmds)
	})
	t.Run("run command remotely with failure in systemd-run", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return nil
		}
		roachprodRun = func(ctx context.Context, l *logger.Logger, clusterName,
			SSHOptions, processTag string, secure bool, stdout, stderr io.Writer,
			cmdArray []string, options install.RunOptions) error {
			require.Equal(t, "test-monitor", clusterName)
			if strings.HasPrefix(cmdArray[0], "sudo systemd-run") {
				return fmt.Errorf("systemd-run command failed")
			}
			return nil
		}
		roachprodPut = func(ctx context.Context, l *logger.Logger, clusterName, src, dest string, useTreeDist bool) error {
			require.Equal(t, "test-monitor", clusterName)
			return nil
		}
		err = processYaml(ctx, "location/to/test.yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			getRemoteConfigYaml(), false, nil)
		require.NotNil(t, err)
		require.Equal(t, "systemd-run command failed", err.Error())
		require.Equal(t, 2, len(executedCmds))
	})
	t.Run("run command remotely with no failure", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		runCmds := make(map[string][]string)
		runCmdsLock := syncutil.Mutex{}
		putCmds := make(map[string]int)
		putCmdsLock := syncutil.Mutex{}
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return nil
		}
		roachprodRun = func(ctx context.Context, l *logger.Logger, clusterName,
			SSHOptions, processTag string, secure bool, stdout, stderr io.Writer,
			cmdArray []string, options install.RunOptions) error {
			require.Equal(t, "test-monitor", clusterName)
			runCmdsLock.Lock()
			defer runCmdsLock.Unlock()
			if strings.HasPrefix(cmdArray[0], "mkdir -p") {
				if _, ok := runCmds["mkdir"]; !ok {
					runCmds["mkdir"] = make([]string, 0)
				}
				runCmds["mkdir"] = append(runCmds["mkdir"], cmdArray[0])
			} else if strings.HasPrefix(cmdArray[0], "sudo mv") {
				if _, ok := runCmds["mv"]; !ok {
					runCmds["mv"] = make([]string, 0)
				}
				runCmds["mv"] = append(runCmds["mv"], cmdArray[0])
			} else if strings.HasPrefix(cmdArray[0], "sudo systemd-run") {
				if _, ok := runCmds["systemd"]; !ok {
					runCmds["systemd"] = make([]string, 0)
				}
				runCmds["systemd"] = append(runCmds["systemd"], cmdArray[0])
			}
			return nil
		}
		roachprodPut = func(ctx context.Context, l *logger.Logger, clusterName, src, dest string, useTreeDist bool) error {
			require.Equal(t, "test-monitor", clusterName)
			putCmdsLock.Lock()
			defer putCmdsLock.Unlock()
			require.Equal(t, src, dest)
			if strings.Contains(src, "drtprod") {
				putCmds["drtprod"] += 1
			} else if strings.Contains(src, "put") {
				putCmds["put"] += 1
			} else if strings.Contains(src, "script") {
				putCmds["script"] += 1
			} else if strings.Contains(src, "yaml") {
				putCmds["yaml"] += 1
			}
			return nil
		}
		require.Nil(t, processYaml(ctx, "location/to/test.yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			getRemoteConfigYaml(), false, nil))
		require.Equal(t, 2, len(executedCmds))
		require.Equal(t, 4, len(putCmds))
		for _, v := range putCmds {
			require.Equal(t, 1, v)
		}
		t.Log(runCmds)
		require.Equal(t, 3, len(runCmds))
		require.Equal(t, 4, len(runCmds["mkdir"]))
		require.Equal(t, 1, len(runCmds["mv"]))
		require.Equal(t, 1, len(runCmds["systemd"]))
		require.Equal(t, "sudo systemd-run --unit test-monitor --same-dir --uid $(id -u) --gid $(id -g) drtprod execute ./location/to/test.yaml",
			runCmds["systemd"][0])
	})
	t.Run("run command remotely with no failure and targets specified", func(t *testing.T) {
		f, err := os.CreateTemp("", "drtprod")
		require.Nil(t, err)
		drtprodLocation = f.Name()
		scriptsDir := os.TempDir()
		executedCmds := make([]string, 0)
		runCmds := make(map[string][]string)
		runCmdsLock := syncutil.Mutex{}
		putCmds := make(map[string]int)
		putCmdsLock := syncutil.Mutex{}
		commandExecutor = func(ctx context.Context, logPrefix string, cmd string, args ...string) error {
			require.Equal(t, "test-monitor", logPrefix)
			executedCmds = append(executedCmds, (&command{name: cmd, args: args}).String())
			return nil
		}
		roachprodRun = func(ctx context.Context, l *logger.Logger, clusterName,
			SSHOptions, processTag string, secure bool, stdout, stderr io.Writer,
			cmdArray []string, options install.RunOptions) error {
			require.Equal(t, "test-monitor", clusterName)
			runCmdsLock.Lock()
			defer runCmdsLock.Unlock()
			if strings.HasPrefix(cmdArray[0], "mkdir -p") {
				if _, ok := runCmds["mkdir"]; !ok {
					runCmds["mkdir"] = make([]string, 0)
				}
				runCmds["mkdir"] = append(runCmds["mkdir"], cmdArray[0])
			} else if strings.HasPrefix(cmdArray[0], "sudo mv") {
				if _, ok := runCmds["mv"]; !ok {
					runCmds["mv"] = make([]string, 0)
				}
				runCmds["mv"] = append(runCmds["mv"], cmdArray[0])
			} else if strings.HasPrefix(cmdArray[0], "sudo systemd-run") {
				if _, ok := runCmds["systemd"]; !ok {
					runCmds["systemd"] = make([]string, 0)
				}
				runCmds["systemd"] = append(runCmds["systemd"], cmdArray[0])
			}
			return nil
		}
		roachprodPut = func(ctx context.Context, l *logger.Logger, clusterName, src, dest string, useTreeDist bool) error {
			require.Equal(t, "test-monitor", clusterName)
			putCmdsLock.Lock()
			defer putCmdsLock.Unlock()
			require.Equal(t, src, dest)
			if strings.Contains(src, "drtprod") {
				putCmds["drtprod"] += 1
			} else if strings.Contains(src, "put") {
				putCmds["put"] += 1
			} else if strings.Contains(src, "script") {
				putCmds["script"] += 1
			} else if strings.Contains(src, "yaml") {
				putCmds["yaml"] += 1
			}
			return nil
		}
		require.Nil(t, processYaml(ctx, "location/to/test.yaml", addRemoteConfig(t, getTestYaml(), scriptsDir),
			getRemoteConfigYaml(), false, []string{"target1"}))
		require.Equal(t, 2, len(executedCmds))
		require.Equal(t, 4, len(putCmds))
		for _, v := range putCmds {
			require.Equal(t, 1, v)
		}
		t.Log(runCmds)
		require.Equal(t, 3, len(runCmds))
		require.Equal(t, 4, len(runCmds["mkdir"]))
		require.Equal(t, 1, len(runCmds["mv"]))
		require.Equal(t, 1, len(runCmds["systemd"]))
		require.Equal(t, "sudo systemd-run --unit test-monitor --same-dir --uid $(id -u) --gid $(id -g) drtprod execute ./location/to/test.yaml -t target1",
			runCmds["systemd"][0])
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
	c := []byte(
		fmt.Sprintf("%s%s",
			string(config),
			fmt.Sprintf(`
dependent_file_locations:
  - %[1]s
  - %[2]s
`, scriptFile.Name(), putFile.Name(),
			)))
	cleanupFuncs = append(cleanupFuncs, func() {
		_ = os.Remove("script2")
	})
	return c
}

func getRemoteConfigYaml() []byte {
	return []byte(`environment:
  ROACHPROD_GCE_DEFAULT_SERVICE_ACCOUNT: 622274581499-compute@developer.gserviceaccount.com
  ROACHPROD_DNS: drt.crdb.io
  ROACHPROD_GCE_DNS_DOMAIN: drt.crdb.io
  ROACHPROD_GCE_DNS_ZONE: drt
  ROACHPROD_GCE_DEFAULT_PROJECT: cockroach-drt
  MONITOR_CLUSTER: <yaml file name>-monitor # this is overwritten with the yaml file name

targets:
  - target_name: $MONITOR_CLUSTER
    steps:
      - command: create
        args:
          - $MONITOR_CLUSTER
        flags:
          clouds: gce
          gce-zones: "us-central1-a"
          nodes: 1
          gce-machine-type: n2-standard-2
          username: drt
          lifetime: 8760h
          gce-image: "ubuntu-2204-jammy-v20240319"
      - command: sync
        flags:
          clouds: gce
`)
}
