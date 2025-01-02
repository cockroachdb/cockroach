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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/helpers"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

// commandExecutor is responsible for executing the shell commands
var commandExecutor = helpers.ExecuteCmdWithPrefix

// GetYamlProcessor creates a new Cobra command for processing a YAML file.
// The command expects a YAML file as an argument and runs the commands defined in it.
func GetYamlProcessor(ctx context.Context) *cobra.Command {
	displayOnly := false
	userProvidedTargetNames := make([]string, 0)
	cobraCmd := &cobra.Command{
		Use:   "execute <yaml file> [flags]",
		Short: "Executes the commands in sequence as specified in the YAML",
		Long: `Executes the commands in sequence as specified in the YAML.
You can also specify the rollback commands in case of a step failure.
`,
		Args: cobra.ExactArgs(1),
		// Wraps the command execution with additional error handling
		Run: helpers.Wrap(func(cmd *cobra.Command, args []string) (retErr error) {
			yamlFileLocation := args[0]
			// Read the YAML file from the specified location
			yamlContent, err := os.ReadFile(yamlFileLocation)
			if err != nil {
				return err
			}
			return processYaml(ctx, yamlContent, displayOnly, userProvidedTargetNames)
		}),
	}
	cobraCmd.Flags().BoolVarP(&displayOnly,
		"display-only", "d", false, "displays the commands that will be executed without running them")
	cobraCmd.Flags().StringArrayVarP(&userProvidedTargetNames,
		"targets", "t", nil, "the targets to execute. executes all if not mentioned.")
	return cobraCmd
}

// step represents an individual step in the YAML configuration.
// It can include an ActionStep and additional information for error handling and rollback.
type step struct {
	Command string `yaml:"command"` // The command to execute
	Script  string `yaml:"script"`  // The script to execute

	Args              []string          `yaml:"args"`                // Arguments to pass to the command or script
	Flags             map[string]string `yaml:"flags"`               // Flags to pass to the command or script
	ContinueOnFailure bool              `yaml:"continue_on_failure"` // Whether to continue on failure
	OnRollback        []step            `yaml:"on_rollback"`         // Steps to execute if rollback is needed
}

// target defines a target cluster with associated steps to be executed.
type target struct {
	TargetName       string   `yaml:"target_name"`       // Name of the target cluster
	DependentTargets []string `yaml:"dependent_targets"` // targets should complete before starting this target
	Steps            []step   `yaml:"steps"`             // Steps to execute on the target cluster
	commands         []*command
}

// yamlConfig represents the structure of the entire YAML configuration file.
type yamlConfig struct {
	Environment map[string]string `yaml:"environment"` // Environment variables to set
	Targets     []target          `yaml:"targets"`     // List of target clusters with their steps
}

// command is a simplified representation of a shell command that needs to be executed.
type command struct {
	name              string     // Command name
	args              []string   // Command arguments
	continueOnFailure bool       // Whether to continue on failure
	rollbackCmds      []*command // Rollback commands to execute in case of failure
}

// String returns the command as a string for easy printing.
func (c *command) String() string {
	cmdStr := c.name
	for _, arg := range c.args {
		cmdStr = fmt.Sprintf("%s %s", cmdStr, arg)
	}
	return cmdStr
}

// processYaml reads the YAML file, parses it, sets the environment variables, and processes the targets.
func processYaml(
	ctx context.Context, yamlContent []byte, displayOnly bool, userProvidedTargetNames []string,
) (err error) {

	// Unmarshal the YAML content into the yamlConfig struct
	var config yamlConfig
	if err = yaml.UnmarshalStrict(yamlContent, &config); err != nil {
		return err
	}

	// Set the environment variables specified in the YAML
	if err = setEnv(config.Environment, displayOnly); err != nil {
		return err
	}

	// Process the targets defined in the YAML
	if err = processTargets(ctx, config.Targets, displayOnly, userProvidedTargetNames); err != nil {
		return err
	}

	return nil
}

// setEnv sets the environment variables as defined in the YAML configuration.
func setEnv(environment map[string]string, displayOnly bool) error {
	for key, value := range environment {
		if displayOnly {
			fmt.Printf("export %s=%s\n", key, value)
		} else {
			fmt.Printf("Setting env %s to %s\n", key, value)
		}
		// setting the environment for display only as well. This is because
		// the environment will be used in the yaml as well.
		err := os.Setenv(key, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// processTargets processes each target defined in the YAML configuration.
// It generates commands for each target and executes them concurrently.
func processTargets(
	ctx context.Context, targets []target, displayOnly bool, userProvidedTargetNames []string,
) error {
	// targetNameMap is used to check all targets that are provided as user input
	targetNameMap := make(map[string]struct{})
	for _, tn := range userProvidedTargetNames {
		targetNameMap[tn] = struct{}{}
	}
	waitGroupTracker, err := buildTargetCmdsAndRegisterWaitGroups(targets, targetNameMap, userProvidedTargetNames)
	if err != nil {
		return err
	}

	// if displayOnly, we just print and exit
	if displayOnly {
		for _, t := range targets {
			if !shouldSkipTarget(targetNameMap, t, userProvidedTargetNames) {
				displayCommands(t)
			}
		}
		return nil
	}
	// Use a WaitGroup to wait for commands executed concurrently
	wg := sync.WaitGroup{}
	for _, t := range targets {
		if shouldSkipTarget(targetNameMap, t, userProvidedTargetNames) {
			continue
		}
		wg.Add(1)
		go func(t target) {
			// defer complete the wait group for the dependent targets to proceed
			defer waitGroupTracker[t.TargetName].Done()
			defer wg.Done()
			for _, dt := range t.DependentTargets {
				if twg, ok := waitGroupTracker[dt]; ok {
					fmt.Printf("%s: waiting on <%s>\n", t.TargetName, dt)
					// wait on the dependent targets
					// it would not matter if we wait sequentially as all dependent targets need to complete
					twg.Wait()
				}
			}
			err := executeCommands(ctx, t.TargetName, t.commands)
			if err != nil {
				fmt.Printf("%s: Error executing commands: %v\n", t.TargetName, err)
			}
		}(t)
	}
	// final wait for all targets to complete
	wg.Wait()
	return nil
}

// shouldSkipTarget returns true if the target should be skipped
func shouldSkipTarget(
	targetNameMap map[string]struct{}, t target, userProvidedTargetNames []string,
) bool {
	_, ok := targetNameMap[t.TargetName]
	// the targets provided in "--targets" does not contain the current target
	// so, this target is skipped
	return len(userProvidedTargetNames) > 0 && !ok
}

// buildTargetCmdsAndRegisterWaitGroups builds the commands per target and registers the target to a wait group
// tracker and returns the same.
// The wait group tracker is a map of target name to a wait group. A delta is added to the wait group that is
// marked done when the specific target is complete. The wait group is use by the dependent targets to wait for
// the completion of the target.
func buildTargetCmdsAndRegisterWaitGroups(
	targets []target, targetNameMap map[string]struct{}, userProvidedTargetNames []string,
) (map[string]*sync.WaitGroup, error) {
	// map of target name to a wait group. The wait group is used by dependent target to wait for the target to complete
	waitGroupTracker := make(map[string]*sync.WaitGroup)

	// iterate over all the targets and create all the commands that should be executed for the target
	for i := 0; i < len(targets); i++ {
		// expand the environment variables
		targets[i].TargetName = os.ExpandEnv(targets[i].TargetName)
		t := targets[i]
		for j := 0; j < len(t.DependentTargets); j++ {
			targets[i].DependentTargets[j] = os.ExpandEnv(targets[i].DependentTargets[j])
		}
		if shouldSkipTarget(targetNameMap, t, userProvidedTargetNames) {
			fmt.Printf("Ignoring execution for target %s\n", t.TargetName)
			continue
		}
		// add a delta wait for this target. This is added here so that when the execution loop is run, we need not
		// worry about the sequence
		waitGroupTracker[t.TargetName] = &sync.WaitGroup{}
		waitGroupTracker[t.TargetName].Add(1)
		// Generate the commands for each target's steps
		targetSteps, err := generateCmdsFromSteps(t.TargetName, t.Steps)
		if err != nil {
			return waitGroupTracker, err
		}
		targets[i].commands = targetSteps
	}
	return waitGroupTracker, nil
}

// displayCommands prints the commands in stdout
func displayCommands(t target) {
	if len(t.DependentTargets) > 0 {
		fmt.Printf("For target <%s> after [%s]:\n", t.TargetName, strings.Join(t.DependentTargets, ", "))
	} else {
		fmt.Printf("For target <%s>:\n", t.TargetName)
	}
	for _, cmd := range t.commands {
		fmt.Printf("|-> %s\n", cmd)
		for _, rCmd := range cmd.rollbackCmds {
			fmt.Printf("    |-> (Rollback) %s\n", rCmd)
		}
	}
}

// executeCommands runs the list of commands for a specific target.
// It handles output streaming and error management.
func executeCommands(ctx context.Context, logPrefix string, cmds []*command) error {
	// rollbackCmds maintains a list of commands to be executed in case of a failure
	rollbackCmds := make([]*command, 0)

	// Defer rollback execution if any rollback commands are added
	defer func() {
		if len(rollbackCmds) > 0 {
			_ = executeCommands(ctx, fmt.Sprintf("%s:Rollback", logPrefix), rollbackCmds)
		}
	}()

	for _, cmd := range cmds {
		fmt.Printf("[%s] Starting <%v>\n", logPrefix, cmd)
		err := commandExecutor(ctx, logPrefix, cmd.name, cmd.args...)
		if err != nil {
			if !cmd.continueOnFailure {
				// Return the error if not configured to continue on failure
				return err
			}
			// Log the failure and continue if configured to do so
			fmt.Printf("[%s] Failed <%v>, Error Ignored: %v\n", logPrefix, cmd, err)
		} else {
			fmt.Printf("[%s] Completed <%v>\n", logPrefix, cmd)
		}

		// Add rollback commands if specified
		if len(cmd.rollbackCmds) > 0 {
			for i := 0; i < len(cmd.rollbackCmds); i++ {
				// rollback command failures are ignored
				cmd.rollbackCmds[i].continueOnFailure = true
			}
			rollbackCmds = append(cmd.rollbackCmds, rollbackCmds...)
		}
	}
	// Clear rollback commands if all commands executed successfully
	rollbackCmds = make([]*command, 0)
	return nil
}

// generateCmdsFromSteps generates the commands to be executed for a given cluster and steps.
func generateCmdsFromSteps(clusterName string, steps []step) ([]*command, error) {
	cmds := make([]*command, 0)
	for _, s := range steps {
		// Generate a command from each step
		cmd, err := generateStepCmd(clusterName, s)
		if err != nil {
			return nil, err
		}
		if cmd == nil {
			continue
		}
		cmds = append(cmds, cmd)
	}
	return cmds, nil
}

// generateStepCmd generates a command for a given step within a target.
// It handles both command-based and script-based steps.
func generateStepCmd(clusterName string, s step) (*command, error) {
	var cmd *command
	var err error

	// Generate the command based on whether it's a command or a script
	if s.Command != "" {
		cmd, err = generateCmdFromCommand(s, clusterName)
	} else if s.Script != "" {
		cmd, err = generateCmdFromScript(s, clusterName)
	}

	if err != nil {
		return nil, err
	}

	// Generate rollback commands if specified
	if len(s.OnRollback) > 0 {
		cmd.rollbackCmds, err = generateCmdsFromSteps(clusterName, s.OnRollback)
		if err != nil {
			return nil, err
		}
	}
	return cmd, err
}

// generateCmdFromCommand creates a command from a step that uses a command.
func generateCmdFromCommand(s step, _ string) (*command, error) {
	// Prepend the cluster name to the command arguments
	s.Args = append([]string{s.Command}, s.Args...)
	return getCommand(s, "drtprod")
}

// generateCmdFromScript creates a command from a step that uses a script.
func generateCmdFromScript(s step, _ string) (*command, error) {
	return getCommand(s, s.Script)
}

// getCommand constructs the final command with all arguments and flags.
func getCommand(step step, name string) (*command, error) {
	args := make([]string, 0)
	for _, arg := range step.Args {
		args = append(args, os.ExpandEnv(arg))
	}

	// Append flags to the command arguments
	for key, value := range step.Flags {
		args = append(args, fmt.Sprintf("--%s=%s", key, os.ExpandEnv(value)))
	}

	return &command{
		name:              name,
		args:              args,
		continueOnFailure: step.ContinueOnFailure,
	}, nil
}
