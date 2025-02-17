// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commands

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/helpers"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
)

type targetStatus int

const (
	targetResultUnknown targetStatus = iota
	targetResultSuccess              // the target has succeeded
	targetResultFailure              // the target has failed
)

var (
	// commandExecutor is responsible for executing the shell commands
	commandExecutor = helpers.ExecuteCmdWithPrefix

	// roachprodRun executes roachprod.Run. This helps in unit tests
	roachprodRun = roachprod.Run

	// roachprodPut executes roachprod.Put. This helps in unit tests
	roachprodPut = roachprod.Put

	drtprodLocation = "artifacts/drtprod"
)

// GetYamlProcessor creates a new Cobra command for processing a YAML file.
// The command expects a YAML file as an argument and runs the commands defined in it.
func GetYamlProcessor(ctx context.Context) *cobra.Command {
	displayOnly := false
	remoteConfigYaml := ""
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
			_, err := exec.LookPath("drtprod")
			if err != nil {
				// drtprod is needed in the path to run yaml commands
				return err
			}
			yamlFileLocation := args[0]
			return processYamlFile(ctx, yamlFileLocation, displayOnly, remoteConfigYaml, userProvidedTargetNames)
		}),
	}
	cobraCmd.Flags().BoolVarP(&displayOnly,
		"display-only", "d", false, "displays the commands that will be executed without running them.")
	cobraCmd.Flags().StringArrayVarP(&userProvidedTargetNames,
		"targets", "t", nil, "the targets to execute. executes all if not mentioned.")
	cobraCmd.Flags().StringVarP(&remoteConfigYaml,
		"remote-config-yaml", "r", "", "runs the deployment remotely in the monitor node. This is "+
			"useful for long running deployments as the local execution completes after creating a remote cluster. The remote "+
			"config YAML provides the configuration of the remote monitor cluster.")
	return cobraCmd
}

// targetStatusRegistry maintains the registry of target to status mapping
type targetStatusRegistry struct {
	syncutil.Mutex
	registry map[string]targetStatus
}

func (ts *targetStatusRegistry) getTargetStatus(name string) targetStatus {
	ts.Lock()
	defer ts.Unlock()
	if status, ok := ts.registry[name]; ok {
		return status
	}
	return targetResultUnknown
}

func (ts *targetStatusRegistry) setTargetStatus(name string, status targetStatus) {
	// registry will get updated concurrently by different targets and so, we need to ensure we lock
	ts.Lock()
	defer ts.Unlock()
	ts.registry[name] = status
}

// step represents an individual step in the YAML configuration.
// It can include an ActionStep and additional information for error handling and rollback.
type step struct {
	Command string `yaml:"command"` // The command to execute
	Script  string `yaml:"script"`  // The script to execute

	Args              []string               `yaml:"args"`                // Arguments to pass to the command or script
	Flags             map[string]interface{} `yaml:"flags"`               // Flags to pass to the command or script
	ContinueOnFailure bool                   `yaml:"continue_on_failure"` // Whether to continue on failure
	OnRollback        []step                 `yaml:"on_rollback"`         // Steps to execute if rollback is needed
}

// target defines a target cluster with associated steps to be executed.
type target struct {
	TargetName             string   `yaml:"target_name"`              // Name of the target cluster
	DependentTargets       []string `yaml:"dependent_targets"`        // targets should complete before starting this target
	IgnoreDependentFailure bool     `yaml:"ignore_dependent_failure"` // ignore and continue even dependent targets have failed
	Steps                  []step   `yaml:"steps"`                    // Steps to execute on the target cluster
	commands               []*command
}

// yamlConfig represents the structure of the entire YAML configuration file.
type yamlConfig struct {
	DependentFileLocations []string          `yaml:"dependent_file_locations,omitempty"` // location of all the dependent files - scripts/binaries
	Environment            map[string]string `yaml:"environment"`                        // Environment variables to set
	Targets                []target          `yaml:"targets"`                            // List of target clusters with their steps
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

// processYamlFile is responsible for processing the yaml configuration. The YAML commands can be executed
// either locally or in a remote VM based on whether "remoteConfigYaml" is provided or not.
// If the remoteConfigYaml is provided, a new VM is created based on the configuration provided in the remoteConfigYaml
// is created and the current YAML is copied across along with the required scripts and binaries and execution
// is run there. This is useful for long-running deployments where the local deployment just delegates
// the execution of the deployment to the remote machine.
func processYamlFile(
	ctx context.Context,
	yamlFileLocation string,
	displayOnly bool,
	remoteConfigYaml string,
	userProvidedTargetNames []string,
) (err error) {
	if _, err = os.Stat(yamlFileLocation); err != nil {
		// if YAML config file is not available, we cannot proceed
		return errors.Wrapf(err, "%s is not present", yamlFileLocation)
	}
	// Read the YAML file from the specified location
	yamlContent, err := os.ReadFile(yamlFileLocation)
	if err != nil {
		return errors.Wrapf(err, "error reading %s", yamlFileLocation)
	}
	remoteDeployYamlContent := make([]byte, 0)
	if remoteConfigYaml != "" {
		if _, err = os.Stat(remoteConfigYaml); err != nil {
			// if remote YAML config file is not available, we cannot proceed with remote deployment
			return errors.Wrapf(err, "%s is not present", remoteConfigYaml)
		}
		remoteDeployYamlContent, err = os.ReadFile(remoteConfigYaml)
		if err != nil {
			return errors.Wrapf(err, "error reading %s", remoteConfigYaml)
		}
	}
	return processYaml(ctx, yamlFileLocation, yamlContent, remoteDeployYamlContent, displayOnly, userProvidedTargetNames)
}

// processYaml processes the YAML content as explained in processYamlFile. A separate function taking the binary direct
// helps in writing unit tests.
func processYaml(
	ctx context.Context,
	yamlFileLocation string,
	yamlContent, remoteDeployYamlContent []byte,
	displayOnly bool,
	userProvidedTargetNames []string,
) error {
	// Unmarshal the YAML content into the yamlConfig struct
	var clusterConfig yamlConfig
	if err := yaml.UnmarshalStrict(yamlContent, &clusterConfig); err != nil {
		return err
	}
	if !displayOnly {
		if err := checkForDependentFiles(clusterConfig.DependentFileLocations); err != nil {
			return err
		}
	}
	if len(remoteDeployYamlContent) > 0 {
		// Unmarshal the YAML content for the remote deployment and the cluster config will be executed in that remote VM.
		var remoteDeploymentConfig yamlConfig
		if err := yaml.UnmarshalStrict(remoteDeployYamlContent, &remoteDeploymentConfig); err != nil {
			return err
		}
		return processYamlRemote(ctx, yamlFileLocation, clusterConfig, remoteDeploymentConfig, displayOnly, userProvidedTargetNames)
	}
	return processYamlConfig(ctx, clusterConfig, displayOnly, userProvidedTargetNames)
}

// checkForDependentFiles checks if the dependent files are missing and raises an error if not
func checkForDependentFiles(dependentFileLocations []string) error {
	missingFiles := make([]string, 0)
	for _, f := range dependentFileLocations {
		_, err := os.Stat(f)
		if err != nil {
			missingFiles = append(missingFiles, f)
		}
	}
	if len(missingFiles) == 0 {
		return nil
	}
	return errors.Errorf("dependent files are missing for the YAML: %s", strings.Join(missingFiles, ", "))
}

// processYamlRemote executes the YAML remotely
func processYamlRemote(
	ctx context.Context,
	yamlFileLocation string,
	config, remoteDeploymentConfig yamlConfig,
	displayOnly bool,
	userProvidedTargetNames []string,
) (err error) {
	if _, err = os.Stat(drtprodLocation); err != nil {
		// if drtprod binary is not available in artifacts, we cannot proceed as this is needed for executing
		// the YAML remotely
		return errors.Wrapf(err, "%s must be available for executing remotely", drtprodLocation)
	}
	// displayOnly has to be run locally as this is just for displaying the commands
	if displayOnly {
		return errors.Errorf("display option is not valid for remote execution")
	}
	// the MONITOR_CLUSTER is overwritten with the YAML file name
	yamlFileName := strings.Split(filepath.Base(yamlFileLocation), ".")[0]
	monitorClusterName := fmt.Sprintf("%s-monitor", strings.ReplaceAll(yamlFileName, "_", "-"))
	remoteDeploymentConfig.Environment["MONITOR_CLUSTER"] = monitorClusterName
	// processing the remoteDeploymentConfig creates the VM for the remote execution.
	err = processYamlConfig(ctx, remoteDeploymentConfig, false, make([]string, 0))
	if err != nil {
		return err
	}
	// Once the cluster is ready, the dependent files are uploaded
	err = uploadAllDependentFiles(ctx, yamlFileLocation, config, monitorClusterName)
	if err != nil {
		return err
	}
	// The last step is to setup and execute the command on the remote VM
	return setupAndExecute(ctx, yamlFileLocation, userProvidedTargetNames, monitorClusterName)
}

// setupAndExecute moves the drtprod binary to /usr/bin on the remote cluster
// and then starts its execution using systemd, optionally targeting specific user-provided targets.
func setupAndExecute(
	ctx context.Context,
	yamlFileLocation string,
	userProvidedTargetNames []string,
	monitorClusterName string,
) error {
	logger := config.Logger
	// Move the drtprod binary to /usr/bin to ensure it is available system-wide on the cluster.
	err := roachprodRun(ctx, logger, monitorClusterName, "", "", true,
		os.Stdout, os.Stderr,
		[]string{fmt.Sprintf("sudo mv %s /usr/bin", drtprodLocation)},
		install.RunOptions{FailOption: install.FailSlow})
	if err != nil {
		return err
	}

	// Prepare the systemd command to execute the drtprod binary.
	executeArgs := fmt.Sprintf(
		"sudo systemd-run --unit %s --same-dir --uid $(id -u) --gid $(id -g) drtprod execute ./%s",
		monitorClusterName,
		yamlFileLocation)

	// If the user provided specific target names, add them to the execution command.
	if len(userProvidedTargetNames) > 0 {
		executeArgs = fmt.Sprintf("%s -t %s", executeArgs, strings.Join(userProvidedTargetNames, " "))
	}

	// Run the systemd command on the remote cluster.
	return roachprodRun(ctx, logger, monitorClusterName, "", "", true,
		os.Stdout, os.Stderr,
		[]string{executeArgs},
		install.RunOptions{FailOption: install.FailSlow})
}

// uploadAllDependentFiles uploads all dependent files (including the YAML file and others)
// to the specified remote cluster in parallel using goroutines.
// If any error occurs during the upload, it is tracked and returned at the end.
func uploadAllDependentFiles(
	ctx context.Context, yamlFileLocation string, c yamlConfig, monitorClusterName string,
) error {
	logger := config.Logger
	var g errgroup.Group
	// Loop through all dependent files and upload them in parallel.
	for _, fileLocation := range append(c.DependentFileLocations, yamlFileLocation, drtprodLocation) {
		fl := fileLocation
		g.Go(func() error {
			// Create directory on the remote if the file contains a directory path.
			if strings.Contains(fl, "/") {
				dirLocation := filepath.Dir(fl)
				// Use roachprod to create the directory on the remote.
				err := roachprodRun(ctx, logger, monitorClusterName, "", "", true,
					os.Stdout, os.Stderr,
					[]string{fmt.Sprintf("mkdir -p %s", dirLocation)},
					install.RunOptions{FailOption: install.FailSlow})
				if err != nil {
					return errors.Wrapf(err, "Error while creating directory for file %s", fl)
				}
			}
			// Upload the file using roachprod.
			err := roachprodPut(ctx, logger, monitorClusterName, fl, fl, true)
			if err != nil {
				return errors.Wrapf(err, "Error while putting file %s", fl)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	// Log success if all files were uploaded successfully.
	fmt.Printf("All the dependencies are uploaded\n")
	return nil
}

// processYamlConfig executes the YAML configuration. This execution may happen locally or remotely.
// This reads the YAML to set the environment variables, and processes the targets.
func processYamlConfig(
	ctx context.Context, config yamlConfig, displayOnly bool, userProvidedTargetNames []string,
) (err error) {

	// Set the environment variables specified in the YAML
	if err = setEnv(config.Environment, displayOnly); err != nil {
		return err
	}

	// Process the targets defined in the YAML
	return processTargets(ctx, config.Targets, displayOnly, userProvidedTargetNames)
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
	sr := &targetStatusRegistry{
		Mutex:    syncutil.Mutex{},
		registry: make(map[string]targetStatus),
	}
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
	var g errgroup.Group
	for _, theTarget := range targets {
		t := theTarget
		if shouldSkipTarget(targetNameMap, t, userProvidedTargetNames) {
			continue
		}
		g.Go(func() error {
			// defer complete the wait group for the dependent targets to proceed
			defer waitGroupTracker[t.TargetName].Done()
			err := waitForDependentTargets(t, waitGroupTracker, sr)
			// dependent targets must be success for executing further commands
			if err == nil {
				err = executeCommands(ctx, t.TargetName, t.commands)
			}
			if err != nil {
				fmt.Printf("[%s] Error executing commands: %v\n", t.TargetName, err)
				sr.setTargetStatus(t.TargetName, targetResultFailure)
				return errors.Wrapf(err, "target %s failed", t.TargetName)
			}
			sr.setTargetStatus(t.TargetName, targetResultSuccess)
			return nil
		})
	}
	return g.Wait()
}

// waitForDependentTargets waits for the dependent targets and returns an error if ignore_dependent_failure is false
// and any dependent target fails
func waitForDependentTargets(
	t target, waitGroupTracker map[string]*sync.WaitGroup, sr *targetStatusRegistry,
) error {
	for _, dt := range t.DependentTargets {
		if twg, ok := waitGroupTracker[dt]; ok {
			fmt.Printf("[%s] waiting on <%s>\n", t.TargetName, dt)
			// wait on the dependent targets
			// it would not matter if we wait sequentially as all dependent targets need to complete
			twg.Wait()
			//	if we reach here, the dependent target execution has finished. So, there will be an entry in the
			// status registry for the dependent target. The only reason why an entry could be missing is that the
			// dependent target is not selected by the user in -t option.
			// We also need to check if we need to ignore the dependent target failure
			if !t.IgnoreDependentFailure && sr.getTargetStatus(dt) == targetResultFailure {
				fmt.Printf("[%s] Not proceeding as the dependent target %s was not successful.\n", t.TargetName, dt)
				// if the dependent target has failed, the current target is marked as failure
				return errors.Errorf("[%s] not proceeding as the dependent target %s was not successful", t.TargetName, dt)
			}
		}
	}
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

	keys := maps.Keys(step.Flags)
	slices.Sort(keys)

	// Append flags to the command arguments, flags are added in alphabetical order
	for _, key := range keys {
		value := step.Flags[key]
		switch v := value.(type) {
		case string:
			args = append(args, fmt.Sprintf("--%s=%s", key, os.ExpandEnv(v)))
		case []interface{}:
			for _, val := range v {
				if valStr, ok := val.(string); ok {
					args = append(args, fmt.Sprintf("--%s=%s", key, os.ExpandEnv(valStr)))
					continue
				}
				args = append(args, fmt.Sprintf("--%s=%v", key, val))
			}
		default:
			args = append(args, fmt.Sprintf("--%s=%v", key, v))
		}
	}

	return &command{
		name:              name,
		args:              args,
		continueOnFailure: step.ContinueOnFailure,
	}, nil
}
