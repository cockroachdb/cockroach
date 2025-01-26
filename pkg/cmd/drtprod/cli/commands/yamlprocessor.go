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
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/spf13/cobra"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v2"
)

type targetStatus int

const (
	targetResultUnknown targetStatus = iota
	targetResultSuccess              // the target has succeeded
	targetResultFailure              // the target has failed
)

// commandExecutor is responsible for executing the shell commands
var commandExecutor = helpers.ExecuteCmdWithPrefix
var drtprodLocation = "artifacts/drtprod"

// GetYamlProcessor creates a new Cobra command for processing a YAML file.
// The command expects a YAML file as an argument and runs the commands defined in it.
func GetYamlProcessor(ctx context.Context) *cobra.Command {
	displayOnly := false
	useRemoteDeployer := false
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
			return processYamlFile(ctx, yamlFileLocation, displayOnly, useRemoteDeployer, userProvidedTargetNames)
		}),
	}
	cobraCmd.Flags().BoolVarP(&displayOnly,
		"display-only", "d", false, "displays the commands that will be executed without running them")
	cobraCmd.Flags().StringArrayVarP(&userProvidedTargetNames,
		"targets", "t", nil, "the targets to execute. executes all if not mentioned.")
	cobraCmd.Flags().BoolVarP(&useRemoteDeployer,
		"remote", "r", false, "runs the deployment remotely in the deployment node. This is "+
			"useful for long running deployments as the local execution completes soon. A \"deployment_name\" must be mentioned "+
			"in the YAML for running a deployment remotely.")
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
	DeploymentName         string            `yaml:"deployment_name,omitempty"`          // name provided to the deployment
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

func processYamlFile(
	ctx context.Context,
	yamlFileLocation string,
	displayOnly, useRemoteDeployer bool,
	userProvidedTargetNames []string,
) (err error) {
	// Read the YAML file from the specified location
	yamlContent, err := os.ReadFile(yamlFileLocation)
	if err != nil {
		return err
	}
	return processYaml(ctx, yamlFileLocation, yamlContent, displayOnly, userProvidedTargetNames, useRemoteDeployer)
}

// processYaml is responsible for processing the yaml configuration. The YAML commands can be executed
// either locally or in a remote VM based on the flag "useRemoteDeployer".
// If the useRemoteDeployer is set to true, a new VM is created and the current YAML is copied across along
// with the required scripts and binaries and execution is run there. This is useful for long-running deployments
// where the local deployment just delegates the execution of the deployment to the remote machine.
func processYaml(
	ctx context.Context,
	yamlFileLocation string,
	yamlContent []byte,
	displayOnly bool,
	userProvidedTargetNames []string,
	useRemoteDeployer bool,
) error {
	// Unmarshal the YAML content into the yamlConfig struct
	var config yamlConfig
	if err := yaml.UnmarshalStrict(yamlContent, &config); err != nil {
		return err
	}
	if !displayOnly {
		if err := checkForDependentFiles(config.DependentFileLocations); err != nil {
			return err
		}
	}
	if useRemoteDeployer {
		return processYamlRemote(ctx, yamlFileLocation, config, displayOnly, userProvidedTargetNames)
	}
	return processYamlConfig(ctx, config, displayOnly, userProvidedTargetNames)
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
	return fmt.Errorf("dependent files are missing for the YAML: %s",
		strings.Join(missingFiles, ", "))
}

// processYamlRemote executes the YAML remotely
func processYamlRemote(
	ctx context.Context,
	yamlFileLocation string,
	config yamlConfig,
	displayOnly bool,
	userProvidedTargetNames []string,
) (err error) {
	if _, err := os.Stat(drtprodLocation); err != nil {
		// if drtprod binary is not available in artifacts, we cannot proceed as this is needed for executing
		// the YAML remotely
		return fmt.Errorf("%s must be available for executing remotely", drtprodLocation)
	}
	// displayOnly has to be run locally as this is just for displaying the commands
	if displayOnly {
		return fmt.Errorf("display option is not valid for remote execution")
	}
	// DeploymentName is used for identifying the deployment node. The same can be used later for checking the
	// status of the execution and also, run further commands if needed.
	if config.DeploymentName == "" {
		return fmt.Errorf("deployment_name is a required configuration for remote execution")
	}
	deploymentConfig := buildDeploymentYaml(config, yamlFileLocation, userProvidedTargetNames)
	// here processYamlConfig is executing the processYamlConfig which executes the user provided YAML in the remote
	// machine
	return processYamlConfig(ctx, deploymentConfig, false, make([]string, 0))
}

// buildDeploymentYaml builds the deployment YAML responsible for creating the remote VM and executing the YAML
func buildDeploymentYaml(
	config yamlConfig, yamlFileLocation string, userProvidedTargetNames []string,
) yamlConfig {
	// monitorClusterName is used as the cluster name for the 1 node monitor cluster
	monitorClusterName := fmt.Sprintf("drt-%s-monitor", config.DeploymentName)
	// deploymentConfig is created to create the
	deploymentConfig := yamlConfig{
		// environment is configured to be in cockroach-drt project in GCP. As this will be mostly responsible for
		// running commands, it would not matter even if we are deploying clusters on any other cloud.
		Environment: map[string]string{
			"ROACHPROD_GCE_DEFAULT_SERVICE_ACCOUNT": "622274581499-compute@developer.gserviceaccount.com",
			"ROACHPROD_GCE_DNS_DOMAIN":              "drt.crdb.io",
			"ROACHPROD_GCE_DNS_ZONE":                "drt",
			"ROACHPROD_GCE_DEFAULT_PROJECT":         "cockroach-drt",
		},
		Targets: []target{
			{
				// This target is responsible for creating the monitor cluster
				TargetName: monitorClusterName,
				Steps: []step{
					{
						// the first step is to create the monitor cluster with 1 node. This does not need a very powerful
						// machine as we will be mostly running deployment commands from here.
						Command: "create",
						Args:    []string{monitorClusterName},
						Flags: map[string]interface{}{
							"clouds":           "gce",
							"gce-zones":        "us-central1-a",
							"nodes":            "1",
							"gce-machine-type": "n2-standard-2",
							"os-volume-size":   "10",
							"username":         "drt",
							"lifetime":         "8760h",
							"gce-image":        "ubuntu-2204-jammy-v20250112",
						},
						// continue on failure as the same node can be used to execute further commands
						// in which case the cluster would have been already created
						ContinueOnFailure: true,
					},
					{
						// roachprod sync is run to ensure that further roachprod commands can recognise the cluster
						Command: "sync",
						Flags:   map[string]interface{}{"clouds": "gce"},
					},
				},
			},
			{
				// this target is responsible for upload drtprod. this can be run concurrently with other
				// targets as the monitor cluster is ready
				TargetName:       fmt.Sprintf("%s-upload", monitorClusterName),
				DependentTargets: []string{monitorClusterName},
				Steps: []step{
					{
						// drtprod binary is put in the monitor node
						Command: "put",
						Args: []string{
							monitorClusterName, drtprodLocation,
						},
					},
					{
						// the drtprod binary is moved to /usr/bin so that it can be executed from anywhere
						// this is easier that trying to modify the PATH environment
						Command: "run",
						Args: []string{
							monitorClusterName, "--", "sudo mv ./drtprod /usr/bin",
						},
					},
					{
						// the YAML configuration which will be executed is put
						Command: "put",
						Args:    []string{monitorClusterName, yamlFileLocation},
					},
				},
			},
		},
	}
	deploymentConfig = buildDeploymentPartFromTargets(config, deploymentConfig, monitorClusterName, userProvidedTargetNames)
	deploymentConfig = buildYamlExecutionDeployment(yamlFileLocation, userProvidedTargetNames, deploymentConfig, monitorClusterName)
	return deploymentConfig
}

// buildYamlExecutionDeployment builds the final execution command to executing the user provided YAML configuration
// on the monitor node. The execution is run as a system daemon process.
func buildYamlExecutionDeployment(
	yamlFileLocation string,
	userProvidedTargetNames []string,
	deploymentConfig yamlConfig,
	deploymentClusterName string,
) yamlConfig {
	yamlFileName := filepath.Base(yamlFileLocation)
	executeArgs := fmt.Sprintf(
		"sudo systemd-run --unit %s --same-dir --uid $(id -u) --gid $(id -g) drtprod execute ./%s",
		deploymentClusterName,
		yamlFileName)
	if len(userProvidedTargetNames) > 0 {
		// if -t is provided the execution needs to run for the specified targets only
		executeArgs = fmt.Sprintf("%s -t %s", executeArgs, strings.Join(userProvidedTargetNames, " "))
	}
	// this target is dependent on all the targets before this
	dependentTargets := make([]string, 0)
	for _, t := range deploymentConfig.Targets {
		dependentTargets = append(dependentTargets, t.TargetName)
	}
	deploymentConfig.Targets = append(deploymentConfig.Targets, target{
		TargetName:       fmt.Sprintf("%s-execute", deploymentConfig.Targets[0].TargetName),
		DependentTargets: dependentTargets,
		Steps: []step{
			{
				Command: "run",
				Args:    []string{deploymentClusterName, "--", executeArgs},
			},
		},
	})
	return deploymentConfig
}

// buildDeploymentPartFromTargets builds the deployment from the targets defined in the provided YAML config.
// We need this to make the binaries and scripts available in the remote machine during the YAML execution.
// There are only a few ways a user can define externally dependent files:
// 1. it can be a script step
// 2. it can be an input to "roachprod put" command.
// In both the above conditions, the files are uploaded to the required location.
func buildDeploymentPartFromTargets(
	config yamlConfig,
	deploymentConfig yamlConfig,
	deploymentClusterName string,
	userProvidedTargetNames []string,
) yamlConfig {
	userTargetsMap := map[string]struct{}{}
	for _, ut := range userProvidedTargetNames {
		userTargetsMap[ut] = struct{}{}
	}
	for targetCount, t := range config.Targets {
		if _, ok := userTargetsMap[t.TargetName]; len(userProvidedTargetNames) > 0 && !ok {
			// proceed only if user has provided targets and this target is present
			continue
		}

		for stepCount, s := range t.Steps {
			// if a script is found, we have to put the script in the remote VM in the same directory
			if s.Script != "" {
				if _, err := os.Stat(s.Script); err != nil {
					// A script that does not exist should be a system command like "rm", "cp", so those are ignored
					continue
				}
				steps := make([]step, 0)
				if strings.Contains(s.Script, "/") {
					dirLocation := filepath.Dir(s.Script)
					// we need to add the binary in the same location as in the local
					steps =
						append(steps,
							step{
								// this step creates the directory where the script needs to be placed
								Command: "run",
								Args:    []string{deploymentClusterName, "--", fmt.Sprintf("mkdir -p %s", dirLocation)},
							})
				}
				deploymentConfig.Targets = append(deploymentConfig.Targets, target{
					// adding a new target so that we can run these concurrently
					TargetName:       fmt.Sprintf("%s-%d-%d", deploymentConfig.Targets[0].TargetName, targetCount, stepCount),
					DependentTargets: []string{deploymentConfig.Targets[0].TargetName},
					Steps: append(steps, step{
						Command: "put",
						Args:    []string{deploymentClusterName, s.Script, s.Script},
					}),
				})
			} else if s.Command == "put" {
				// if there is a "roachprod put" command, we need to ensure that the binary is put in the remote VM
				// in the same location
				binary := s.Args[1]
				if _, err := os.Stat(binary); err != nil {
					// binary may not exist if it is downloaded by the YAML itself. So, we need not copy in that case
					continue
				}

				steps := make([]step, 0)
				if strings.Contains(binary, "/") {
					dirLocation := filepath.Dir(binary)
					// we need to add the binary in the same location as in the local
					steps = append(steps,
						step{
							// this step creates the directory where the binary needs to be placed
							Command: "run",
							Args:    []string{deploymentClusterName, "--", fmt.Sprintf("mkdir -p %s", dirLocation)},
						})
				}
				deploymentConfig.Targets = append(deploymentConfig.Targets, target{
					// adding a new target so that we can run these concurrently
					TargetName:       fmt.Sprintf("%s-%d-%d", deploymentConfig.Targets[0].TargetName, targetCount, stepCount),
					DependentTargets: []string{deploymentConfig.Targets[0].TargetName},
					Steps: append(steps, step{
						// this step places the binary in the location
						Command: "put",
						Args:    []string{deploymentClusterName, binary, binary},
					}),
				})
			}
		}
	}
	return deploymentConfig
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
			err := waitForDependentTargets(t, waitGroupTracker, sr)
			// dependent targets must be success for executing further commands
			if err == nil {
				err = executeCommands(ctx, t.TargetName, t.commands)
			}
			if err != nil {
				fmt.Printf("[%s] Error executing commands: %v\n", t.TargetName, err)
				sr.setTargetStatus(t.TargetName, targetResultFailure)
				return
			}
			sr.setTargetStatus(t.TargetName, targetResultSuccess)
		}(t)
	}
	// final wait for all targets to complete
	wg.Wait()
	return nil
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
				return fmt.Errorf("[%s] not proceeding as the dependent target %s was not successful", t.TargetName, dt)
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
