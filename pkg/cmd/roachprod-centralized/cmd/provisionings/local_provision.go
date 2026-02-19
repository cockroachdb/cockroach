// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/tofu"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/vars"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var localProvisionCmd = &cobra.Command{
	Use:   "local-provision",
	Short: "Provision infrastructure locally using a template",
	Long: `Run an OpenTofu template end-to-end for local development and debugging.
This command does not require the API server, database, or config system.
It directly instantiates a template manager and executor from flags.

The command uses a local backend (state stored in the working directory).
Environment resolution is not supported — use --var flags to pass all
variables directly.

Use --destroy to tear down previously provisioned infrastructure in the
same working directory.

This command is permanently useful for template authors to test and debug
templates before deploying them via the service.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		return runLocalProvision(cmd, args)
	},
}

func init() {
	localProvisionCmd.Flags().String(
		"template", "",
		"Template name (directory name or metadata name)",
	)
	localProvisionCmd.Flags().String(
		"identifier", "",
		"8-character identifier for resource naming",
	)
	localProvisionCmd.Flags().StringSlice(
		"var", nil,
		"Variable in key=value format (can be repeated)",
	)
	localProvisionCmd.Flags().Bool(
		"plan-only", false,
		"Stop after showing the plan without applying",
	)
	localProvisionCmd.Flags().Bool(
		"destroy", false,
		"Destroy previously provisioned infrastructure in the working directory",
	)
	localProvisionCmd.Flags().String(
		"working-dir", "",
		"Working directory for terraform files (default: temp dir)",
	)
	localProvisionCmd.Flags().String(
		"tofu-binary", "tofu",
		"Path to OpenTofu binary",
	)
	localProvisionCmd.Flags().String(
		"environment", "local",
		"Environment name (injected if the template declares an 'environment' variable)",
	)
	localProvisionCmd.Flags().String(
		"owner", "local-user",
		"Owner identity (injected if the template declares an 'owner' variable)",
	)

	_ = localProvisionCmd.MarkFlagRequired("template")
	_ = localProvisionCmd.MarkFlagRequired("identifier")
}

func runLocalProvision(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	// Parse flags.
	templateName, _ := cmd.Flags().GetString("template")
	identifier, _ := cmd.Flags().GetString("identifier")
	varSlice, _ := cmd.Flags().GetStringSlice("var")
	planOnly, _ := cmd.Flags().GetBool("plan-only")
	destroy, _ := cmd.Flags().GetBool("destroy")
	workingDir, _ := cmd.Flags().GetString("working-dir")
	tofuBinary, _ := cmd.Flags().GetString("tofu-binary")
	environment, _ := cmd.Flags().GetString("environment")
	owner, _ := cmd.Flags().GetString("owner")

	templatesDir, err := getTemplatesDir(cmd)
	if err != nil {
		return err
	}

	// Parse --var key=value flags into a map.
	userVars, err := parseVarFlags(varSlice)
	if err != nil {
		return err
	}

	l := logger.NewLogger("info")

	// Step 1: Create template manager.
	mgr := templates.NewManager(templatesDir)

	// Step 2: GetTemplate -> get template + parsed variables.
	tmpl, err := mgr.GetTemplate(templateName)
	if err != nil {
		return errors.Wrapf(err, "get template %s", templateName)
	}
	fmt.Printf("Template:    %s (%s)\n", tmpl.Name, tmpl.DirName)
	fmt.Printf("Identifier:  %s\n", identifier)
	fmt.Printf("Environment: %s\n", environment)
	fmt.Printf("Owner:       %s\n", owner)
	fmt.Println()

	// Pre-flight: check that all relative module source paths resolve to
	// existing directories. This catches a common authoring mistake where
	// a symlinked shared module references a sibling that wasn't also
	// symlinked into the template tree.
	if warnings := templates.ValidateModuleReferences(tmpl.Path); len(warnings) > 0 {
		fmt.Println("Module reference warnings:")
		for _, w := range warnings {
			fmt.Printf("  - %s\n", w)
		}
		return errors.Newf(
			"template has %d broken module reference(s); "+
				"ensure all referenced modules are present in the template directory",
			len(warnings),
		)
	}

	if destroy {
		return runLocalDestroy(localDestroyParams{
			ctx:         ctx,
			l:           l,
			tmpl:        tmpl,
			identifier:  identifier,
			environment: environment,
			owner:       owner,
			userVars:    userVars,
			workingDir:  workingDir,
			tofuBinary:  tofuBinary,
		})
	}

	// Step 3: SnapshotTemplate -> archive.
	archive, checksum, err := mgr.SnapshotTemplate(templateName)
	if err != nil {
		return errors.Wrap(err, "snapshot template")
	}
	fmt.Printf("Snapshot:    %d bytes, checksum %s\n", len(archive), checksum[:12]+"...")

	// Step 4: Create working directory. The working directory is
	// intentionally left behind after the command completes (both on
	// success and failure) so the user can inspect state, re-run
	// commands, or use --destroy to tear down.
	if workingDir == "" {
		workingDir, err = os.MkdirTemp("", "local-provision-*")
		if err != nil {
			return errors.Wrap(err, "create temp working directory")
		}
		fmt.Printf("Working dir: %s\n", workingDir)
	} else {
		fmt.Printf("Working dir: %s\n", workingDir)
		if err := os.MkdirAll(workingDir, 0o755); err != nil {
			return errors.Wrapf(err, "create working directory %s", workingDir)
		}
	}

	// Step 5: ExtractSnapshot -> extract template files into working dir.
	if err := templates.ExtractSnapshot(archive, workingDir); err != nil {
		return errors.Wrap(err, "extract snapshot")
	}

	// Step 6: WriteBackendTF with local backend.
	localBackend := templates.NewLocalBackend()
	backendContent := localBackend.GenerateTF("")
	if err := templates.WriteBackendTF(workingDir, backendContent); err != nil {
		return errors.Wrap(err, "write backend.tf")
	}

	// Step 7: BuildVarMaps -> assemble vars and envVars.
	// No environment resolution in the local command — use --var flags only.
	tfVars, envVars, err := vars.BuildVarMaps(vars.BuildVarMapsInput{
		UserVars:       userVars,
		TemplateVars:   tmpl.Variables,
		Identifier:     identifier,
		TemplateType:   tmpl.Name,
		Environment:    environment,
		Owner:          owner,
		BackendEnvVars: localBackend.EnvVars(),
	})
	if err != nil {
		return errors.Wrap(err, "build variable maps")
	}

	// Step 8: Create executor and run init.
	executor := tofu.NewExecutor(tofuBinary)

	fmt.Println()
	fmt.Println("Running tofu init...")
	if err := executor.Init(ctx, l, workingDir, envVars); err != nil {
		return errors.Wrap(err, "tofu init")
	}
	fmt.Println("Init complete.")

	// Step 9: Run plan.
	fmt.Println()
	fmt.Println("Running tofu plan...")
	hasChanges, planJSON, err := executor.Plan(ctx, l, workingDir, tfVars, envVars)
	if err != nil {
		return errors.Wrap(err, "tofu plan")
	}

	if hasChanges {
		fmt.Println("Plan: changes detected.")
	} else {
		fmt.Println("Plan: no changes.")
	}

	// Print a summary of the plan if available.
	printPlanSummary(planJSON)

	// Step 10: If --plan-only, exit.
	if planOnly {
		fmt.Println()
		fmt.Println("Plan-only mode: stopping without applying.")
		return nil
	}

	// Step 11: Apply.
	fmt.Println()
	fmt.Println("Running tofu apply...")
	if err := executor.Apply(ctx, l, workingDir, tfVars, envVars); err != nil {
		return errors.Wrap(err, "tofu apply")
	}
	fmt.Println("Apply complete.")

	// Step 12: Read and display outputs.
	fmt.Println()
	fmt.Println("Reading outputs...")
	outputs, err := executor.Output(ctx, l, workingDir, envVars)
	if err != nil {
		return errors.Wrap(err, "tofu output")
	}

	printOutputs(outputs)

	return nil
}

// localDestroyParams holds parsed parameters for the destroy flow,
// avoiding a long parameter list.
type localDestroyParams struct {
	ctx         context.Context
	l           *logger.Logger
	tmpl        provisionings.Template
	identifier  string
	environment string
	owner       string
	userVars    map[string]interface{}
	workingDir  string
	tofuBinary  string
}

// runLocalDestroy tears down previously provisioned infrastructure.
// Requires --working-dir pointing to an existing directory that was
// used during a previous provision run (contains .tf files and state).
func runLocalDestroy(p localDestroyParams) error {
	if p.workingDir == "" {
		return errors.New("--working-dir is required for --destroy (must point to the directory used during provisioning)")
	}

	// Build the same var maps that were used during provisioning so
	// that tofu destroy can match the resource definitions.
	tfVars, envVars, err := vars.BuildVarMaps(vars.BuildVarMapsInput{
		UserVars:       p.userVars,
		TemplateVars:   p.tmpl.Variables,
		Identifier:     p.identifier,
		TemplateType:   p.tmpl.Name,
		Environment:    p.environment,
		Owner:          p.owner,
		BackendEnvVars: templates.NewLocalBackend().EnvVars(),
	})
	if err != nil {
		return errors.Wrap(err, "build variable maps")
	}

	executor := tofu.NewExecutor(p.tofuBinary)

	fmt.Printf("Working dir: %s\n", p.workingDir)
	fmt.Println()
	fmt.Println("Running tofu init...")
	if err := executor.Init(p.ctx, p.l, p.workingDir, envVars); err != nil {
		return errors.Wrap(err, "tofu init")
	}
	fmt.Println("Init complete.")

	fmt.Println()
	fmt.Println("Running tofu destroy...")
	if err := executor.Destroy(p.ctx, p.l, p.workingDir, tfVars, envVars); err != nil {
		return errors.Wrap(err, "tofu destroy")
	}
	fmt.Println("Destroy complete.")

	// Clean up the working directory after successful destroy.
	fmt.Println()
	fmt.Printf("Removing working directory: %s\n", p.workingDir)
	if err := os.RemoveAll(p.workingDir); err != nil {
		return errors.Wrapf(err, "remove working directory %s", p.workingDir)
	}
	fmt.Println("Cleanup complete.")

	return nil
}

// parseVarFlags parses a slice of "key=value" strings into a
// map[string]interface{}.
func parseVarFlags(varSlice []string) (map[string]interface{}, error) {
	result := make(map[string]interface{}, len(varSlice))
	for _, v := range varSlice {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) != 2 {
			return nil, errors.Newf("invalid --var format %q: expected key=value", v)
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			return nil, errors.New("--var key must not be empty")
		}
		result[key] = parts[1]
	}
	return result, nil
}

// printPlanSummary prints a brief summary of the plan JSON.
func printPlanSummary(planJSON json.RawMessage) {
	if len(planJSON) == 0 {
		return
	}

	var plan struct {
		ResourceChanges []struct {
			Address string `json:"address"`
			Change  struct {
				Actions []string `json:"actions"`
			} `json:"change"`
		} `json:"resource_changes"`
	}
	if err := json.Unmarshal(planJSON, &plan); err != nil {
		return
	}

	if len(plan.ResourceChanges) == 0 {
		return
	}

	fmt.Println()
	fmt.Println("Resource changes:")
	for _, rc := range plan.ResourceChanges {
		actions := strings.Join(rc.Change.Actions, ", ")
		fmt.Printf("  %s: %s\n", rc.Address, actions)
	}
}

// printOutputs prints terraform outputs in a readable format.
func printOutputs(outputs map[string]interface{}) {
	if len(outputs) == 0 {
		fmt.Println("No outputs defined.")
		return
	}

	fmt.Println("Outputs:")
	for name, val := range outputs {
		data, err := json.MarshalIndent(val, "  ", "  ")
		if err != nil {
			fmt.Printf("  %s = %v\n", name, val)
			continue
		}
		fmt.Printf("  %s = %s\n", name, string(data))
	}
}
