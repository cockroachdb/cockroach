// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client"
	provtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/provisionings/types"
	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// buildProvisioningCmd creates the parent "provisioning" command.
func (cr *commandRegistry) buildProvisioningCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "provisioning",
		Aliases: []string{"prov"},
		Short:   "manage infrastructure provisionings",
		Long:    `Commands for managing infrastructure provisionings via OpenTofu templates.`,
	}
	cmd.AddCommand(
		cr.buildProvListCmd(),
		cr.buildProvCreateCmd(),
		cr.buildProvGetCmd(),
		cr.buildProvDestroyCmd(),
		cr.buildProvDeleteCmd(),
		cr.buildProvPlanCmd(),
		cr.buildProvOutputsCmd(),
		cr.buildProvLogsCmd(),
		cr.buildProvExtendCmd(),
		cr.buildProvTemplatesCmd(),
	)
	return cmd
}

// buildProvListCmd lists provisionings with optional filters.
func (cr *commandRegistry) buildProvListCmd() *cobra.Command {
	var (
		envFlag    string
		stateFlag  string
		ownerFlag  string
		outputFlag string
	)

	cmd := &cobra.Command{
		Use:   "list",
		Short: "list provisionings",
		Args:  cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			provs, err := c.ListProvisionings(context.Background(), l,
				client.ListProvisioningsOptions{
					State:       stateFlag,
					Environment: envFlag,
					Owner:       ownerFlag,
				},
			)
			if err != nil {
				return errors.Wrap(err, "list provisionings")
			}

			if outputFlag == "json" {
				return printJSON(provs)
			}

			if len(provs) == 0 {
				fmt.Println("No provisionings found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "ID\tIDENTIFIER\tENV\tTEMPLATE\tSTATE\tOWNER\tEXPIRES\n")
			for _, p := range provs {
				expires := ""
				if p.ExpiresAt != nil {
					expires = p.ExpiresAt.Format("2006-01-02 15:04")
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					p.ID, p.Identifier, p.Environment, p.TemplateType,
					p.State, p.Owner, expires,
				)
			}
			return tw.Flush()
		}),
	}

	cmd.Flags().StringVar(&envFlag, "env", "", "filter by environment")
	cmd.Flags().StringVar(&stateFlag, "state", "", "filter by state")
	cmd.Flags().StringVar(&ownerFlag, "owner", "", "filter by owner")
	cmd.Flags().StringVarP(&outputFlag, "output", "o", "text", "output format (text, json)")
	_ = cmd.RegisterFlagCompletionFunc("env", func(
		cmd *cobra.Command, args []string, toComplete string,
	) ([]string, cobra.ShellCompDirective) {
		return completeEnvironments(toComplete), cobra.ShellCompDirectiveNoFileComp
	})
	_ = cmd.RegisterFlagCompletionFunc("state", func(
		cmd *cobra.Command, args []string, toComplete string,
	) ([]string, cobra.ShellCompDirective) {
		return []string{
			"new", "initializing", "planning", "provisioning",
			"provisioned", "failed", "destroying", "destroyed",
			"destroy_failed",
		}, cobra.ShellCompDirectiveNoFileComp
	})
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvCreateCmd creates a new provisioning.
func (cr *commandRegistry) buildProvCreateCmd() *cobra.Command {
	var (
		templateType string
		envFlag      string
		varFlags     []string
		varFileFlag  string
		lifetimeFlag string
		detachFlag   bool
	)

	cmd := &cobra.Command{
		Use:   "create",
		Short: "create a provisioning",
		Long: `Create a new infrastructure provisioning from a template.

Examples:
  roachprod prov create --type gce-instance --env test --var region=us-east1
  roachprod prov create --type aurora-ec2 --env prod --var-file vars.yaml --lifetime 2h
  roachprod prov create --type gce-instance --env test --var-file vars.yaml --var region=us-west1 -d
`,
		Args: cobra.NoArgs,
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			if templateType == "" {
				return errors.New("--type is required")
			}
			if envFlag == "" {
				return errors.New("--env is required")
			}

			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			ctx := context.Background()

			// Fetch template schema for type coercion.
			var schema map[string]provmodels.TemplateOption
			tmpl, tmplErr := c.GetTemplate(ctx, l, templateType)
			if tmplErr == nil && tmpl != nil {
				schema = tmpl.Variables
			}

			// Parse variables from flags.
			flagVars, err := parseVarFlags(varFlags, schema)
			if err != nil {
				return errors.Wrap(err, "parse --var flags")
			}

			// Parse variables from file if provided.
			vars := flagVars
			if varFileFlag != "" {
				fileVars, err := parseVarFile(varFileFlag)
				if err != nil {
					return err
				}
				vars = mergeVars(fileVars, flagVars)
			}

			input := provtypes.InputCreateProvisioningDTO{
				Environment:  envFlag,
				TemplateType: templateType,
				Variables:    vars,
				Lifetime:     lifetimeFlag,
			}

			resp, err := c.CreateProvisioning(ctx, l, input)
			if err != nil {
				return errors.Wrap(err, "create provisioning")
			}

			if resp.Data != nil {
				fmt.Printf("Provisioning: %s (identifier: %s)\n",
					resp.Data.ID, resp.Data.Identifier)
			}

			if resp.TaskID != "" {
				if detachFlag {
					fmt.Printf("Task: %s (detached)\n", resp.TaskID)
					return nil
				}
				fmt.Printf("Task: %s\nStreaming logs...\n\n", resp.TaskID)
				streamCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				return streamSSELogs(streamCtx, c, resp.TaskID, 0)
			}

			return nil
		}),
	}

	cmd.Flags().StringVar(&templateType, "type", "", "template type (required)")
	cmd.Flags().StringVar(&envFlag, "env", "", "environment name (required)")
	cmd.Flags().StringArrayVar(&varFlags, "var", nil, "variable in key=value format (repeatable)")
	cmd.Flags().StringVar(&varFileFlag, "var-file", "", "path to YAML or JSON variables file")
	cmd.Flags().StringVar(&lifetimeFlag, "lifetime", "", "provisioning lifetime (e.g. 24h)")
	cmd.Flags().BoolVarP(&detachFlag, "detach", "d", false, "do not stream logs after creation")
	_ = cmd.RegisterFlagCompletionFunc("type", func(
		cmd *cobra.Command, args []string, toComplete string,
	) ([]string, cobra.ShellCompDirective) {
		return completeTemplates(toComplete), cobra.ShellCompDirectiveNoFileComp
	})
	_ = cmd.RegisterFlagCompletionFunc("env", func(
		cmd *cobra.Command, args []string, toComplete string,
	) ([]string, cobra.ShellCompDirective) {
		return completeEnvironments(toComplete), cobra.ShellCompDirectiveNoFileComp
	})
	_ = cmd.RegisterFlagCompletionFunc("var", func(
		cmd *cobra.Command, args []string, toComplete string,
	) ([]string, cobra.ShellCompDirective) {
		return completeTemplateVars(templateType, toComplete),
			cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	})
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvGetCmd shows provisioning details.
func (cr *commandRegistry) buildProvGetCmd() *cobra.Command {
	var outputFlag string

	cmd := &cobra.Command{
		Use:   "get <id>",
		Short: "show provisioning details",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			p, err := c.GetProvisioning(context.Background(), l, args[0])
			if err != nil {
				return errors.Wrap(err, "get provisioning")
			}

			if outputFlag == "json" {
				return printJSON(p)
			}

			fmt.Printf("ID:         %s\n", p.ID)
			fmt.Printf("Identifier: %s\n", p.Identifier)
			fmt.Printf("Name:       %s\n", p.Name)
			fmt.Printf("Template:   %s\n", p.TemplateType)
			fmt.Printf("Env:        %s\n", p.Environment)
			fmt.Printf("State:      %s\n", p.State)
			fmt.Printf("Owner:      %s\n", p.Owner)
			fmt.Printf("Created:    %s\n", p.CreatedAt.Format("2006-01-02 15:04:05"))
			fmt.Printf("Updated:    %s\n", p.UpdatedAt.Format("2006-01-02 15:04:05"))
			if p.ExpiresAt != nil {
				fmt.Printf("Expires:    %s\n", p.ExpiresAt.Format("2006-01-02 15:04:05"))
			}
			if p.Error != "" {
				fmt.Printf("Error:      %s\n", p.Error)
			}
			return nil
		}),
	}

	cmd.Flags().StringVarP(&outputFlag, "output", "o", "text", "output format (text, json)")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvDestroyCmd destroys a provisioning.
func (cr *commandRegistry) buildProvDestroyCmd() *cobra.Command {
	var detachFlag bool

	cmd := &cobra.Command{
		Use:   "destroy <id>",
		Short: "destroy a provisioning",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			resp, err := c.DestroyProvisioning(context.Background(), l, args[0])
			if err != nil {
				return errors.Wrap(err, "destroy provisioning")
			}

			fmt.Printf("Provisioning %s destroy initiated.\n", args[0])

			if resp.TaskID != "" {
				if detachFlag {
					fmt.Printf("Task: %s (detached)\n", resp.TaskID)
					return nil
				}
				fmt.Printf("Task: %s\nStreaming logs...\n\n", resp.TaskID)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				return streamSSELogs(ctx, c, resp.TaskID, 0)
			}

			return nil
		}),
	}

	cmd.Flags().BoolVarP(&detachFlag, "detach", "d", false, "do not stream logs")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvDeleteCmd deletes a provisioning record.
func (cr *commandRegistry) buildProvDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <id>",
		Short: "delete a provisioning record",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			if err := c.DeleteProvisioning(
				context.Background(), l, args[0],
			); err != nil {
				return errors.Wrap(err, "delete provisioning")
			}

			fmt.Printf("Provisioning %s deleted.\n", args[0])
			return nil
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvPlanCmd shows the plan for a provisioning.
func (cr *commandRegistry) buildProvPlanCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plan <id>",
		Short: "show provisioning plan",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			plan, err := c.GetProvisioningPlan(context.Background(), l, args[0])
			if err != nil {
				return errors.Wrap(err, "get plan")
			}

			if len(plan) == 0 {
				fmt.Println("No plan available.")
				return nil
			}

			// Pretty-print the JSON plan.
			formatted, err := json.MarshalIndent(plan, "", "  ")
			if err != nil {
				// Fallback to raw output if indentation fails.
				fmt.Println(string(plan))
				return nil //nolint:returnerrcheck
			}
			fmt.Println(string(formatted))
			return nil
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvOutputsCmd shows outputs for a provisioning.
func (cr *commandRegistry) buildProvOutputsCmd() *cobra.Command {
	var outputFlag string

	cmd := &cobra.Command{
		Use:   "outputs <id>",
		Short: "show provisioning outputs",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			outputs, err := c.GetProvisioningOutputs(context.Background(), l, args[0])
			if err != nil {
				return errors.Wrap(err, "get outputs")
			}

			if outputFlag == "json" {
				return printJSON(outputs)
			}

			if len(outputs) == 0 {
				fmt.Println("No outputs available.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "KEY\tVALUE\n")
			for k, v := range outputs {
				fmt.Fprintf(tw, "%s\t%s\n", k, formatValue(v))
			}
			return tw.Flush()
		}),
	}

	cmd.Flags().StringVarP(&outputFlag, "output", "o", "text", "output format (text, json)")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvLogsCmd streams logs for a provisioning task.
func (cr *commandRegistry) buildProvLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logs <task-id>",
		Short: "stream provisioning task logs",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}
			_ = l

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			return streamSSELogs(ctx, c, args[0], 0)
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvExtendCmd extends a provisioning's lifetime.
func (cr *commandRegistry) buildProvExtendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "extend <id>",
		Short: "extend provisioning lifetime",
		Args:  cobra.ExactArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			p, err := c.ExtendProvisioningLifetime(context.Background(), l, args[0])
			if err != nil {
				return errors.Wrap(err, "extend lifetime")
			}

			fmt.Printf("Provisioning %s lifetime extended.\n", p.ID)
			if p.ExpiresAt != nil {
				fmt.Printf("New expiry: %s\n", p.ExpiresAt.Format("2006-01-02 15:04:05"))
			}
			return nil
		}),
	}

	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// buildProvTemplatesCmd lists or shows template details.
func (cr *commandRegistry) buildProvTemplatesCmd() *cobra.Command {
	var outputFlag string

	cmd := &cobra.Command{
		Use:   "templates [<name>]",
		Short: "list templates or show template details",
		Args:  cobra.MaximumNArgs(1),
		Run: Wrap(func(cmd *cobra.Command, args []string) error {
			c, l, err := newAuthClient()
			if err != nil {
				return errors.Wrap(err, "create API client")
			}

			ctx := context.Background()

			if len(args) == 1 {
				// Show template detail.
				tmpl, err := c.GetTemplate(ctx, l, args[0])
				if err != nil {
					return errors.Wrap(err, "get template")
				}

				if outputFlag == "json" {
					return printJSON(tmpl)
				}

				fmt.Printf("Name:             %s\n", tmpl.DirName)
				fmt.Printf("Description:      %s\n", tmpl.Description)
				fmt.Printf("Default Lifetime: %s\n", tmpl.DefaultLifetime)
				fmt.Println()

				if len(tmpl.Variables) > 0 {
					tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
					fmt.Fprintf(tw, "VARIABLE\tTYPE\tREQUIRED\tDESCRIPTION\n")
					for name, opt := range tmpl.Variables {
						fmt.Fprintf(tw, "%s\t%s\t%v\t%s\n",
							name, opt.Type, opt.Required, opt.Description,
						)
					}
					return tw.Flush()
				}
				return nil
			}

			// List all templates.
			templates, err := c.ListTemplates(ctx, l)
			if err != nil {
				return errors.Wrap(err, "list templates")
			}

			if outputFlag == "json" {
				return printJSON(templates)
			}

			if len(templates) == 0 {
				fmt.Println("No templates found.")
				return nil
			}

			tw := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(tw, "NAME\tDESCRIPTION\tDEFAULT LIFETIME\n")
			for _, t := range templates {
				fmt.Fprintf(tw, "%s\t%s\t%s\n",
					t.DirName, t.Description, t.DefaultLifetime,
				)
			}
			return tw.Flush()
		}),
	}

	cmd.Flags().StringVarP(&outputFlag, "output", "o", "text", "output format (text, json)")
	cr.addToExcludeFromBashCompletion(cmd)
	cr.addToExcludeFromClusterFlagsMulti(cmd)

	return cmd
}

// --- Shell completion helpers ---

func completeEnvironments(prefix string) []string {
	c, l, err := newAuthClient()
	if err != nil {
		return nil
	}
	envs, err := c.ListEnvironments(context.Background(), l)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range envs {
		if strings.HasPrefix(e.Name, prefix) {
			names = append(names, e.Name)
		}
	}
	return names
}

func completeTemplates(prefix string) []string {
	c, l, err := newAuthClient()
	if err != nil {
		return nil
	}
	templates, err := c.ListTemplates(context.Background(), l)
	if err != nil {
		return nil
	}
	var names []string
	for _, t := range templates {
		if strings.HasPrefix(t.DirName, prefix) {
			names = append(names, t.DirName)
		}
	}
	return names
}

func completeTemplateVars(templateName, prefix string) []string {
	if templateName == "" {
		return nil
	}
	c, l, err := newAuthClient()
	if err != nil {
		return nil
	}
	tmpl, err := c.GetTemplate(context.Background(), l, templateName)
	if err != nil || tmpl == nil {
		return nil
	}
	var suggestions []string
	for name := range tmpl.Variables {
		suggestion := name + "="
		if strings.HasPrefix(suggestion, prefix) {
			suggestions = append(suggestions, suggestion)
		}
	}
	return suggestions
}

// --- Output formatting helpers ---

// printJSON marshals v as indented JSON and writes it to stdout.
func printJSON(v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal JSON")
	}
	fmt.Println(string(data))
	return nil
}

// formatValue formats a value for display in a table cell. Complex types
// (maps, slices) are JSON-serialized; scalars use their default format.
func formatValue(v interface{}) string {
	switch v.(type) {
	case string, float64, bool, int, int64:
		return fmt.Sprintf("%v", v)
	case nil:
		return ""
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(data)
	}
}

// --- Variable parsing utilities ---

// parseVarFlags parses --var key=value flags with dotted notation support.
// Uses the template schema for type coercion when available.
func parseVarFlags(
	vars []string, schema map[string]provmodels.TemplateOption,
) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, v := range vars {
		parts := strings.SplitN(v, "=", 2)
		if len(parts) != 2 {
			return nil, errors.Newf("invalid variable format %q, expected key=value", v)
		}
		key, rawValue := strings.TrimSpace(parts[0]), parts[1]
		if key == "" {
			return nil, errors.New("variable key must not be empty")
		}
		segments := strings.Split(key, ".")
		if err := setNestedValue(result, segments, rawValue, schema); err != nil {
			return nil, errors.Wrapf(err, "variable %q", key)
		}
	}
	return result, nil
}

// setNestedValue recursively sets a value in a nested map using path segments.
func setNestedValue(
	m map[string]interface{},
	path []string,
	rawValue string,
	schema map[string]provmodels.TemplateOption,
) error {
	if len(path) == 1 {
		m[path[0]] = coerceValue(rawValue, path[0], schema)
		return nil
	}

	key := path[0]
	child, ok := m[key]
	if !ok {
		child = make(map[string]interface{})
		m[key] = child
	}
	childMap, ok := child.(map[string]interface{})
	if !ok {
		return errors.Newf("key %q is not a map", key)
	}

	// Resolve child schema if available.
	var childSchema map[string]provmodels.TemplateOption
	if schema != nil {
		if opt, ok := schema[key]; ok {
			if attrs, ok := opt.Value.(map[string]provmodels.TemplateOption); ok {
				childSchema = attrs
			}
		}
	}

	return setNestedValue(childMap, path[1:], rawValue, childSchema)
}

// coerceValue converts a raw string value to the appropriate Go type based
// on the template schema.
func coerceValue(raw string, key string, schema map[string]provmodels.TemplateOption) interface{} {
	if schema == nil {
		return raw
	}
	opt, ok := schema[key]
	if !ok {
		return raw
	}
	switch opt.Type {
	case "number":
		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			return f
		}
		return raw
	case "bool":
		switch strings.ToLower(raw) {
		case "true", "yes", "1":
			return true
		case "false", "no", "0":
			return false
		}
		return raw
	case "list", "set", "tuple", "object", "map":
		var parsed interface{}
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			return parsed
		}
		return raw
	default:
		return raw
	}
}

// parseVarFile reads variables from a YAML or JSON file.
func parseVarFile(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrapf(err, "read var file %q", path)
	}
	var result map[string]interface{}
	if strings.HasSuffix(path, ".json") {
		err = json.Unmarshal(data, &result)
	} else {
		err = yaml.Unmarshal(data, &result)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "parse var file %q", path)
	}
	return result, nil
}

// mergeVars merges file variables with flag variables. Flag variables take
// precedence.
func mergeVars(fileVars, flagVars map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range fileVars {
		result[k] = v
	}
	for k, v := range flagVars {
		result[k] = v
	}
	return result
}
