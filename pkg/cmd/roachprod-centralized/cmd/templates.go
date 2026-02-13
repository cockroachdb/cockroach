// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var templatesCmd = &cobra.Command{
	Use:   "templates",
	Short: "Manage terraform provisioning templates",
	Long: `Commands for discovering and inspecting terraform templates used
for provisioning infrastructure.`,
}

var templatesListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all discovered templates",
	Long: `Scan the templates directory for subdirectories containing a template.yaml
or template.yml marker file and list them.`,
	RunE: runTemplatesList,
}

var templatesInspectCmd = &cobra.Command{
	Use:   "inspect <name>",
	Short: "Show parsed variables for a template",
	Long: `Parse and display all HCL variable declarations from a template,
including type, default value, required/sensitive flags, and description.

Use --output=json for the full structured output with complete default values
and HCL type constraints.`,
	Args: cobra.ExactArgs(1),
	RunE: runTemplatesInspect,
}

func init() {
	rootCmd.AddCommand(templatesCmd)
	templatesCmd.AddCommand(templatesListCmd)
	templatesCmd.AddCommand(templatesInspectCmd)

	templatesCmd.PersistentFlags().String(
		"templates-dir", "",
		"Path to templates directory (overrides ROACHPROD_PROVISIONINGS_TEMPLATES_DIR)",
	)

	templatesInspectCmd.Flags().StringP(
		"output", "o", "text",
		"Output format: text or json",
	)
}

func getTemplatesDir(cmd *cobra.Command) (string, error) {
	dir, _ := cmd.Flags().GetString("templates-dir")
	if dir == "" {
		dir = os.Getenv("ROACHPROD_PROVISIONINGS_TEMPLATES_DIR")
	}
	if dir == "" {
		return "", errors.New(
			"templates directory not set: use --templates-dir flag or " +
				"ROACHPROD_PROVISIONINGS_TEMPLATES_DIR environment variable",
		)
	}
	return dir, nil
}

func runTemplatesList(cmd *cobra.Command, args []string) error {
	dir, err := getTemplatesDir(cmd)
	if err != nil {
		return err
	}

	mgr := templates.NewManager(dir)
	tmplList, err := mgr.ListTemplates()
	if err != nil {
		return errors.Wrap(err, "list templates")
	}

	if len(tmplList) == 0 {
		fmt.Println("No templates found.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DIRECTORY\tNAME\tDESCRIPTION\tVARIABLES")
	for _, t := range tmplList {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", t.DirName, t.Name, t.Description, len(t.Variables))
	}
	return w.Flush()
}

func runTemplatesInspect(cmd *cobra.Command, args []string) error {
	dir, err := getTemplatesDir(cmd)
	if err != nil {
		return err
	}

	outputFmt, _ := cmd.Flags().GetString("output")

	mgr := templates.NewManager(dir)
	tmpl, err := mgr.GetTemplate(args[0])
	if err != nil {
		return errors.Wrapf(err, "get template %s", args[0])
	}

	switch strings.ToLower(outputFmt) {
	case "json":
		return renderInspectJSON(tmpl)
	default:
		return renderInspectText(tmpl)
	}
}

// inspectJSONVariable is the JSON output structure for a single variable.
// It uses GetAsInterface() for the default value so the output contains
// native JSON types instead of the internal TemplateOption tree.
type inspectJSONVariable struct {
	Type        string      `json:"type"`
	FullType    string      `json:"full_type,omitempty"`
	Default     interface{} `json:"default"`
	Required    bool        `json:"required"`
	Sensitive   bool        `json:"sensitive,omitempty"`
	Description string      `json:"description,omitempty"`
}

// inspectJSONOutput is the top-level JSON output for the inspect command.
type inspectJSONOutput struct {
	DirName     string                         `json:"directory"`
	Name        string                         `json:"name"`
	Description string                         `json:"description,omitempty"`
	Variables   map[string]inspectJSONVariable `json:"variables"`
}

func renderInspectJSON(tmpl provisionings.Template) error {
	vars := make(map[string]inspectJSONVariable, len(tmpl.Variables))
	for name, opt := range tmpl.Variables {
		vars[name] = inspectJSONVariable{
			Type:        opt.Type,
			FullType:    opt.FullType,
			Default:     opt.GetAsInterface(),
			Required:    opt.Required,
			Sensitive:   opt.Sensitive,
			Description: opt.Description,
		}
	}

	output := inspectJSONOutput{
		DirName:     tmpl.DirName,
		Name:        tmpl.Name,
		Description: tmpl.Description,
		Variables:   vars,
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(output)
}

func renderInspectText(tmpl provisionings.Template) error {
	fmt.Printf("Directory:   %s\n", tmpl.DirName)
	fmt.Printf("Name:        %s\n", tmpl.Name)
	fmt.Printf("Description: %s\n", tmpl.Description)
	fmt.Printf("Path:        %s\n", tmpl.Path)
	fmt.Println()

	if len(tmpl.Variables) == 0 {
		fmt.Println("No variables declared.")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "VARIABLE\tTYPE\tDEFAULT\tREQUIRED\tSENSITIVE\tDESCRIPTION")
	for name, opt := range tmpl.Variables {
		defaultStr := formatDefault(opt)
		fmt.Fprintf(w, "%s\t%s\t%s\t%t\t%t\t%s\n",
			name, opt.Type, defaultStr, opt.Required, opt.Sensitive, opt.Description,
		)
	}
	return w.Flush()
}

// formatDefault returns a human-readable string representation of a variable's
// default value. Complex types are JSON-serialized.
func formatDefault(opt provisionings.TemplateOption) string {
	if opt.Required {
		return "(required)"
	}
	val := opt.GetAsInterface()
	if val == nil {
		return "null"
	}
	switch v := val.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return truncate(fmt.Sprintf("%v", v), 60)
		}
		return truncate(string(data), 60)
	}
}

func truncate(s string, max int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > max {
		return s[:max-3] + "..."
	}
	return s
}
