// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"os"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

// Cmd is the parent command for all provisioning-related subcommands.
// It owns the --templates-dir PersistentFlag, inherited by templates
// and local-provision subcommands.
var Cmd = &cobra.Command{
	Use:   "provisionings",
	Short: "Provisioning infrastructure management",
	Long: `Commands for managing terraform-based provisioning infrastructure,
including template management and local provisioning for development.`,
}

func init() {
	Cmd.AddCommand(templatesCmd)
	Cmd.AddCommand(localProvisionCmd)

	Cmd.PersistentFlags().String(
		"templates-dir", "",
		"Path to templates directory (overrides ROACHPROD_PROVISIONINGS_TEMPLATES_DIR)",
	)
}

// getTemplatesDir resolves the templates directory from the --templates-dir
// PersistentFlag (defined on Cmd and inherited by all subcommands) or the
// ROACHPROD_PROVISIONINGS_TEMPLATES_DIR environment variable.
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
