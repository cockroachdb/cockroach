// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var licenseAuditCmd = &cobra.Command{
	Use:    "audit",
	Short:  "generate vCPU consumption audit report",
	Hidden: true,
	Long: `
Generate a vCPU consumption audit report for the cluster.

The command connects to a live cluster node and exports consumption data
for all licenses applied to the cluster since creation.

Output formats:
  - yaml (default): Human-readable YAML format
  - json: Machine-readable JSON format for ingestion by external systems

Examples:
  # Generate YAML report
  cockroach license audit --host=localhost:26257 --certs-dir=/certs

  # Generate JSON report
  cockroach license audit --host=localhost:26257 --certs-dir=/certs --format=json > audit.json

  # Using connection URL
  cockroach license audit --url="postgresql://root@localhost:26257?sslmode=verify-full" --format=json
`,
	Args: cobra.NoArgs,
	RunE: clierrorplus.MaybeDecorateError(runLicenseAudit),
}

func runLicenseAudit(_ *cobra.Command, _ []string) error {
	// TODO(sadaf-crl): Implement actual audit logic
	// - Connect to cluster
	// - Query system.vcpu_audit_summary table
	// - Fetch cluster metadata
	// - Format output as YAML or JSON

	format := licenseCtx.auditFormat
	if format != "yaml" && format != "json" {
		return errors.Errorf("invalid format %q: must be 'yaml' or 'json'", format)
	}

	fmt.Printf("License audit report (format: %s)\n", format)
	fmt.Println("TODO: Implement license audit functionality")

	return nil
}

var licenseCmd = &cobra.Command{
	Use:   "license [command]",
	Short: "license management commands",
	Long: `
Commands for managing and auditing CockroachDB licenses.
`,
	RunE: UsageAndErr,
}

func init() {
	licenseCmd.AddCommand(licenseAuditCmd)

	// Add format flag for audit subcommand.
	licenseAuditCmd.Flags().StringVar(
		&licenseCtx.auditFormat,
		"format",
		"yaml",
		"output format (yaml or json)",
	)
}
