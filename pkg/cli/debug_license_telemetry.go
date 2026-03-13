// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const debugLicenseTelemetryAppName = catconstants.InternalAppNamePrefix + " cockroach debug license-telemetry export"

var debugLicenseTelemetryCmd = &cobra.Command{
	Use:   "license-telemetry [command]",
	Short: "license telemetry management commands",
	RunE:  UsageAndErr,
}

var debugLicenseTelemetryExportCmd = &cobra.Command{
	Use:   "export",
	Short: "export license telemetry records from system.license_telemetry",
	Long: `Exports license validation telemetry records as JSON.
This is useful for air-gapped environments where telemetry data
cannot be sent automatically to Cockroach Labs.`,
	RunE: clierrorplus.MaybeDecorateError(runLicenseTelemetryExport),
}

var licenseTelemetryExportOpts = struct {
	Since time.Duration
}{
	Since: 7 * 24 * time.Hour,
}

func runLicenseTelemetryExport(_ *cobra.Command, _ []string) (resErr error) {
	ctx := context.Background()
	sqlConn, err := makeSQLClient(ctx, debugLicenseTelemetryAppName, useSystemDb)
	if err != nil {
		return errors.Wrap(err, "could not establish connection to cluster")
	}
	defer func() { resErr = errors.CombineErrors(resErr, sqlConn.Close()) }()

	since := timeutil.Now().Add(-licenseTelemetryExportOpts.Since)
	rows, err := sqlConn.QueryRow(ctx,
		`SELECT COALESCE(json_agg(event_payload ORDER BY event_timestamp DESC), '[]'::JSONB)
		 FROM system.license_telemetry
		 WHERE event_timestamp >= $1`, since)
	if err != nil {
		return errors.Wrap(err, "querying license telemetry")
	}
	if len(rows) > 0 {
		fmt.Println(rows[0])
	}
	return nil
}
