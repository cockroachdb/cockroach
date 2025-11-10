// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// createStatementFingerprintsTable creates the system.statement_fingerprints table
// and its associated sequence.
func createStatementFingerprintsTable(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// First create the sequence that will be used by the table
	if err := createSystemTable(
		ctx, d.DB, d.Settings, d.Codec, systemschema.StatementFingerprintIDSequence, tree.LocalityLevelTable,
	); err != nil {
		return err
	}
	
	// Then create the table that uses the sequence
	return createSystemTable(
		ctx, d.DB, d.Settings, d.Codec, systemschema.StatementFingerprintsTable, tree.LocalityLevelTable,
	)
}
