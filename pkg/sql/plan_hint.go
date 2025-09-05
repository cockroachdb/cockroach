// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// HintSetting adds a plan hint for the query with the given ID which runs a
// SET statement before the query is executed.
func (p *planner) HintSetting(
	ctx context.Context, queryFingerprint, settingName, settingValue string,
) error {
	// Check that the plan_hints table is active.
	if !p.execCfg.Settings.Version.IsActive(ctx, clusterversion.V25_4_AddSystemPlanHintsTable) {
		return errors.New("can't use external plan hints until the upgrade to 24.1 is complete")
	}

	// Check that the setting exists and the provided value is valid.
	settingName = strings.ToLower(settingName)
	err := CheckSessionVariableValueValid(ctx, p.EvalContext().Settings, settingName, settingValue)
	if err != nil {
		return err
	}

	// Add the new hint to the system.plan_hints table.
	planHints := &hints.PlanHints{
		Settings: []*hints.SettingHint{
			{
				SettingName:  settingName,
				SettingValue: settingValue,
			},
		},
	}
	return p.addPlanHints(ctx, queryFingerprint, planHints)
}

// HintAST adds an external plan hint for the query with the given fingerprint
// which applies inline hints from a hinted version of the same query fingerprint.
func (p *planner) HintAST(ctx context.Context, queryFingerprint, hintedFingerprint string) error {
	// Check that the plan_hints table is active.
	if !p.execCfg.Settings.Version.IsActive(ctx, clusterversion.V25_4_AddSystemPlanHintsTable) {
		return errors.New("can't use external plan hints until the upgrade to 25.4 is complete")
	}

	// TODO(drewk): validate that hintedFingerprint has the same structure as
	// queryFingerprint (same table names, join structure, etc.) but with inline hints applied.

	// Add the new hint to the system.plan_hints table.
	planHints := &hints.PlanHints{
		AstHint: &hints.AstHint{
			HintedFingerprint: hintedFingerprint,
		},
	}
	return p.addPlanHints(ctx, queryFingerprint, planHints)
}

func (p *planner) addPlanHints(
	ctx context.Context, queryFingerprint string, planHints *hints.PlanHints,
) error {
	// TODO(drewk): need to normalize the query fingerprint.
	queryHash := hints.FingerprintHashForPlanHints(queryFingerprint)

	// Retrieve the existing hints, if any.
	// TODO(drewk): we assume here and elsewhere that there is only one row with
	// a given hash and fingerprint, but that isn't enforced by the schema.
	var opName redact.RedactableString = "add-plan-hints"
	const getQuery = `SELECT "row_id", "plan_hints" FROM system.plan_hints WHERE "query_hash" = $1 AND "fingerprint" = $2`
	res, err := p.InternalSQLTxn().QueryRow(ctx, opName, p.Txn(), getQuery, queryHash, queryFingerprint)
	if err != nil {
		return err
	}
	if res != nil {
		existingHints, err := hints.NewPlanHints([]byte(tree.MustBeDBytes(res[1])))
		if err != nil {
			return err
		}
		planHints = hints.MergePlanHints(existingHints, planHints)
	}
	hintBytes, err := planHints.ToBytes()
	if err != nil {
		return err
	}
	if res == nil {
		const setStmt = `INSERT INTO system.plan_hints ("query_hash", "row_id", "fingerprint", "plan_hints") VALUES ($1, unique_rowid(), $2, $3)`
		_, err = p.InternalSQLTxn().ExecEx(
			ctx, opName, p.Txn(), sessiondata.NodeUserSessionDataOverride,
			setStmt, queryHash, queryFingerprint, hintBytes,
		)
	} else {
		// Use the row_id from the earlier query. The fingerprint is already set for
		// the row, so no need to update it.
		rowID := tree.MustBeDInt(res[0])
		const setStmt = `UPSERT INTO system.plan_hints ("query_hash", "row_id", "plan_hints") VALUES ($1, $2, $3)`
		_, err = p.InternalSQLTxn().ExecEx(
			ctx, opName, p.Txn(), sessiondata.NodeUserSessionDataOverride,
			setStmt, queryHash, rowID, hintBytes,
		)
	}
	return err
}
