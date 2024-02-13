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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// HintSetting adds a plan hint for the query with the given ID which runs a
// SET statement before the query is executed.
func (p *planner) HintSetting(
	ctx context.Context, queryID uint64, settingName, settingValue string,
) error {
	// Check that the plan_hints table is active.
	if !p.execCfg.Settings.Version.IsActive(ctx, clusterversion.V25_2_AddSystemPlanHintsTable) {
		// TODO(drewk): fix up this error.
		return errors.New("can't use external plan hints until the upgrade to 25.2 is complete")
	}

	// Check that the setting exists and the provided value is valid.
	settingName = strings.ToLower(settingName)
	err := CheckSessionVariableValueValid(ctx, p.EvalContext().Settings, settingName, settingValue)
	if err != nil {
		return err
	}

	// Retrieve the existing hints, if any.
	getQuery := `SELECT "plan_hints" FROM system.plan_hints WHERE "query_id" = $1`
	res, err := p.InternalSQLTxn().QueryRow(ctx, "hint-setting", p.Txn(), getQuery, queryID)
	if err != nil {
		return err
	}
	planHints := &hints.PlanHints{}
	if res != nil {
		err = protoutil.Unmarshal([]byte(tree.MustBeDBytes(res[0])), planHints)
		if err != nil {
			return err
		}
	}
	planHints.Settings = append(planHints.Settings, &hints.SettingHint{
		SettingName:  settingName,
		SettingValue: settingValue,
	})
	hintBytes, err := planHints.ToBytes()
	if err != nil {
		return err
	}
	setStmt := `UPSERT INTO system.plan_hints VALUES ($1, $2)`
	_, err = p.InternalSQLTxn().ExecEx(
		ctx, "hint-setting", p.Txn(), sessiondata.NodeUserSessionDataOverride, setStmt, queryID,
		hintBytes,
	)
	return err
}
