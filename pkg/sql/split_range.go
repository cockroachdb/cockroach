// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type splitRange struct {
	optColumnsSlot

	rows           planNode
	expirationTime hlc.Timestamp
	run            splitRangeState
}

// splitRangeState contains the run-time state of
// splitRange during local execution.
type splitRangeState struct {
	lastRangeID   roachpb.RangeID
	lastRangeDesc *roachpb.RangeDescriptor
	lastErr       error
}

func (n *splitRange) startExec(runParams) error {
	return nil
}

func (n *splitRange) Next(params runParams) (bool, error) {
	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}
	datum := n.rows.Values()[0]
	if datum == tree.DNull {
		return true, nil
	}
	rangeID := roachpb.RangeID(tree.MustBeDInt(datum))

	rangeDesc, err := split(params, rangeID, n.expirationTime)

	// record the results of the relocation run, so we can output it.
	n.run = splitRangeState{
		lastRangeID:   rangeID,
		lastRangeDesc: rangeDesc,
		lastErr:       err,
	}
	return true, nil
}

func (n *splitRange) Values() tree.Datums {
	// TODO(lunevalex): figure out how not to hard code this and instead
	// get the value from kv/kvserver/split/finder.
	result := "requested, may take 10s to take effect"
	if n.run.lastErr != nil {
		result = n.run.lastErr.Error()
	}
	pretty := ""
	if n.run.lastRangeDesc != nil {
		pretty = keys.PrettyPrint(nil /* valDirs */, n.run.lastRangeDesc.StartKey.AsRawKey())
	}
	return tree.Datums{
		tree.NewDInt(tree.DInt(n.run.lastRangeID)),
		tree.NewDString(pretty),
		tree.NewDString(result),
	}
}

func (n *splitRange) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

func split(
	params runParams, rangeID roachpb.RangeID, expirationTime hlc.Timestamp,
) (*roachpb.RangeDescriptor, error) {
	rangeDesc, err := lookupRangeDescriptorByRangeID(params.ctx, params.extendedEvalCtx.ExecCfg.DB, rangeID)
	if err != nil {
		return nil, errors.Wrapf(err, "error looking up range descriptor")
	}
	if err := params.ExecCfg().DB.AdminLoadBasedSplit(params.ctx, rangeDesc.StartKey, expirationTime); err != nil {
		return rangeDesc, err
	}
	return rangeDesc, nil
}
