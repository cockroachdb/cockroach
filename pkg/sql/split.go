// Copyright 2016 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type splitNode struct {
	optColumnsSlot

	tableDesc      catalog.TableDescriptor
	index          catalog.Index
	rows           planNode
	run            splitRun
	expirationTime hlc.Timestamp
}

// splitRun contains the run-time state of splitNode during local execution.
type splitRun struct {
	lastSplitKey       []byte
	lastExpirationTime hlc.Timestamp
}

func (n *splitNode) startExec(params runParams) error {
	return nil
}

func (n *splitNode) Next(params runParams) (bool, error) {
	// TODO(radu): instead of performing the splits sequentially, accumulate all
	// the split keys and then perform the splits in parallel (e.g. split at the
	// middle key and recursively to the left and right).

	if ok, err := n.rows.Next(params); err != nil || !ok {
		return ok, err
	}

	rowKey, err := getRowKey(params.ExecCfg().Codec, n.tableDesc, n.index, n.rows.Values())
	if err != nil {
		return false, err
	}

	if err := params.ExecCfg().DB.AdminSplit(params.ctx, rowKey, n.expirationTime); err != nil {
		return false, err
	}

	n.run.lastSplitKey = rowKey
	n.run.lastExpirationTime = n.expirationTime

	return true, nil
}

func (n *splitNode) Values() tree.Datums {
	splitEnforcedUntil := tree.DNull
	if !n.run.lastExpirationTime.IsEmpty() {
		splitEnforcedUntil = tree.TimestampToInexactDTimestamp(n.run.lastExpirationTime)
	}
	return tree.Datums{
		tree.NewDBytes(tree.DBytes(n.run.lastSplitKey)),
		tree.NewDString(catalogkeys.PrettyKey(nil /* valDirs */, n.run.lastSplitKey, 2)),
		splitEnforcedUntil,
	}
}

func (n *splitNode) Close(ctx context.Context) {
	n.rows.Close(ctx)
}

// getRowKey generates a key that corresponds to a row (or prefix of a row) in a table or index.
// Both tableDesc and index are required (index can be the primary index).
func getRowKey(
	codec keys.SQLCodec, tableDesc catalog.TableDescriptor, index catalog.Index, values []tree.Datum,
) ([]byte, error) {
	if index.NumKeyColumns() < len(values) {
		return nil, pgerror.Newf(pgcode.Syntax, "excessive number of values provided: expected %d, got %d", index.NumKeyColumns(), len(values))
	}
	var colMap catalog.TableColMap
	for i := range values {
		colMap.Set(index.GetKeyColumnID(i), i)
	}
	prefix := rowenc.MakeIndexKeyPrefix(codec, tableDesc.GetID(), index.GetID())
	keyCols := tableDesc.IndexFetchSpecKeyAndSuffixColumns(index)
	key, _, err := rowenc.EncodePartialIndexKey(keyCols[:len(values)], colMap, values, prefix)
	if err != nil {
		return nil, err
	}
	return key, nil
}

// parseExpriationTime parses an expression into a hlc.Timestamp representing
// the expiration time of the split.
func parseExpirationTime(
	evalCtx *tree.EvalContext, expireExpr tree.TypedExpr,
) (hlc.Timestamp, error) {
	if !tree.IsConst(evalCtx, expireExpr) {
		return hlc.Timestamp{}, errors.Errorf("SPLIT AT: only constant expressions are allowed for expiration")
	}
	d, err := expireExpr.Eval(evalCtx)
	if err != nil {
		return hlc.Timestamp{}, err
	}
	if d == tree.DNull {
		return hlc.MaxTimestamp, nil
	}
	stmtTimestamp := evalCtx.GetStmtTimestamp()
	ts, err := tree.DatumToHLC(evalCtx, stmtTimestamp, d)
	if err != nil {
		return ts, errors.Wrap(err, "SPLIT AT")
	}
	if ts.GoTime().Before(stmtTimestamp) {
		return ts, errors.Errorf("SPLIT AT: expiration time should be greater than or equal to current time")
	}
	return ts, nil
}
