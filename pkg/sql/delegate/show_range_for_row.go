// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package delegate

import (
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowRangeForRow(n *tree.ShowRangeForRow) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	idx, resName, err := cat.ResolveTableIndex(d.ctx, d.catalog, flags, &n.TableOrIndex)
	if err != nil {
		return nil, err
	}
	if err := d.catalog.CheckPrivilege(d.ctx, idx.Table(), privilege.SELECT); err != nil {
		return nil, err
	}
	span := idx.Span()
	startKey := span.Key

	// TODO (rohany): Its unclear what this check should be in the case of a secondary
	//  index. If the index is not unique, then just the keys that the user provides
	//  isn't enough to get the unique range for the row, because cockroach could
	//  use parts of the primary key in the index to ensure that the keys for each
	//  row are unique.
	if len(n.Key) != idx.KeyColumnCount() {
		return nil, pgerror.New(pgcode.Syntax, "must have an equal number of value as the index")
	}

	// Process the Datums within the expressions.
	var semaCtx tree.SemaContext
	var keyDatums tree.Datums
	for i, expr := range n.Key {
		colTyp := idx.Column(i).DatumType()
		typedExpr, err := sqlbase.SanitizeVarFreeExpr(expr, colTyp, "range-for-row", &semaCtx, false)
		if err != nil {
			return nil, err
		}
		if !tree.IsConst(d.evalCtx, typedExpr) {
			return nil, pgerror.Newf(pgcode.Syntax, "%s: row values must be constant", typedExpr)
		}
		datum, err := typedExpr.Eval(d.evalCtx)
		if err != nil {
			return nil, errors.Wrap(err, typedExpr.String())
		}
		keyDatums = append(keyDatums, datum)
	}

	// Construct what we need to build the key for the row we have.
	var columnIDs []sqlbase.ColumnID
	colIdentityMap := make(map[sqlbase.ColumnID]int)
	var directions []sqlbase.IndexDescriptor_Direction
	for i := 0; i < len(keyDatums); i++ {
		columnIDs = append(columnIDs, sqlbase.ColumnID(idx.Column(i).ColID()))
		colIdentityMap[sqlbase.ColumnID(idx.Column(i).ColID())] = i
		if idx.Column(i).Descending {
			directions = append(directions, sqlbase.IndexDescriptor_DESC)
		} else {
			directions = append(directions, sqlbase.IndexDescriptor_ASC)
		}
	}

	// Build the key for this row.
	resBytes, _, err := sqlbase.EncodeColumns(columnIDs, directions, colIdentityMap, keyDatums, startKey)
	if err != nil {
		return nil, err
	}
	resKey := roachpb.Key(resBytes)
	hexKey := hex.EncodeToString([]byte(resKey))
	idxSpanStart := hex.EncodeToString([]byte(span.Key))
	idxSpanEnd := hex.EncodeToString([]byte(span.EndKey))
	const query = `
SELECT
	CASE WHEN r.start_key < x'%[3]s' THEN NULL ELSE crdb_internal.pretty_key(r.start_key, 2) END AS start_key,
	CASE WHEN r.end_key >= x'%[4]s' THEN NULL ELSE crdb_internal.pretty_key(r.end_key, 2) END AS end_key,
	range_id,
	lease_holder,
	gossip_nodes.locality as lease_holder_locality,
	replicas,
	replica_localities
FROM %[2]s.crdb_internal.ranges AS r
LEFT JOIN %[2]s.crdb_internal.gossip_nodes ON lease_holder = node_id
WHERE (r.start_key <= x'%[1]s')
  AND (r.end_key   > x'%[1]s') ORDER BY r.start_key
	`
	// note: CatalogName.String() != Catalog()
	return parse(fmt.Sprintf(query, hexKey, resName.CatalogName.String(), idxSpanStart, idxSpanEnd))
}
