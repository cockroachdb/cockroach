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
	table := idx.Table()

	// Process the Datums within the expressions.
	var semaCtx tree.SemaContext
	var rowDatums tree.Datums
	for i, expr := range n.Key {
		colTyp := table.Column(i).DatumType()
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
		rowDatums = append(rowDatums, datum)
	}

	resKey, err := idx.EncodeKey(rowDatums)
	if err != nil {
		return nil, err
	}

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
