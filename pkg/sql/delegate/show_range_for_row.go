// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowRangeForRow(n *tree.ShowRangeForRow) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	idx, resName, err := cat.ResolveTableIndex(d.ctx, d.catalog, flags, &n.TableOrIndex)
	if err != nil {
		return nil, err
	}
	// Basic requirement is SELECT privileges
	if err = d.catalog.CheckPrivilege(d.ctx, idx.Table(), d.catalog.GetCurrentUser(), privilege.SELECT); err != nil {
		return nil, err
	}
	if idx.Table().IsVirtualTable() {
		return nil, errors.New("SHOW RANGE FOR ROW may not be called on a virtual table")
	}
	// Use qualifyDataSourceNamesInAST similarly to the Builder so that
	// CREATE TABLE AS can source from a delegated expression.
	// For example: CREATE TABLE t2 AS SELECT * FROM [SHOW RANGE FROM TABLE t1 FOR ROW (0)];
	if d.qualifyDataSourceNamesInAST {
		resName.ExplicitSchema = true
		resName.ExplicitCatalog = true
		(n.TableOrIndex).Table = resName.ToUnresolvedObjectName().ToTableName()
	}
	span := idx.Span()
	table := idx.Table()
	idxSpanStart := hex.EncodeToString(span.Key)
	idxSpanEnd := hex.EncodeToString(span.EndKey)

	sqltelemetry.IncrementShowCounter(sqltelemetry.RangeForRow)

	// Format the expressions into a string to be passed into the
	// crdb_internal.encode_key function. We have to be sneaky here and special
	// case when exprs has length 1 and place a comma after the single tuple
	// element so that we can deduce the expression actually has a tuple type for
	// the crdb_internal.encode_key function.
	// Example: exprs = (1)
	// Output when used: crdb_internal.encode_key(x, y, (1,))
	var fmtCtx tree.FmtCtx
	fmtCtx.WriteString("(")
	if len(n.Row) == 1 {
		fmtCtx.FormatNode(n.Row[0])
		fmtCtx.WriteString(",")
	} else {
		fmtCtx.FormatNode(&n.Row)
	}
	fmtCtx.WriteString(")")
	rowString := fmtCtx.String()

	const query = `
SELECT
	CASE
    WHEN r.start_key = crdb_internal.table_span(%[1]d)[1] THEN '…/<TableMin>'
    WHEN r.start_key < crdb_internal.table_span(%[1]d)[1] THEN '<before:'||crdb_internal.pretty_key(r.start_key,-1)||'>'
    ELSE '…'||crdb_internal.pretty_key(r.start_key, 2)
  END AS start_key,
	CASE
    WHEN r.end_key = crdb_internal.table_span(%[1]d)[2] THEN '…/<TableMax>'
    WHEN r.end_key > crdb_internal.table_span(%[1]d)[2] THEN '<after:'||crdb_internal.pretty_key(r.end_key,-1)||'>'
    ELSE '…'||crdb_internal.pretty_key(r.end_key, 2)
  END AS end_key,
	range_id,
	lease_holder,
	replica_localities[array_position(replicas, lease_holder)] as lease_holder_locality,
	replicas,
	replica_localities,
	voting_replicas,
	non_voting_replicas
FROM %[4]s.crdb_internal.ranges AS r
WHERE (r.start_key <= crdb_internal.encode_key(%[1]d, %[2]d, %[3]s))
  AND (r.end_key   >  crdb_internal.encode_key(%[1]d, %[2]d, %[3]s)) ORDER BY r.start_key
	`
	// note: CatalogName.String() != Catalog()
	return d.parse(
		fmt.Sprintf(
			query,
			table.ID(),
			idx.ID(),
			rowString,
			resName.CatalogName.String(),
			idxSpanStart,
			idxSpanEnd,
		),
	)
}
