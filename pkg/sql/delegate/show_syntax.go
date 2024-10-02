// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/pgreplparser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
)

// delegateShowSyntax implements SHOW SYNTAX. This statement is usually handled
// as a special case in Executor, but for FROM [SHOW SYNTAX ...] we will arrive
// here too.
func (d *delegator) delegateShowSyntax(n *tree.ShowSyntax) (tree.Statement, error) {
	// Construct an equivalent SELECT query that produces the results:
	//
	// SELECT f AS field, m AS message
	//   FROM (VALUES
	//           ('file',     'foo.go'),
	//           ('line',     '123'),
	//           ('function', 'blix()'),
	//           ('detail',   'some details'),
	//           ('hint',     'some hints')) v(f, m)
	//
	var query bytes.Buffer
	fmt.Fprintf(
		&query, "SELECT f AS %s, m AS %s FROM (VALUES ",
		colinfo.ShowSyntaxColumns[0].Name, colinfo.ShowSyntaxColumns[1].Name,
	)

	// For replication based statements, return nothing for now.
	if d.evalCtx.SessionData().ReplicationMode != sessiondatapb.ReplicationMode_REPLICATION_MODE_DISABLED &&
		pgreplparser.IsReplicationProtocolCommand(n.Statement) {
		return d.parse(fmt.Sprintf(
			`SELECT '' AS %s, '' AS %s FROM generate_series(0, -1) x`,
			colinfo.ShowSyntaxColumns[0].Name,
			colinfo.ShowSyntaxColumns[1].Name,
		))
	}

	comma := ""
	// TODO(knz): in the call below, reportErr is nil although we might
	// want to be able to capture (and report) these errors as well.
	//
	// However, this code path is only used when SHOW SYNTAX is used as
	// a data source, i.e. a client actively uses a query of the form
	// SELECT ... FROM [SHOW SYNTAX ' ... '] WHERE ....  This is not
	// what `cockroach sql` does: the SQL shell issues a straight `SHOW
	// SYNTAX` that goes through the "statement observer" code
	// path. Since we care mainly about what users do in the SQL shell,
	// it's OK if we only deal with that case well for now and, for the
	// time being, forget/ignore errors when SHOW SYNTAX is used as data
	// source. This can be added later if deemed useful or necessary.
	parser.RunShowSyntax(
		d.ctx, n.Statement,
		func(ctx context.Context, field, msg string) {
			fmt.Fprintf(&query, "%s('%s', ", comma, field)
			lexbase.EncodeSQLString(&query, msg)
			query.WriteByte(')')
			comma = ", "
		},
		nil, /* reportErr */
	)
	query.WriteString(") v(f, m)")

	return d.parse(query.String())
}
