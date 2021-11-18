package delegate

import (
	"bytes"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (d *delegator) delegateShowCompletions(n *tree.ShowCompletions) (tree.Statement, error) {
	offsetVal, ok := n.Offset.AsConstantInt()
	if !ok {
		return nil, errors.Newf("invalid offset %v", n.Offset)
	}
	offset, err := strconv.Atoi(offsetVal.String())
	if err != nil {
		return nil, err
	}
	tableNames, err := d.catalog.GetAllTableNames(d.ctx)
	if err != nil {
		return nil, err
	}
	completions := parser.RunShowCompletions(n.Statement, offset, tableNames)

	if len(completions) == 0 {
		return parse(`SELECT '' as completions`)
	}

	var query bytes.Buffer
	fmt.Fprintf(
		&query, "SELECT @1 AS %s FROM (VALUES ",
		"completions",
	)

	comma := ""
	for _, completion := range completions {
		fmt.Fprintf(&query, "%s(", comma)
		lexbase.EncodeSQLString(&query, completion)
		query.WriteByte(')')
		comma = ", "
	}

	fmt.Fprintf(&query, ")")

	return parse(query.String())
}
