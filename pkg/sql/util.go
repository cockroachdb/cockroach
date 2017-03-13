package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// GetTableSpan gets the key span for a SQL table, including any indices.
func GetTableSpan(
	ctx context.Context,
	user string,
	txn *client.Txn,
	dbName, tableName string,
	memMetrics *MemoryMetrics,
) (roachpb.Span, error) {
	p := makeInternalPlanner("get-table-span", txn, user, memMetrics)
	defer finishInternalPlanner(p)

	// Lookup the table ID.
	tn := parser.TableName{DatabaseName: parser.Name(dbName), TableName: parser.Name(tableName)}
	tableID, err := getTableID(ctx, p, &tn)
	if err != nil {
		return roachpb.Span{}, err
	}

	// Determine table data span.
	tablePrefix := keys.MakeTablePrefix(uint32(tableID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	return roachpb.Span{Key: tableStartKey, EndKey: tableEndKey}, nil
}

// getTableID retrieves the table ID for the specified table.
func getTableID(ctx context.Context, p *planner, tn *parser.TableName) (sqlbase.ID, error) {
	if err := tn.QualifyWithDatabase(p.session.Database); err != nil {
		return 0, err
	}

	virtual, err := p.session.virtualSchemas.getVirtualTableDesc(tn)
	if err != nil {
		return 0, err
	}
	if virtual != nil {
		return virtual.GetID(), nil
	}

	dbID, err := p.getDatabaseID(ctx, tn.Database())
	if err != nil {
		return 0, err
	}

	nameKey := tableKey{dbID, tn.Table()}
	key := nameKey.Key()
	gr, err := p.txn.Get(key)
	if err != nil {
		return 0, err
	}
	if !gr.Exists() {
		return 0, sqlbase.NewUndefinedTableError(parser.AsString(tn))
	}
	return sqlbase.ID(gr.ValueInt()), nil
}
