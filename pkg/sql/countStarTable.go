package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type countStarTable struct {
	columns sqlbase.ResultColumns

	desc *sqlbase.TableDescriptor
}

func (countStarTable) Next(params runParams) (bool, error) {
	return false, nil
}

func (countStarTable) Values() tree.Datums {
	return nil
}

func (countStarTable) Close(ctx context.Context) {
}
