package job

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type newSchemaChangeResumer struct {
	jobID int64
}

func (n newSchemaChangeResumer) Resume(
	ctx context.Context, execCtx interface{}, resultsCh chan<- tree.Datums,
) error {
	panic("unimplemented")
}

func (n newSchemaChangeResumer) OnFailOrCancel(ctx context.Context, execCtx interface{}) error {
	panic("unimplemented")
}
