package scop

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

//go:generate bash ./generate_visitor.sh scop Backfill backfill.go backfill_visitor.go

type backfillOp struct{ baseOp }

func (backfillOp) Type() Type { return BackfillType }

type IndexBackfill struct {
	backfillOp
	TableID descpb.ID
	IndexID descpb.IndexID
}
