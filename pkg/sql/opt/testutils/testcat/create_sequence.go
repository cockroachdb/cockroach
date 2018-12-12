package testcat

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (tc *Catalog) CreateSequence(stmt *tree.CreateSequence) *Sequence {
	tc.qualifyTableName(&stmt.Name)

	seq := &Sequence{
		StableID: tc.nextStableID(),
		SeqName:  stmt.Name,
		Catalog:  tc,
	}

	tc.AddSequence(seq)
	return seq
}
