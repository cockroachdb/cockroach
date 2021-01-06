package descriptorutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

func GetIndexMutation(
	table catalog.TableDescriptor, idxID descpb.IndexID,
) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
	mutations := table.GetMutations()
	for i := range mutations {
		mut := &mutations[i]
		idx := mut.GetIndex()
		if idx != nil && idx.ID == idxID {
			return mut, i, nil
		}
	}
	return nil, 0, errors.AssertionFailedf("mutation not found")
}

func GetColumnMutation(
	table catalog.TableDescriptor, colID descpb.ColumnID,
) (mut *descpb.DescriptorMutation, sliceIdx int, err error) {
	mutations := table.GetMutations()
	for i := range mutations {
		mut := &mutations[i]
		col := mut.GetColumn()
		if col != nil && col.ID == colID {
			return mut, i, nil
		}
	}
	return nil, 0, errors.AssertionFailedf("mutation not found")
}
