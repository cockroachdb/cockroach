// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupccl

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func BenchmarkSpansForAllTableIndexes(b *testing.B) {
	defer leaktest.AfterTest(b)()
	execCfg := &sql.ExecutorConfig{
		Codec: keys.SystemSQLCodec,
	}
	const descCount = 15000

	primaryIndex := getMockIndexDesc(descpb.IndexID(1))
	secondaryIndexes := make([]descpb.IndexDescriptor, descCount)
	revs := make([]backuppb.BackupManifest_DescriptorRevision, descCount)
	for i := 0; i < descCount; i++ {
		idxDesc := getMockIndexDesc(descpb.IndexID(i + 1))
		secondaryIndexes[i] = idxDesc

		tableRev := getMockTableDesc(descpb.ID(42), idxDesc, nil, nil, nil)
		revs[i] = backuppb.BackupManifest_DescriptorRevision{
			Desc: tableRev.DescriptorProto(),
		}
	}

	b.Run("secondaryIndexesCount=15000", func(b *testing.B) {
		tableDesc := getMockTableDesc(descpb.ID(42), primaryIndex, secondaryIndexes, nil, nil)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			_, err := spansForAllTableIndexes(execCfg, []catalog.TableDescriptor{tableDesc}, nil /* revs */)
			require.NoError(b, err)
		}
	})
	b.Run("revisionCount=15000", func(b *testing.B) {
		tableDesc := getMockTableDesc(descpb.ID(42), primaryIndex, nil, nil, nil)
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			_, err := spansForAllTableIndexes(execCfg, []catalog.TableDescriptor{tableDesc}, revs)
			require.NoError(b, err)
		}
	})
}
