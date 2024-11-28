// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestBackupResolveOptionsForJobDescription tests that
// resolveOptionsForBackupJobDescription handles every field in the
// BackupOptions struct.
func TestBackupResolveOptionsForJobDescription(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// The input struct must have a non-zero value for every
	// element of the struct.
	input := tree.BackupOptions{
		CaptureRevisionHistory:          tree.NewDString("test expr"),
		IncludeAllSecondaryTenants:      tree.NewDString("test expr"),
		EncryptionPassphrase:            tree.NewDString("test expr"),
		Detached:                        tree.DBoolTrue,
		EncryptionKMSURI:                []tree.Expr{tree.NewDString("test expr")},
		IncrementalStorage:              []tree.Expr{tree.NewDString("test expr")},
		ExecutionLocality:               tree.NewDString("test expr"),
		UpdatesClusterMonitoringMetrics: tree.NewDString("test expr"),
	}

	ensureAllStructFieldsSet := func(s tree.BackupOptions, name string) {
		structType := reflect.TypeOf(s)
		require.Equal(t, reflect.Struct, structType.Kind())

		sv := reflect.ValueOf(s)
		for i := 0; i < sv.NumField(); i++ {
			field := sv.Field(i)
			fieldName := structType.Field(i).Name
			require.True(t, field.IsValid(), "BackupOptions field %s in %s is not valid", fieldName, name)
			require.False(t, field.IsZero(), "BackupOptions field %s in %s is not non-zero", fieldName, name)
		}
	}

	ensureAllStructFieldsSet(input, "input")
	output, err := resolveOptionsForBackupJobDescription(input, []string{"http://example.com"}, []string{"http://example.com"})
	require.NoError(t, err)
	ensureAllStructFieldsSet(output, "output")

}

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
