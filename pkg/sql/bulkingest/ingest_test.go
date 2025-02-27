package bulkingest

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestIngestFileProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dirname, cleanup := testutils.TempDir(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dirname,
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, "CREATE TABLE kv (k STRING PRIMARY KEY, v STRING)")

	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)
	tableEncoder := getEncoder(execCfg, "kv")

	sst := writeSST(t, srv, &tableEncoder, []row{{"a", "1"}, {"b", "2"}})
	require.Equal(t, sst.Uri, "nodelocal://1/bulk_ingest_test/0.sst")

	jobExecCtx, cleanup := sql.MakeJobExecContext(ctx, "test", username.RootUserName(), &sql.MemoryMetrics{}, &execCfg)
	defer cleanup()

	require.NoError(t, IngestFiles(ctx, jobExecCtx, []roachpb.Span{tableEncoder.tableSpan()}, []execinfrapb.BulkMergeSpec_SST{sst}))

	content := runner.QueryStr(t, "SELECT k, v FROM kv ORDER BY k")
	require.Equal(t, [][]string{{"a", "1"}, {"b", "2"}}, content)
}

type row struct {
	key   string
	value string
}

// writeSST creates an SST file that uses the key encoding of a (key PRIMARY KEY
// STRING, value STRING) table. It uses the application layer interface to retrieve
// external storage and the table descriptor.
func writeSST(
	t *testing.T, s serverutils.ApplicationLayerInterface, tableEncoder *encoder, values []row,
) execinfrapb.BulkMergeSpec_SST {
	exec := s.ExecutorConfig().(sql.ExecutorConfig)
	nodeURI := "nodelocal://1/bulk_ingest_test/"
	nodeStorage, err := exec.DistSQLSrv.ExternalStorageFromURI(context.Background(), nodeURI, username.RootUserName())
	require.NoError(t, err)
	defer nodeStorage.Close()

	now := exec.Clock.Now()

	// Retrieve the table descriptor for the given table name.

	// Create a settings object for the SST writer.
	settings := cluster.MakeTestingClusterSettings()

	allocator := bulksst.NewExternalFileAllocator(nodeStorage, nodeURI)

	// Define a file allocator function.

	// Initialize the SST writer.
	sstWriter := bulksst.NewUnsortedSSTBatcher(settings, allocator)
	defer sstWriter.Close(context.Background())

	for _, row := range values {
		key := storage.MVCCKey{Key: tableEncoder.encodePrimaryKey(row.key)}
		value := tableEncoder.encodeValue(row.value)
		err = sstWriter.AddMVCCKey(context.Background(), storage.MVCCKey{
			Key:       key.Key,
			Timestamp: now,
		}, value.RawBytes)
		require.NoError(t, err)
	}

	require.NoError(t, sstWriter.CloseWithError(context.Background()))

	sst := allocator.GetFileList().SST[0]
	return execinfrapb.BulkMergeSpec_SST{
		StartKey: tableEncoder.tableSpan().Key,
		EndKey:   tableEncoder.tableSpan().EndKey,
		Uri:      sst.URI,
	}
}

func getEncoder(exec sql.ExecutorConfig, tableName string) encoder {
	return encoder{
		tableDesc: desctestutils.TestingGetTableDescriptor(exec.DB, exec.Codec, "defaultdb", "public", tableName),
		codec:     exec.Codec,
	}
}

type encoder struct {
	codec     keys.SQLCodec
	tableDesc catalog.TableDescriptor
}

func (e *encoder) tableSpan() roachpb.Span {
	return e.codec.TableSpan(uint32(e.tableDesc.GetID()))
}

// encodePrimaryKey encodes the primary key for a given row using the table descriptor.
func (e *encoder) encodePrimaryKey(keyValue string) roachpb.Key {
	key := rowenc.MakeIndexKeyPrefix(e.codec, e.tableDesc.GetID(), e.tableDesc.GetPrimaryIndex().GetID())
	key = encoding.EncodeStringAscending(key, keyValue)
	key = keys.MakeFamilyKey(key, 0)
	return key
}

func (e *encoder) encodeValue(value string) roachpb.Value {
	var encodedValue roachpb.Value
	encoded := encoding.EncodeBytesValue(nil, 2, []byte(value))
	encodedValue.SetTuple(encoded)
	return encodedValue
}
