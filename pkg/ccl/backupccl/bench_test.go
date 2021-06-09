// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl/sampledataccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/workloadccl/format"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/storage/cloudimpl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/bank"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// mockSender is an SSTSender that is a no-op but is configured to take a fixed
// amount of time for benchmarking purposes.
type mockSender struct{}

func (mockSender) AddSSTable(
	_ context.Context,
	_, _ interface{},
	_ []byte,
	_ bool,
	_ *enginepb.MVCCStats,
	_ bool,
	_ hlc.Timestamp,
) error {
	// Sleep for the typical length of time it takes to ingest and
	// replicate an AddSSTable.
	time.Sleep(400 * time.Millisecond)
	return nil
}

func (mockSender) SplitAndScatter(ctx context.Context, _ roachpb.Key, _ hlc.Timestamp) error {
	return nil
}

type entryRowSource struct {
	entries []execinfrapb.RestoreSpanEntry
	i       int
}

func (*entryRowSource) OutputTypes() []*types.T { return nil }
func (*entryRowSource) Start(context.Context)   {}

func (s *entryRowSource) Next() (rowenc.EncDatumRow, *execinfrapb.ProducerMetadata) {
	if s.i == len(s.entries) {
		return nil, nil
	}
	entry := s.entries[s.i]
	s.i++

	entryBytes, err := protoutil.Marshal(&entry)
	if err != nil {
		return nil, &execinfrapb.ProducerMetadata{Err: err}
	}

	row := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entryBytes))), // Don't bother with a routing datum for this test setup.
		rowenc.DatumToEncDatum(types.Bytes, tree.NewDBytes(tree.DBytes(entryBytes))),
	}
	return row, nil
}

func (*entryRowSource) ConsumerDone()   {}
func (*entryRowSource) ConsumerClosed() {}

func marshall(b *testing.B, tableDesc *descpb.TableDescriptor) []byte {
	desc := tabledesc.NewBuilder(tableDesc).BuildImmutable().DescriptorProto()
	// Set the timestamp to a non-zero value.
	descpb.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(desc, hlc.Timestamp{WallTime: 1})
	bytes, err := protoutil.Marshal(desc)
	if err != nil {
		b.Fatal(err)
	}
	return bytes
}

func makeTestStorage(uri string) (cloud.ExternalStorage, error) {
	conf, err := cloud.ExternalStorageConfFromURI(uri, security.RootUserName())
	if err != nil {
		return nil, err
	}
	testSettings := cluster.MakeTestingClusterSettings()

	// Setup a sink for the given args.
	clientFactory := blobs.TestBlobServiceClient(testSettings.ExternalIODir)
	s, err := cloud.MakeExternalStorage(context.Background(), conf, base.ExternalIODirConfig{},
		testSettings, clientFactory, nil, nil)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// BenchmarkRestoreProcessor benchmarks the performance of the restoreDataProcessor
// which is the workhorse of RESTORE. Rather than just measuring the overhead (and
// having external storage and AddSSTable requests return immediately) it benchmarks
// the interaction of all of these components during the flow of the RESTORE. At the
// time of writing, these benchmarks are pretty consistent with the RESTORE roachperf
// results.
//
// TODO(pbardea):
//  - Vary parallelization
//  - Introduce pipelining
//  - Tune ExternalStorage latency
//
// BenchmarkRestoreProcessor/30-tables-100000-rows-8     1      16223386193 ns/op         62.69 MB/s
func BenchmarkRestoreProcessor(b *testing.B) {
	// Having at least 30 sstables is useful to make sure that this benchmark has enough
	// work to do so that pipelining and parallelization can kick in. At the time of
	// writing, increasing the number of tables had no effect on the throughput, just
	// the amount of time it took the benchmark to run.
	for _, numTables := range []int{30} {
		// 100_000 rows produces SSTs of roughly 16MB, which is the default ingestion size.
		// Making the number of rows too small tanks the throughput since the overhead of
		// the AddSSTable is mocked to a fixed value.
		for _, numRows := range []int{100_000} {
			b.Run(fmt.Sprintf("%d-tables-%d-rows", numTables, numRows), func(b *testing.B) {
				benchmarkRestoreProcessorSize(b, numRows, numTables)
			})
		}
	}
}

func benchmarkRestoreProcessorSize(b *testing.B, numRows int, numTables int) {
	tr := tracing.NewTracer()
	ctx, span := tracing.EnsureChildSpan(context.Background(), tr, "benchmark", tracing.WithForceRealSpan())
	span.Finish()
	// Set enableJSONTrace to true for this benchmark to print out JSON
	// that can be visualized in Jaeger to help debug performance of this
	// benchmark.
	enableJSONTrace := false
	if enableJSONTrace {
		span.SetVerbose(true)
		defer func() {
			json, err := span.GetRecording().ToJaegerJSON("RestoreDataProcessor benchmark")
			require.NoError(b, err)
			fmt.Println(json)
		}()
	}
	settings := cluster.MakeTestingClusterSettings()

	evalCtx := tree.EvalContext{Settings: settings}
	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			DB: nil,
			ExternalStorage: func(ctx context.Context, dest roachpb.ExternalStorage) (cloud.ExternalStorage, error) {
				return cloud.MakeExternalStorage(ctx, dest, base.ExternalIODirConfig{},
					settings, nil /* blob service client */, nil, nil)
			},
			Settings: settings,
			Codec:    keys.SystemSQLCodec,
		},
		EvalCtx: &tree.EvalContext{
			Codec:    keys.SystemSQLCodec,
			Settings: settings,
		},
	}

	bankData := bank.FromRows(numRows).Tables()[0]
	dir, err := makeTestStorage("mem-test://test/")
	require.NoError(b, err)

	// This size is the size of the SST written, revisit this.
	bytesPerRow := 113
	// Speed, replicated.
	b.SetBytes(int64(numRows * numTables * bytesPerRow * 3))

	entries := make([]execinfrapb.RestoreSpanEntry, numTables)
	ts := timeutil.Now()
	rekeys := make([]execinfrapb.TableRekey, numTables)
	for i := 0; i < numTables; i++ {
		tableID := descpb.ID(keys.MinUserDescID + 1 + i)
		tableDesc, err := format.ToTableDescriptor(bankData, tableID, time.Unix(0, 0))
		if err != nil {
			b.Fatal(err)
		}
		rekeys[i] = execinfrapb.TableRekey{
			OldID:   uint32(tableID),
			NewDesc: marshall(b, tableDesc.TableDesc()),
		}
		sst, err := format.ToSSTable(bankData, tableID, ts)
		if err != nil {
			b.Fatal(err)
		}

		filename := fmt.Sprintf("%d.sst", i)
		writer, err := dir.Writer(context.Background(), filename)
		require.NoError(b, err)
		_, err = writer.Write(sst)
		require.NoError(b, err)

		startKey := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
		// "Upload" the bytes to the mock external storage.
		entry := execinfrapb.RestoreSpanEntry{
			Span: roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()},
			Files: []execinfrapb.RestoreFileSpec{
				{
					Dir:  dir.Conf(),
					Path: filename,
				},
			},
			ProgressIdx: int64(i),
		}
		entry.Span.EndKey = entry.Span.Key.PrefixEnd()

		entries[i] = entry
	}

	restoreDataSpec := execinfrapb.RestoreDataSpec{
		Rekeys: rekeys,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var mockInput execinfra.RowSource = &entryRowSource{
			entries: entries,
		}
		rd, _, err := backupccl.NewTestingRestoreDataProcessor(ctx, &evalCtx, &flowCtx, mockInput,
			restoreDataSpec, mockSender{})
		require.NoError(b, err)
		rd.Start(ctx)
		for {
			row, meta := rd.Next()
			if meta != nil && len(meta.TraceData) > 0 {
				span.ImportRemoteSpans(meta.TraceData)
			}
			if row == nil && meta == nil {
				break
			}
			if meta.Err != nil {
				b.Fatal(errors.Wrap(meta.Err, "unexpected meta err"))
			}
		}
	}
}

func BenchmarkDatabaseBackup(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, _, sqlDB, dir, cleanupFn := backupccl.BackupRestoreTestSetup(b, backupccl.MultiNode,
		0 /* numAccounts */, backupccl.InitManualReplication)
	defer cleanupFn()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	bankData := bank.FromRows(b.N).Tables()[0]
	loadURI := "nodelocal://0/load"
	if _, err := sampledataccl.ToBackup(b, bankData, dir, "load"); err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, fmt.Sprintf(`RESTORE data.* FROM '%s'`, loadURI))

	// TODO(dan): Ideally, this would split and rebalance the ranges in a more
	// controlled way. A previous version of this code did it manually with
	// `SPLIT AT` and TestCluster's TransferRangeLease, but it seemed to still
	// be doing work after returning, which threw off the timing and the results
	// of the benchmark. DistSQL is working on improving this infrastructure, so
	// use what they build.

	b.ResetTimer()
	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, fmt.Sprintf(`BACKUP DATABASE data TO '%s'`, backupccl.LocalFoo)).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)
	b.StopTimer()
	b.SetBytes(dataSize / int64(b.N))
}

func BenchmarkDatabaseRestore(b *testing.B) {
	// NB: This benchmark takes liberties in how b.N is used compared to the go
	// documentation's description. We're getting useful information out of it,
	// but this is not a pattern to cargo-cult.

	_, _, sqlDB, dir, cleanup := backupccl.BackupRestoreTestSetup(b, backupccl.MultiNode,
		0 /* numAccounts*/, backupccl.InitManualReplication)
	defer cleanup()
	sqlDB.Exec(b, `DROP TABLE data.bank`)

	bankData := bank.FromRows(b.N).Tables()[0]
	if _, err := sampledataccl.ToBackup(b, bankData, dir, "foo"); err != nil {
		b.Fatalf("%+v", err)
	}

	b.ResetTimer()
	sqlDB.Exec(b, `RESTORE data.* FROM 'nodelocal://0/foo'`)
	b.StopTimer()
}

func BenchmarkEmptyIncrementalBackup(b *testing.B) {
	const numStatements = 100000

	_, _, sqlDB, dir, cleanupFn := backupccl.BackupRestoreTestSetup(b, backupccl.MultiNode,
		0 /* numAccounts */, backupccl.InitManualReplication)
	defer cleanupFn()

	restoreURI := backupccl.LocalFoo + "/restore"
	fullURI := backupccl.LocalFoo + "/full"

	bankData := bank.FromRows(numStatements).Tables()[0]
	_, err := sampledataccl.ToBackup(b, bankData, dir, "foo/restore")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, `DROP TABLE data.bank`)
	sqlDB.Exec(b, `RESTORE data.* FROM $1`, restoreURI)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, `BACKUP DATABASE data TO $1`, fullURI).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		incrementalDir := backupccl.LocalFoo + fmt.Sprintf("/incremental%d", i)
		sqlDB.Exec(b, `BACKUP DATABASE data TO $1 INCREMENTAL FROM $2`, incrementalDir, fullURI)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}

func BenchmarkDatabaseFullBackup(b *testing.B) {
	const numStatements = 100000

	_, _, sqlDB, dir, cleanupFn := backupccl.BackupRestoreTestSetup(b, backupccl.MultiNode,
		0 /* numAccounts */, backupccl.InitManualReplication)
	defer cleanupFn()

	restoreURI := backupccl.LocalFoo + "/restore"
	fullURI := backupccl.LocalFoo + "/full"

	bankData := bank.FromRows(numStatements).Tables()[0]
	_, err := sampledataccl.ToBackup(b, bankData, dir, "foo/restore")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	sqlDB.Exec(b, `DROP TABLE data.bank`)
	sqlDB.Exec(b, `RESTORE data.* FROM $1`, restoreURI)

	var unused string
	var dataSize int64
	sqlDB.QueryRow(b, `BACKUP DATABASE data TO $1`, fullURI).Scan(
		&unused, &unused, &unused, &unused, &unused, &dataSize,
	)

	// We intentionally don't write anything to the database between the full and
	// incremental backup.

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		backupDir := backupccl.LocalFoo + fmt.Sprintf("/backup%d", i)
		sqlDB.Exec(b, `BACKUP DATABASE data TO $1`, backupDir)
	}
	b.StopTimer()

	// We report the number of bytes that incremental backup was able to
	// *skip*--i.e., the number of bytes in the full backup.
	b.SetBytes(int64(b.N) * dataSize)
}
