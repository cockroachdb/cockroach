// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"bytes"
	"context"
	"fmt"
	math "math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/bulk"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/limit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func slurpSSTablesLatestKey(
	t *testing.T, dir string, paths []string, oldPrefix, newPrefix []byte,
) []storage.MVCCKeyValue {
	start, end := storage.MVCCKey{Key: keys.LocalMax}, storage.MVCCKey{Key: keys.MaxKey}
	kr := prefixRewriter{rewrites: []prefixRewrite{{OldPrefix: oldPrefix, NewPrefix: newPrefix}}}

	e := storage.NewDefaultInMemForTesting()
	defer e.Close()
	batch := e.NewBatch()
	defer batch.Close()

	for _, path := range paths {
		file, err := vfs.Default.Open(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}
		iterOpts := storage.IterOptions{
			KeyTypes:   storage.IterKeyTypePointsOnly,
			LowerBound: keys.LocalMax,
			UpperBound: keys.MaxKey,
		}
		sst, err := storage.NewSSTIterator([][]sstable.ReadableFile{{file}}, iterOpts)
		if err != nil {
			t.Fatal(err)
		}
		defer sst.Close()

		sst.SeekGE(start)
		for {
			if valid, err := sst.Valid(); !valid || err != nil {
				if err != nil {
					t.Fatal(err)
				}
				break
			}
			if !sst.UnsafeKey().Less(end) {
				break
			}
			key := sst.UnsafeKey()
			value, err := storage.DecodeMVCCValueAndErr(sst.UnsafeValue())
			if err != nil {
				t.Fatal(err)
			}
			newKey := key
			newKey.Key = append([]byte(nil), newKey.Key...)
			var ok bool
			newKey.Key, ok = kr.rewriteKey(newKey.Key)
			if !ok {
				t.Fatalf("could not rewrite key: %s", newKey.Key)
			}
			newValue := value
			newValue.Value.RawBytes = append([]byte(nil), newValue.Value.RawBytes...)
			newValue.Value.ClearChecksum()
			newValue.Value.InitChecksum(newKey.Key)
			if err := batch.PutMVCC(newKey, newValue); err != nil {
				t.Fatal(err)
			}
			sst.Next()
		}
	}

	var kvs []storage.MVCCKeyValue
	it, err := batch.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
	require.NoError(t, err)
	defer it.Close()
	for it.SeekGE(start); ; it.NextKey() {
		if ok, err := it.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok || !it.UnsafeKey().Less(end) {
			break
		}
		val, err := storage.DecodeMVCCValueAndErr(it.Value())
		if err != nil {
			t.Fatal(err)
		}
		kvs = append(kvs, storage.MVCCKeyValue{Key: it.UnsafeKey().Clone(), Value: val.Value.RawBytes})
	}
	return kvs
}

func clientKVsToEngineKVs(kvs []kv.KeyValue) []storage.MVCCKeyValue {
	var ret []storage.MVCCKeyValue
	for _, kv := range kvs {
		if kv.Value == nil {
			continue
		}
		k := storage.MVCCKey{
			Key:       kv.Key,
			Timestamp: kv.Value.Timestamp,
		}
		ret = append(ret, storage.MVCCKeyValue{Key: k, Value: kv.Value.RawBytes})
	}
	return ret
}

func TestIngest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	t.Run("batch=default", func(t *testing.T) {
		runTestIngest(t, func(_ *cluster.Settings) {})
	})
	t.Run("batch=1", func(t *testing.T) {
		// The test normally doesn't trigger the batching behavior, so lower
		// the threshold to force it.
		init := func(st *cluster.Settings) {
			bulk.IngestBatchSize.Override(ctx, &st.SV, 1)
		}
		runTestIngest(t, init)
	})
}

func runTestIngest(t *testing.T, init func(*cluster.Settings)) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	if err := os.Mkdir(filepath.Join(dir, "foo"), 0755); err != nil {
		t.Fatal(err)
	}

	const (
		oldID   = 51
		indexID = 1
	)

	srcPrefix := MakeKeyRewriterPrefixIgnoringInterleaved(oldID, indexID)
	var keySlice []roachpb.Key
	for i := 0; i < 8; i++ {
		key := append([]byte(nil), srcPrefix...)
		key = encoding.EncodeStringAscending(key, fmt.Sprintf("k%d", i))
		keySlice = append(keySlice, key)
	}

	ctx := context.Background()
	cs := cluster.MakeTestingClusterSettings()
	writeSST := func(t *testing.T, offsets []int) string {
		path := strconv.FormatInt(timeutil.Now().UnixNano(), 10)

		var sstFile bytes.Buffer
		sst := storage.MakeTransportSSTWriter(ctx, cs, &sstFile)
		defer sst.Close()
		ts := hlc.NewClockForTesting(nil).Now()
		value := roachpb.MakeValueFromString("bar")
		for _, idx := range offsets {
			key := keySlice[idx]
			value.ClearChecksum()
			value.InitChecksum(key)
			if err := sst.Put(storage.MVCCKey{Key: key, Timestamp: ts}, value.RawBytes); err != nil {
				t.Fatalf("%+v", err)
			}
		}
		if err := sst.Finish(); err != nil {
			t.Fatalf("%+v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "foo", path), sstFile.Bytes(), 0644); err != nil {
			t.Fatalf("%+v", err)
		}
		return path
	}

	// Make the first few AddSSTable calls return
	// AmbiguousResultError. Import should be resilient to this.
	const initialAmbiguousSubReqs = 3
	remainingAmbiguousSubReqs := int64(initialAmbiguousSubReqs)
	knobs := base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
		EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
			TestingEvalFilter: func(filterArgs kvserverbase.FilterArgs) *kvpb.Error {
				switch filterArgs.Req.(type) {
				case *kvpb.AddSSTableRequest:
				// No-op.
				default:
					return nil
				}
				r := atomic.AddInt64(&remainingAmbiguousSubReqs, -1)
				if r < 0 {
					return nil
				}
				return kvpb.NewError(kvpb.NewAmbiguousResultErrorf("%d", r))
			},
		},
	}}

	args := base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(107812),

		Knobs:         knobs,
		ExternalIODir: dir,
		Settings:      cs,
	}
	// TODO(dan): This currently doesn't work with AddSSTable on in-memory
	// stores because RocksDB's InMemoryEnv doesn't support NewRandomRWFile
	// (which breaks the global-seqno rewrite used when the added sstable
	// overlaps with existing data in the RocksDB instance). #16345.
	args.StoreSpecs = []base.StoreSpec{{InMemory: false, Path: filepath.Join(dir, "testserver")}}
	srv, _, kvDB := serverutils.StartServer(t, args)
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	init(s.ClusterSettings())

	flowCtx := execinfra.FlowCtx{
		Cfg: &execinfra.ServerConfig{
			DB: s.InternalDB().(descs.DB),
			ExternalStorage: func(ctx context.Context, dest cloudpb.ExternalStorage, opts ...cloud.ExternalStorageOption) (cloud.ExternalStorage, error) {
				return cloud.MakeExternalStorage(ctx, dest, base.ExternalIODirConfig{},
					s.ClusterSettings(), blobs.TestBlobServiceClient(args.ExternalIODir),
					nil, /* db */
					nil, /* limiters */
					cloud.NilMetrics,
					opts...)
			},
			Settings: s.ClusterSettings(),
			Codec:    s.Codec(),
			BackupMonitor: mon.NewUnlimitedMonitor(ctx, mon.Options{
				Name:     mon.MakeMonitorName("test"),
				Settings: s.ClusterSettings(),
			}),
			BulkSenderLimiter: limit.MakeConcurrentRequestLimiter("test", math.MaxInt),
		},
		EvalCtx: &eval.Context{
			Codec:    s.Codec(),
			Settings: s.ClusterSettings(),
		},
	}

	storage, err := cloud.ExternalStorageConfFromURI("nodelocal://1/foo", username.RootUserName())
	if err != nil {
		t.Fatalf("%+v", err)
	}

	const splitKey1, splitKey2 = 3, 5
	// Each test case consists of some number of batches of keySlice, represented as
	// ints [0, 8). Splits are at 3 and 5.
	for i, testCase := range [][][]int{
		// Simple cases, no spanning splits, try first, last, middle, etc in each.
		// r1
		{{0}},
		{{1}},
		{{2}},
		{{0, 1, 2}},
		{{0}, {1}, {2}},

		// r2
		{{3}},
		{{4}},
		{{3, 4}},
		{{3}, {4}},

		// r3
		{{5}},
		{{5, 6, 7}},
		{{6}},

		// batches exactly matching spans.
		{{0, 1, 2}, {3, 4}, {5, 6, 7}},

		// every key, in its own batch.
		{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}},

		// every key in one big batch.
		{{0, 1, 2, 3, 4, 5, 6, 7}},

		// Look for off-by-ones on and around the splits.
		{{2, 3}},
		{{1, 3}},
		{{2, 4}},
		{{1, 4}},
		{{1, 5}},
		{{2, 5}},

		// Mixture of split-aligned and non-aligned batches.
		{{1}, {5}, {6}},
		{{1, 2, 3}, {4, 5}, {6, 7}},
		{{0}, {2, 3, 5}, {7}},
		{{0, 4}, {5, 7}},
		{{0, 3}, {4}},
	} {
		t.Run(fmt.Sprintf("%d-%v", i, testCase), func(t *testing.T) {
			newID := descpb.ID(100 + i)
			newPrefix := MakeKeyRewriterPrefixIgnoringInterleaved(newID, indexID)
			kr := prefixRewriter{rewrites: []prefixRewrite{{OldPrefix: srcPrefix, NewPrefix: newPrefix}}}
			rekeys := []execinfrapb.TableRekey{
				{
					OldID: oldID,
					NewDesc: mustMarshalDesc(t, &descpb.TableDescriptor{
						ID: newID,
						PrimaryIndex: descpb.IndexDescriptor{
							ID: indexID,
						},
					}),
				},
			}

			first := keySlice[testCase[0][0]]
			last := keySlice[testCase[len(testCase)-1][len(testCase[len(testCase)-1])-1]]

			reqStartKey, ok := kr.rewriteKey(append([]byte(nil), keySlice[0]...))
			if !ok {
				t.Fatalf("failed to rewrite key: %s", reqStartKey)
			}
			reqEndKey, ok := kr.rewriteKey(append([]byte(nil), keySlice[len(keySlice)-1].PrefixEnd()...))
			if !ok {
				t.Fatalf("failed to rewrite key: %s", reqEndKey)
			}
			reqMidKey1, ok := kr.rewriteKey(append([]byte(nil), keySlice[splitKey1]...))
			if !ok {
				t.Fatalf("failed to rewrite key: %s", reqMidKey1)
			}
			reqMidKey2, ok := kr.rewriteKey(append([]byte(nil), keySlice[splitKey2]...))
			if !ok {
				t.Fatalf("failed to rewrite key: %s", reqMidKey2)
			}

			if err := kvDB.AdminSplit(
				ctx,
				reqMidKey1,
				hlc.MaxTimestamp, /* expirationTime */
			); err != nil {
				t.Fatal(err)
			}
			if err := kvDB.AdminSplit(
				ctx,
				reqMidKey2,
				hlc.MaxTimestamp, /* expirationTime */
			); err != nil {
				t.Fatal(err)
			}

			atomic.StoreInt64(&remainingAmbiguousSubReqs, initialAmbiguousSubReqs)

			mockRestoreDataSpec := execinfrapb.RestoreDataSpec{
				TableRekeys: rekeys,
			}
			restoreSpanEntry := execinfrapb.RestoreSpanEntry{
				Span: roachpb.Span{Key: first, EndKey: last.PrefixEnd()},
			}

			var slurp []string
			for ks := range testCase {
				f := writeSST(t, testCase[ks])
				slurp = append(slurp, f)
				restoreSpanEntry.Files = append(restoreSpanEntry.Files, execinfrapb.RestoreFileSpec{Dir: storage, Path: f})
			}
			expectedKVs := slurpSSTablesLatestKey(t, filepath.Join(dir, "foo"), slurp, srcPrefix, newPrefix)

			mockRestoreDataProcessor := &restoreDataProcessor{
				ProcessorBase: execinfra.ProcessorBase{
					ProcessorBaseNoHelper: execinfra.ProcessorBaseNoHelper{
						FlowCtx: &flowCtx,
					},
				},
				spec: mockRestoreDataSpec,
				qp: backuputils.NewMemoryBackedQuotaPool(
					ctx, nil /* m */, "restore-mon", 0, /* limit */
				),
			}
			sst, res, err := mockRestoreDataProcessor.openSSTs(ctx, restoreSpanEntry, nil)
			require.NoError(t, err)
			require.Equal(t, resumeEntry{done: true, idx: len(restoreSpanEntry.Files)}, *res)
			rewriter, err := MakeKeyRewriterFromRekeys(flowCtx.Codec(), mockRestoreDataSpec.TableRekeys,
				mockRestoreDataSpec.TenantRekeys, false /* restoreTenantFromStream */)
			require.NoError(t, err)
			_, err = mockRestoreDataProcessor.processRestoreSpanEntry(ctx, rewriter, sst)
			require.NoError(t, err)

			clientKVs, err := kvDB.Scan(ctx, reqStartKey, reqEndKey, 0)
			if err != nil {
				t.Fatalf("%+v", err)
			}
			kvs := clientKVsToEngineKVs(clientKVs)
			for i := range kvs {
				if i < len(expectedKVs) {
					expectedKVs[i].Key.Timestamp = kvs[i].Key.Timestamp
				}
			}

			if !reflect.DeepEqual(kvs, expectedKVs) {
				for i := 0; i < len(kvs) || i < len(expectedKVs); i++ {
					if i < len(expectedKVs) {
						t.Logf("expected %d\t%v\t%v", i, expectedKVs[i].Key, expectedKVs[i].Value)
					}
					if i < len(kvs) {
						t.Logf("got      %d\t%v\t%v", i, kvs[i].Key, kvs[i].Value)
					}
					t.Fatalf("got %+v expected %+v", kvs, expectedKVs)
				}
			}

			if r := atomic.LoadInt64(&remainingAmbiguousSubReqs); r > 0 {
				t.Errorf("expected ambiguous sub-requests to be depleted got %d", r)
			}
		})
	}
}
