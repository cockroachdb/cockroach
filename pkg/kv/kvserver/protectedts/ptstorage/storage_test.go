// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptstorage_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"sort"
	"strconv"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	for _, withDeprecatedSpans := range []bool{true, false} {
		for _, test := range testCases {
			name := test.name
			if withDeprecatedSpans {
				name = fmt.Sprintf("%s_withDeprecatedSpans", name)
				test.runWithDeprecatedSpans = true
			}
			t.Run(name, test.run)
		}
	}
}

var testCases = []testCase{
	{
		name: "Protect - simple positive",
		ops: []op{
			protectOp{
				target: tableTarget(42),
				spans:  tableSpans(42),
			},
		},
	},
	{
		name: "Protect - no targets",
		ops: []op{
			protectOp{
				expErr: "invalid (nil target|empty set of spans)",
			},
		},
	},
	{
		name: "Protect - zero timestamp",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				rec := newRecord(tCtx, hlc.Timestamp{}, "", nil, tableTarget(42), tableSpan(42))
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					return tCtx.pts.WithTxn(txn).Protect(ctx, &rec)
				})
				require.Regexp(t, "invalid zero value timestamp", err.Error())
			}),
		},
	},
	{
		name: "Protect - already verified",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				rec := newRecord(tCtx, tCtx.tc.Server(0).Clock().Now(), "", nil, tableTarget(42),
					tableSpan(42))
				rec.Verified = true
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					return tCtx.pts.WithTxn(txn).Protect(ctx, &rec)
				})
				require.Regexp(t, "cannot create a verified record", err.Error())
			}),
		},
	},
	{
		name: "Protect - already exists",
		ops: []op{
			protectOp{spans: tableSpans(42), target: tableTarget(42)},
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				// When max_bytes or max_spans is set to 0 (i.e. unlimited), and a
				// protect op fails because the record already exists, we should report
				// that the record already exists, and not erroneously report that the
				// max_bytes or max_spans has been exceeded.
				_, err := tCtx.tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.protectedts.max_bytes = $1", 0)
				require.NoError(t, err)
				_, err = tCtx.tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.protectedts.max_spans = $1", 0)
				require.NoError(t, err)
			}),
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				rec := newRecord(tCtx, tCtx.tc.Server(0).Clock().Now(), "", nil, tableTarget(42), tableSpan(42))
				rec.ID = pickOneRecord(tCtx).GetBytes()
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					return tCtx.pts.WithTxn(txn).Protect(ctx, &rec)
				})
				require.EqualError(t, err, protectedts.ErrExists.Error())
			}),
		},
	},
	{
		name: "Protect - too many spans",
		ops: []op{
			protectOp{spans: tableSpans(42)},
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				_, err := tCtx.tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.protectedts.max_spans = $1", 3)
				require.NoError(t, err)
			}),
			protectOp{
				metaType: "asdf",
				meta:     []byte("asdf"),
				spans:    tableSpans(1, 2, 3),
				expErr:   "protectedts: limit exceeded: 1\\+3 > 3 spans",
			},
			protectOp{
				metaType: "asdf",
				meta:     []byte("asdf"),
				spans:    tableSpans(1, 2),
			},
			releaseOp{idFunc: pickOneRecord},
			releaseOp{idFunc: pickOneRecord},
			protectOp{spans: tableSpans(1)},
			protectOp{spans: tableSpans(2)},
			protectOp{spans: tableSpans(3)},
			protectOp{
				spans:  tableSpans(4),
				expErr: "protectedts: limit exceeded: 3\\+1 > 3 spans",
			},
		},
		runWithDeprecatedSpans: true,
	},
	{
		name: "Protect - too many bytes",
		ops: []op{
			protectOp{spans: tableSpans(42), target: tableTarget(42)},
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				_, err := tCtx.tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.protectedts.max_bytes = $1", 1024)
				require.NoError(t, err)
			}),
			protectOp{
				spans: append(tableSpans(1, 2),
					func() roachpb.Span {
						s := tableSpan(3)
						s.EndKey = append(s.EndKey, bytes.Repeat([]byte{'a'}, 1024)...)
						return s
					}()),
				target: largeTableTarget(1024),
				expErr: "protectedts: limit exceeded: .* bytes",
			},
			protectOp{
				spans:  tableSpans(1, 2),
				target: tableTargets(1, 2),
			},
		},
	},
	{
		name: "Protect - unlimited bytes",
		ops: []op{
			protectOp{spans: tableSpans(42), target: tableTarget(42)},
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				_, err := tCtx.tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.protectedts.max_bytes = $1", 0)
				require.NoError(t, err)
			}),
			protectOp{
				spans: append(tableSpans(1, 2),
					func() roachpb.Span {
						s := tableSpan(3)
						s.EndKey = append(s.EndKey, bytes.Repeat([]byte{'a'}, 2<<20 /* 2 MiB */)...)
						return s
					}()),
				target: largeTableTarget(2 << 20 /* 2 MiB */),
			},
			protectOp{
				spans:  tableSpans(1, 2),
				target: tableTargets(1, 2),
			},
		},
	},
	{
		name: "Protect - unlimited spans",
		ops: []op{
			protectOp{spans: tableSpans(42)},
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				_, err := tCtx.tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.protectedts.max_spans = $1", 0)
				require.NoError(t, err)
			}),
			protectOp{
				spans: func() []roachpb.Span {
					const lotsOfSpans = 1 << 15
					spans := make([]roachpb.Span, lotsOfSpans)
					for i := 0; i < lotsOfSpans; i++ {
						spans[i] = tableSpan(uint32(i))
					}
					return spans
				}(),
			},
			protectOp{
				spans: tableSpans(1, 2),
			},
		},
		runWithDeprecatedSpans: true,
	},
	{
		name: "GetRecord - does not exist",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				var rec *ptpb.Record
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
					rec, err = tCtx.pts.WithTxn(txn).GetRecord(ctx, randomID(tCtx))
					return err
				})
				require.EqualError(t, err, protectedts.ErrNotExists.Error())
				require.Nil(t, rec)
			}),
		},
	},
	{
		name: "MarkVerified",
		ops: []op{
			protectOp{target: tableTarget(42), spans: tableSpans(42)},
			markVerifiedOp{idFunc: pickOneRecord},
			markVerifiedOp{idFunc: pickOneRecord}, // it's idempotent
			markVerifiedOp{
				idFunc: randomID,
				expErr: protectedts.ErrNotExists.Error(),
			},
		},
	},
	{
		name: "Release",
		ops: []op{
			protectOp{target: tableTarget(42), spans: tableSpans(42)},
			releaseOp{idFunc: pickOneRecord},
			releaseOp{
				idFunc: randomID,
				expErr: protectedts.ErrNotExists.Error(),
			},
		},
	},
	{
		name: "UpdateTimestamp",
		ops: []op{
			protectOp{spans: tableSpans(42), target: tableTarget(42)},
			updateTimestampOp{
				expectedRecordFn: func(record ptpb.Record) ptpb.Record {
					record.Timestamp = hlc.Timestamp{WallTime: 1}
					return record
				},
				updateTimestamp: hlc.Timestamp{WallTime: 1},
			},
		},
	},
	{
		name: "UpdateTimestamp -- does not exist",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
					return tCtx.pts.WithTxn(txn).UpdateTimestamp(ctx, randomID(tCtx), hlc.Timestamp{WallTime: 1})
				})
				require.EqualError(t, err, protectedts.ErrNotExists.Error())
			}),
		},
	},
	{
		name: "Protect using synthetic timestamp",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				rec := newRecord(tCtx, tCtx.tc.Server(0).Clock().Now().WithSynthetic(true), "", nil, tableTarget(42),
					tableSpan(42))
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					return tCtx.pts.WithTxn(txn).Protect(ctx, &rec)
				})
				require.NoError(t, err)
				// Synthetic should be reset when writing timestamps to make it
				// compatible with underlying sql schema.
				rec.Timestamp.Synthetic = false
				tCtx.state.Records = append(tCtx.state.Records, rec)
				tCtx.state.Version++
				tCtx.state.NumRecords++
				tCtx.state.NumSpans += uint64(len(rec.DeprecatedSpans))
				var encoded []byte
				if tCtx.runWithDeprecatedSpans {
					encoded, err = protoutil.Marshal(&ptstorage.Spans{Spans: rec.DeprecatedSpans})
					require.NoError(t, err)
				} else {
					encoded, err = protoutil.Marshal(&ptpb.Target{Union: rec.Target.GetUnion()})
					require.NoError(t, err)
				}
				tCtx.state.TotalBytes += uint64(len(encoded))
			}),
		},
	},
}

type testContext struct {
	pts protectedts.Manager
	tc  *testcluster.TestCluster
	db  isql.DB

	// If set to false, the test will be run with
	// `DisableProtectedTimestampForMultiTenant` set to true, thereby testing the
	// "new" protected timestamp logic that runs on targets instead of spans.
	runWithDeprecatedSpans bool

	state ptpb.State
}

type op interface {
	run(ctx context.Context, t *testing.T, testCtx *testContext)
}

type funcOp func(ctx context.Context, t *testing.T, tCtx *testContext)

func (f funcOp) run(ctx context.Context, t *testing.T, tCtx *testContext) {
	f(ctx, t, tCtx)
}

type releaseOp struct {
	idFunc func(tCtx *testContext) uuid.UUID
	expErr string
}

func (r releaseOp) run(ctx context.Context, t *testing.T, tCtx *testContext) {
	id := r.idFunc(tCtx)
	err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return tCtx.pts.WithTxn(txn).Release(ctx, id)
	})
	if !testutils.IsError(err, r.expErr) {
		t.Fatalf("expected error to match %q, got %q", r.expErr, err)
	}
	if err == nil {
		i := sort.Search(len(tCtx.state.Records), func(i int) bool {
			return bytes.Compare(id[:], tCtx.state.Records[i].ID[:]) <= 0
		})
		rec := tCtx.state.Records[i]
		tCtx.state.Records = append(tCtx.state.Records[:i], tCtx.state.Records[i+1:]...)
		if len(tCtx.state.Records) == 0 {
			tCtx.state.Records = nil
		}
		tCtx.state.Version++
		tCtx.state.NumRecords--
		tCtx.state.NumSpans -= uint64(len(rec.DeprecatedSpans))
		var encoded []byte
		if tCtx.runWithDeprecatedSpans {
			encoded, err = protoutil.Marshal(&ptstorage.Spans{Spans: rec.DeprecatedSpans})
			require.NoError(t, err)
		} else {
			encoded, err = protoutil.Marshal(&ptpb.Target{Union: rec.Target.GetUnion()})
			require.NoError(t, err)
		}
		tCtx.state.TotalBytes -= uint64(len(encoded) + len(rec.Meta) + len(rec.MetaType))
	}
}

type markVerifiedOp struct {
	idFunc func(tCtx *testContext) uuid.UUID
	expErr string
}

func (mv markVerifiedOp) run(ctx context.Context, t *testing.T, tCtx *testContext) {
	id := mv.idFunc(tCtx)
	err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return tCtx.pts.WithTxn(txn).MarkVerified(ctx, id)
	})
	if !testutils.IsError(err, mv.expErr) {
		t.Fatalf("expected error to match %q, got %q", mv.expErr, err)
	}
	if err == nil {
		i := sort.Search(len(tCtx.state.Records), func(i int) bool {
			return bytes.Compare(id[:], tCtx.state.Records[i].ID[:]) <= 0
		})
		tCtx.state.Records[i].Verified = true
	}
}

type protectOp struct {
	idFunc   func(*testContext) uuid.UUID
	metaType string
	meta     []byte
	spans    []roachpb.Span
	target   *ptpb.Target
	expErr   string
}

func (p protectOp) run(ctx context.Context, t *testing.T, tCtx *testContext) {
	rec := newRecord(tCtx, tCtx.tc.Server(0).Clock().Now(), p.metaType, p.meta, p.target, p.spans...)
	if p.idFunc != nil {
		rec.ID = p.idFunc(tCtx).GetBytes()
	}
	err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return tCtx.pts.WithTxn(txn).Protect(ctx, &rec)
	})
	if !testutils.IsError(err, p.expErr) {
		t.Fatalf("expected error to match %q, got %q", p.expErr, err)
	}
	if err == nil {
		i := sort.Search(len(tCtx.state.Records), func(i int) bool {
			return bytes.Compare(rec.ID[:], tCtx.state.Records[i].ID[:]) <= 0
		})
		tail := tCtx.state.Records[i:]
		tCtx.state.Records = append(tCtx.state.Records[:i:i], rec)
		tCtx.state.Records = append(tCtx.state.Records, tail...)
		tCtx.state.Version++
		tCtx.state.NumRecords++
		tCtx.state.NumSpans += uint64(len(rec.DeprecatedSpans))
		var encoded []byte
		if tCtx.runWithDeprecatedSpans {
			encoded, err = protoutil.Marshal(&ptstorage.Spans{Spans: rec.DeprecatedSpans})
			require.NoError(t, err)
		} else {
			encoded, err = protoutil.Marshal(&ptpb.Target{Union: rec.Target.GetUnion()})
			require.NoError(t, err)
		}
		tCtx.state.TotalBytes += uint64(len(encoded) + len(p.meta) + len(p.metaType))
	}
}

type updateTimestampOp struct {
	expectedRecordFn func(record ptpb.Record) ptpb.Record
	updateTimestamp  hlc.Timestamp
	expErr           string
}

func (p updateTimestampOp) run(ctx context.Context, t *testing.T, tCtx *testContext) {
	id := pickOneRecord(tCtx)
	err := tCtx.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return tCtx.pts.WithTxn(txn).UpdateTimestamp(ctx, id, p.updateTimestamp)
	})
	if !testutils.IsError(err, p.expErr) {
		t.Fatalf("expected error to match %q, got %q", p.expErr, err)
	}
	if err == nil {
		i := sort.Search(len(tCtx.state.Records), func(i int) bool {
			return bytes.Equal(id[:], tCtx.state.Records[i].ID[:])
		})
		tCtx.state.Records[i] = p.expectedRecordFn(tCtx.state.Records[i])
		tCtx.state.Version++
	}
}

type testCase struct {
	name                   string
	ops                    []op
	runWithDeprecatedSpans bool
}

func (test testCase) run(t *testing.T) {
	ctx := context.Background()
	var params base.TestServerArgs

	ptsKnobs := &protectedts.TestingKnobs{}
	if test.runWithDeprecatedSpans {
		ptsKnobs.DisableProtectedTimestampForMultiTenant = true
		params.Knobs.ProtectedTS = ptsKnobs
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	ptm := ptstorage.New(s.ClusterSettings(), ptsKnobs)
	tCtx := testContext{
		pts:                    ptm,
		db:                     s.InternalDB().(isql.DB),
		tc:                     tc,
		runWithDeprecatedSpans: test.runWithDeprecatedSpans,
	}
	pts := ptstorage.WithDatabase(ptm, s.InternalDB().(isql.DB))
	verify := func(t *testing.T) {
		var state ptpb.State
		state, err := pts.GetState(ctx)
		require.NoError(t, err)

		md, err := pts.GetMetadata(ctx)
		require.NoError(t, err)
		require.EqualValues(t, tCtx.state, state)
		require.EqualValues(t, tCtx.state.Metadata, md)
		for _, r := range tCtx.state.Records {
			var rec *ptpb.Record
			rec, err := pts.GetRecord(ctx, r.ID.GetUUID())
			require.NoError(t, err)
			require.EqualValues(t, &r, rec)
		}
	}

	for i, tOp := range test.ops {
		if !t.Run(strconv.Itoa(i), func(t *testing.T) {
			tOp.run(ctx, t, &tCtx)
			verify(t)
		}) {
			break
		}
	}
}

func randomID(*testContext) uuid.UUID {
	return uuid.MakeV4()
}

func pickOneRecord(tCtx *testContext) uuid.UUID {
	numRecords := len(tCtx.state.Records)
	if numRecords == 0 {
		panic(fmt.Errorf("cannot pick one from zero records: %+v", tCtx))
	}
	return tCtx.state.Records[rand.Intn(numRecords)].ID.GetUUID()
}

func tableTargets(ids ...uint32) *ptpb.Target {
	var tableIDs []descpb.ID
	for _, id := range ids {
		tableIDs = append(tableIDs, descpb.ID(id))
	}
	return ptpb.MakeSchemaObjectsTarget(tableIDs)
}

func tableTarget(tableID uint32) *ptpb.Target {
	return ptpb.MakeSchemaObjectsTarget([]descpb.ID{descpb.ID(tableID)})
}

func largeTableTarget(targetBytesSize int64) *ptpb.Target {
	var tableID descpb.ID
	idSize := int64(unsafe.Sizeof(tableID))
	ids := make([]descpb.ID, 0)
	for i := int64(0); i < targetBytesSize/idSize; i++ {
		ids = append(ids, descpb.ID(rand.Uint32()))
	}
	return ptpb.MakeSchemaObjectsTarget(ids)
}

func tableSpan(tableID uint32) roachpb.Span {
	return roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(tableID),
		EndKey: keys.SystemSQLCodec.TablePrefix(tableID).PrefixEnd(),
	}
}

func tableSpans(tableIDs ...uint32) []roachpb.Span {
	spans := make([]roachpb.Span, len(tableIDs))
	for i, tableID := range tableIDs {
		spans[i] = tableSpan(tableID)
	}
	return spans
}

func newRecord(
	tCtx *testContext,
	ts hlc.Timestamp,
	metaType string,
	meta []byte,
	target *ptpb.Target,
	spans ...roachpb.Span,
) ptpb.Record {
	if tCtx.runWithDeprecatedSpans {
		target = nil
	} else {
		spans = nil
	}
	return ptpb.Record{
		ID:              uuid.MakeV4().GetBytes(),
		Timestamp:       ts,
		Mode:            ptpb.PROTECT_AFTER,
		MetaType:        metaType,
		Meta:            meta,
		DeprecatedSpans: spans,
		Target:          target,
	}
}

// TestCorruptData exercises the handling of malformed data inside the protected
// timestamp tables. We don't anticipate this ever happening and it would
// generally be a bad thing. Nevertheless, we plan for the worst and need to
// understand the system behavior in that scenario.
//
// The main source of corruption in the subsystem would be malformed encoded
// spans. Another possible form of corruption would be that the metadata does
// not align with the data. The metadata misalignment will not lead to a
// foreground error anywhere. Corrupt spans could.
//
// A corrupt spans entry only impacts GetRecord and GetState. In both cases
// we omit the spans from the entry and return it, logging the error. We prefer
// logging the error over returning it as there's a chance that the code is
// merely trying to remove the malformed data. The returned Record which
// contains no spans will be invalid and cannot be Verified. Such a Record
// can be removed.
func TestCorruptData(t *testing.T) {
	ctx := context.Background()

	runCorruptDataTest := func(tCtx *testContext, s serverutils.TestServerInterface,
		tc *testcluster.TestCluster, pts protectedts.Storage) {
		rec := newRecord(tCtx, s.Clock().Now(), "foo", []byte("bar"), tableTarget(42), tableSpan(42))
		require.NoError(t, pts.Protect(ctx, &rec))

		db := tc.Server(0).InternalDB().(isql.DB)
		updateQuery := "UPDATE system.protected_ts_records SET target = $1 WHERE id = $2"
		if tCtx.runWithDeprecatedSpans {
			updateQuery = "UPDATE system.protected_ts_records SET spans = $1 WHERE id = $2"
		}
		affected, err := db.Executor().ExecEx(
			ctx, "corrupt-data", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			updateQuery,
			[]byte("junk"), rec.ID.String())
		require.NoError(t, err)
		require.Equal(t, 1, affected)

		got, err := pts.GetRecord(ctx, rec.ID.GetUUID())
		msg := regexp.MustCompile("failed to unmarshal (span|target) for " + rec.ID.String() + ": ")
		require.Regexp(t, msg, err)
		require.Nil(t, got)

		{
			_, err := pts.GetState(ctx)
			require.NoError(t, err)
		}

		log.FlushFileSinks()
		entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 100, msg,
			log.WithFlattenedSensitiveData)
		require.NoError(t, err)
		require.GreaterOrEqual(t, 1, len(entries), "entries: %v", entries)
		for _, e := range entries {
			require.Equal(t, severity.ERROR, e.Severity)
		}
	}

	// TODO(adityamaru): Remove test when we delete `spans` field from
	// record.
	t.Run("corrupt spans", func(t *testing.T) {
		// Set the log scope so we can introspect the logged errors.
		scope := log.Scope(t)
		defer scope.Close(t)

		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SpanConfig:  &spanconfig.TestingKnobs{ManagerDisableJobCreation: true},
					ProtectedTS: &protectedts.TestingKnobs{DisableProtectedTimestampForMultiTenant: true},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		s := tc.Server(0)
		ptp := s.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		tCtx := &testContext{runWithDeprecatedSpans: true}
		runCorruptDataTest(tCtx, s, tc, ptstorage.WithDatabase(
			ptp, tc.Server(0).InternalDB().(isql.DB),
		))
	})

	t.Run("corrupt target", func(t *testing.T) {
		// Set the log scope so we can introspect the logged errors.
		scope := log.Scope(t)
		defer scope.Close(t)

		params, _ := tests.CreateTestServerParams()
		params.Knobs.SpanConfig = &spanconfig.TestingKnobs{ManagerDisableJobCreation: true}
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
		defer tc.Stopper().Stop(ctx)

		s := tc.Server(0)
		pts := s.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		runCorruptDataTest(
			&testContext{}, s, tc,
			ptstorage.WithDatabase(pts, s.InternalDB().(isql.DB)),
		)
	})
	t.Run("corrupt hlc timestamp", func(t *testing.T) {
		// Set the log scope so we can introspect the logged errors.
		scope := log.Scope(t)
		defer scope.Close(t)

		params, _ := tests.CreateTestServerParams()
		params.Knobs.SpanConfig = &spanconfig.TestingKnobs{ManagerDisableJobCreation: true}
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
		defer tc.Stopper().Stop(ctx)

		s := tc.Server(0)
		ptp := s.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider
		pts := ptstorage.WithDatabase(ptp, s.InternalDB().(isql.DB))
		rec := newRecord(&testContext{}, s.Clock().Now(), "foo", []byte("bar"), tableTarget(42), tableSpan(42))
		require.NoError(t, pts.Protect(ctx, &rec))

		// This timestamp has too many logical digits and thus will fail parsing.
		var d tree.DDecimal
		d.SetFinite(math.MaxInt32, -12)
		ie := tc.Server(0).InternalExecutor().(isql.Executor)
		affected, err := ie.ExecEx(
			ctx, "corrupt-data", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			"UPDATE system.protected_ts_records SET ts = $1 WHERE id = $2",
			d.String(), rec.ID.String())
		require.NoError(t, err)
		require.Equal(t, 1, affected)

		msg := regexp.MustCompile("failed to parse timestamp for " + rec.ID.String() +
			": logical part has too many digits")
		got, err := pts.GetRecord(ctx, rec.ID.GetUUID())
		require.Regexp(t, msg, err)
		require.Nil(t, got)
		_, err = pts.GetState(ctx)
		require.NoError(t, err)
		log.FlushFileSinks()

		entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 100, msg,
			log.WithFlattenedSensitiveData)
		require.NoError(t, err)
		require.GreaterOrEqual(t, 1, len(entries), "entries: %v", entries)
		for _, e := range entries {
			require.Equal(t, severity.ERROR, e.Severity)
		}
	})
}

// TestErrorsFromSQL ensures that errors from the underlying Executor
// are properly transmitted back to the client.
func TestErrorsFromSQL(t *testing.T) {
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	pts := ptstorage.New(s.ClusterSettings(), &protectedts.TestingKnobs{})
	db := s.InternalDB().(isql.DB)
	errFunc := func(string) error { return errors.New("boom") }
	rec := newRecord(&testContext{}, s.Clock().Now(), "foo", []byte("bar"), tableTarget(42), tableSpan(42))
	require.EqualError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pts.WithTxn(wrapTxn(txn, errFunc)).Protect(ctx, &rec)
	}), fmt.Sprintf("failed to write record %v: boom", rec.ID))
	require.EqualError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := pts.WithTxn(wrapTxn(txn, errFunc)).GetRecord(ctx, rec.ID.GetUUID())
		return err
	}), fmt.Sprintf("failed to read record %v: boom", rec.ID))
	require.EqualError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pts.WithTxn(wrapTxn(txn, errFunc)).MarkVerified(ctx, rec.ID.GetUUID())
	}), fmt.Sprintf("failed to mark record %v as verified: boom", rec.ID))
	require.EqualError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return pts.WithTxn(wrapTxn(txn, errFunc)).Release(ctx, rec.ID.GetUUID())
	}), fmt.Sprintf("failed to release record %v: boom", rec.ID))
	require.EqualError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := pts.WithTxn(wrapTxn(txn, errFunc)).GetMetadata(ctx)
		return err
	}), "failed to read metadata: boom")
	require.EqualError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := pts.WithTxn(wrapTxn(txn, errFunc)).GetState(ctx)
		return err
	}), "failed to read metadata: boom")
	// Test that we get an error retrieving the records in GetState.
	// The preceding call tested the error while retrieving the metadata in a
	// call to GetState.
	var seen bool
	errFunc = func(string) error {
		if !seen {
			seen = true
			return nil
		}
		return errors.New("boom")
	}
	require.EqualError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := pts.WithTxn(wrapTxn(txn, errFunc)).GetState(ctx)
		return err
	}), "failed to read records: boom")
}

// wrappedInternalTxn allows errors to be injected in SQL execution.
type wrappedInternalTxn struct {
	wrapped isql.Txn

	errFunc func(statement string) error
}

func (txn *wrappedInternalTxn) KV() *kv.Txn {
	return txn.wrapped.KV()
}

func (txn *wrappedInternalTxn) SessionData() *sessiondata.SessionData {
	return txn.wrapped.SessionData()
}

func wrapTxn(txn isql.Txn, errFunc func(statement string) error) *wrappedInternalTxn {
	return &wrappedInternalTxn{wrapped: txn, errFunc: errFunc}
}

func (txn *wrappedInternalTxn) QueryBufferedExWithCols(
	ctx context.Context,
	opName string,
	_ *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, colinfo.ResultColumns, error) {
	panic("unimplemented")
}

var _ isql.Executor = &wrappedInternalTxn{}

func (txn *wrappedInternalTxn) Exec(
	ctx context.Context, opName string, _ *kv.Txn, statement string, params ...interface{},
) (int, error) {
	panic("unimplemented")
}

func (txn *wrappedInternalTxn) ExecEx(
	ctx context.Context,
	opName string,
	kvTxn *kv.Txn,
	o sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	if f := txn.errFunc; f != nil {
		if err := f(stmt); err != nil {
			return 0, err
		}
	}
	return txn.wrapped.ExecEx(ctx, opName, kvTxn, o, stmt, qargs...)
}

func (txn *wrappedInternalTxn) QueryRowEx(
	ctx context.Context,
	opName string,
	kvTxn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	if f := txn.errFunc; f != nil {
		if err := f(stmt); err != nil {
			return nil, err
		}
	}
	return txn.wrapped.QueryRowEx(ctx, opName, kvTxn, session, stmt, qargs...)
}

func (txn *wrappedInternalTxn) QueryRow(
	ctx context.Context, opName string, _ *kv.Txn, statement string, qargs ...interface{},
) (tree.Datums, error) {
	panic("not implemented")
}

func (txn *wrappedInternalTxn) QueryRowExWithCols(
	ctx context.Context,
	opName string,
	_ *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, colinfo.ResultColumns, error) {
	panic("not implemented")
}

func (txn *wrappedInternalTxn) QueryBuffered(
	ctx context.Context, opName string, _ *kv.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, error) {
	panic("not implemented")
}

func (txn *wrappedInternalTxn) QueryBufferedEx(
	ctx context.Context,
	opName string,
	_ *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	panic("not implemented")
}

func (txn *wrappedInternalTxn) QueryIterator(
	ctx context.Context, opName string, _ *kv.Txn, stmt string, qargs ...interface{},
) (isql.Rows, error) {
	panic("not implemented")
}

func (txn *wrappedInternalTxn) QueryIteratorEx(
	ctx context.Context,
	opName string,
	kvTxn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (isql.Rows, error) {
	if f := txn.errFunc; f != nil {
		if err := f(stmt); err != nil {
			return nil, err
		}
	}
	return txn.wrapped.QueryIteratorEx(ctx, opName, kvTxn, session, stmt, qargs...)
}

func (txn *wrappedInternalTxn) WithSyntheticDescriptors(
	descs []catalog.Descriptor, run func() error,
) error {
	panic("not implemented")
}
