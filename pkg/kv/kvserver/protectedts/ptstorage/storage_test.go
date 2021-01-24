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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	for _, test := range testCases {
		t.Run(test.name, test.run)
	}
}

var testCases = []testCase{
	{
		name: "Protect - simple positive",
		ops: []op{
			protectOp{spans: tableSpans(42)},
		},
	},
	{
		name: "Protect - no spans",
		ops: []op{
			protectOp{
				expErr: "invalid empty set of spans",
			},
		},
	},
	{
		name: "Protect - zero timestamp",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				rec := newRecord(hlc.Timestamp{}, "", nil, tableSpan(42))
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return tCtx.pts.Protect(ctx, txn, &rec)
				})
				require.Regexp(t, "invalid zero value timestamp", err.Error())
			}),
		},
	},
	{
		name: "Protect - already verified",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				rec := newRecord(tCtx.tc.Server(0).Clock().Now(), "", nil, tableSpan(42))
				rec.Verified = true
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return tCtx.pts.Protect(ctx, txn, &rec)
				})
				require.Regexp(t, "cannot create a verified record", err.Error())
			}),
		},
	},
	{
		name: "Protect - already exists",
		ops: []op{
			protectOp{spans: tableSpans(42)},
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
				rec := newRecord(tCtx.tc.Server(0).Clock().Now(), "", nil, tableSpan(42))
				rec.ID = pickOneRecord(tCtx)
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					return tCtx.pts.Protect(ctx, txn, &rec)
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
	},
	{
		name: "Protect - too many bytes",
		ops: []op{
			protectOp{spans: tableSpans(42)},
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
				expErr: "protectedts: limit exceeded: 8\\+1050 > 1024 bytes",
			},
			protectOp{
				spans: tableSpans(1, 2),
			},
		},
	},
	{
		name: "Protect - unlimited bytes",
		ops: []op{
			protectOp{spans: tableSpans(42)},
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
			},
			protectOp{
				spans: tableSpans(1, 2),
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
	},
	{
		name: "GetRecord - does not exist",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				var rec *ptpb.Record
				err := tCtx.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
					rec, err = tCtx.pts.GetRecord(ctx, txn, randomID(tCtx))
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
			protectOp{spans: tableSpans(42)},
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
			protectOp{spans: tableSpans(42)},
			releaseOp{idFunc: pickOneRecord},
			releaseOp{
				idFunc: randomID,
				expErr: protectedts.ErrNotExists.Error(),
			},
		},
	},
	{
		name: "nil transaction errors",
		ops: []op{
			funcOp(func(ctx context.Context, t *testing.T, tCtx *testContext) {
				rec := newRecord(tCtx.tc.Server(0).Clock().Now(), "", nil, tableSpan(42))
				const msg = "must provide a non-nil transaction"
				require.Regexp(t, msg, tCtx.pts.Protect(ctx, nil /* txn */, &rec).Error())
				require.Regexp(t, msg, tCtx.pts.Release(ctx, nil /* txn */, uuid.MakeV4()).Error())
				require.Regexp(t, msg, tCtx.pts.MarkVerified(ctx, nil /* txn */, uuid.MakeV4()).Error())
				_, err := tCtx.pts.GetRecord(ctx, nil /* txn */, uuid.MakeV4())
				require.Regexp(t, msg, err.Error())
				_, err = tCtx.pts.GetMetadata(ctx, nil /* txn */)
				require.Regexp(t, msg, err.Error())
				_, err = tCtx.pts.GetState(ctx, nil /* txn */)
				require.Regexp(t, msg, err.Error())
			}),
		},
	},
}

type testContext struct {
	pts protectedts.Storage
	tc  *testcluster.TestCluster
	db  *kv.DB

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
	err := tCtx.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return tCtx.pts.Release(ctx, txn, id)
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
		tCtx.state.NumSpans -= uint64(len(rec.Spans))
		encoded, err := protoutil.Marshal(&ptstorage.Spans{Spans: rec.Spans})
		require.NoError(t, err)
		tCtx.state.TotalBytes -= uint64(len(encoded) + len(rec.Meta) + len(rec.MetaType))
	}
}

type markVerifiedOp struct {
	idFunc func(tCtx *testContext) uuid.UUID
	expErr string
}

func (mv markVerifiedOp) run(ctx context.Context, t *testing.T, tCtx *testContext) {
	id := mv.idFunc(tCtx)
	err := tCtx.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return tCtx.pts.MarkVerified(ctx, txn, id)
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
	expErr   string
}

func (p protectOp) run(ctx context.Context, t *testing.T, tCtx *testContext) {
	rec := newRecord(tCtx.tc.Server(0).Clock().Now(), p.metaType, p.meta, p.spans...)
	if p.idFunc != nil {
		rec.ID = p.idFunc(tCtx)
	}
	err := tCtx.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return tCtx.pts.Protect(ctx, txn, &rec)
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
		tCtx.state.NumSpans += uint64(len(rec.Spans))
		encoded, err := protoutil.Marshal(&ptstorage.Spans{Spans: p.spans})
		require.NoError(t, err)
		tCtx.state.TotalBytes += uint64(len(encoded) + len(p.meta) + len(p.metaType))
	}
}

type testCase struct {
	name string
	ops  []op
}

func (test testCase) run(t *testing.T) {
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	pts := ptstorage.New(s.ClusterSettings(),
		s.InternalExecutor().(*sql.InternalExecutor))
	db := s.DB()
	tCtx := testContext{
		pts: pts,
		db:  db,
		tc:  tc,
	}
	verify := func(t *testing.T) {
		var state ptpb.State
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			state, err = pts.GetState(ctx, txn)
			return err
		}))
		var md ptpb.Metadata
		require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			md, err = pts.GetMetadata(ctx, txn)
			return err
		}))
		require.EqualValues(t, tCtx.state, state)
		require.EqualValues(t, tCtx.state.Metadata, md)
		for _, r := range tCtx.state.Records {
			var rec *ptpb.Record
			require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				rec, err = pts.GetRecord(ctx, txn, r.ID)
				return err
			}))
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
	return tCtx.state.Records[rand.Intn(numRecords)].ID
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

func newRecord(ts hlc.Timestamp, metaType string, meta []byte, spans ...roachpb.Span) ptpb.Record {
	return ptpb.Record{
		ID:        uuid.MakeV4(),
		Timestamp: ts,
		Mode:      ptpb.PROTECT_AFTER,
		MetaType:  metaType,
		Meta:      meta,
		Spans:     spans,
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

	t.Run("corrupt spans", func(t *testing.T) {
		// Set the log scope so we can introspect the logged errors.
		scope := log.Scope(t)
		defer scope.Close(t)

		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(ctx)

		s := tc.Server(0)
		pts := ptstorage.New(s.ClusterSettings(),
			s.InternalExecutor().(*sql.InternalExecutor))

		rec := newRecord(s.Clock().Now(), "foo", []byte("bar"), tableSpan(42))
		require.NoError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return pts.Protect(ctx, txn, &rec)
		}))
		ie := tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor)
		affected, err := ie.ExecEx(
			ctx, "corrupt-data", nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			"UPDATE system.protected_ts_records SET spans = $1 WHERE id = $2",
			[]byte("junk"), rec.ID.String())
		require.NoError(t, err)
		require.Equal(t, 1, affected)

		var got *ptpb.Record
		msg := regexp.MustCompile("failed to unmarshal spans for " + rec.ID.String() + ": ")
		require.Regexp(t, msg,
			s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				got, err = pts.GetRecord(ctx, txn, rec.ID)
				return err
			}).Error())
		require.Nil(t, got)
		require.NoError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			_, err = pts.GetState(ctx, txn)
			return err
		}))
		log.Flush()
		entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 100, msg,
			log.WithFlattenedSensitiveData)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		for _, e := range entries {
			require.Equal(t, severity.ERROR, e.Severity)
		}
	})
	t.Run("corrupt hlc timestamp", func(t *testing.T) {
		// Set the log scope so we can introspect the logged errors.
		scope := log.Scope(t)
		defer scope.Close(t)

		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
		defer tc.Stopper().Stop(ctx)

		s := tc.Server(0)
		pts := ptstorage.New(s.ClusterSettings(),
			s.InternalExecutor().(*sql.InternalExecutor))

		rec := newRecord(s.Clock().Now(), "foo", []byte("bar"), tableSpan(42))
		require.NoError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return pts.Protect(ctx, txn, &rec)
		}))

		// This timestamp has too many logical digits and thus will fail parsing.
		var d tree.DDecimal
		d.SetFinite(math.MaxInt32, -12)
		ie := tc.Server(0).InternalExecutor().(sqlutil.InternalExecutor)
		affected, err := ie.ExecEx(
			ctx, "corrupt-data", nil, /* txn */
			sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
			"UPDATE system.protected_ts_records SET ts = $1 WHERE id = $2",
			d.String(), rec.ID.String())
		require.NoError(t, err)
		require.Equal(t, 1, affected)

		var got *ptpb.Record
		msg := regexp.MustCompile("failed to parse timestamp for " + rec.ID.String() +
			": logical part has too many digits")
		require.Regexp(t, msg,
			s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
				got, err = pts.GetRecord(ctx, txn, rec.ID)
				return err
			}))
		require.Nil(t, got)
		require.NoError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			_, err = pts.GetState(ctx, txn)
			return err
		}))
		log.Flush()

		entries, err := log.FetchEntriesFromFiles(0, math.MaxInt64, 100, msg,
			log.WithFlattenedSensitiveData)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		for _, e := range entries {
			require.Equal(t, severity.ERROR, e.Severity)
		}
	})
}

// TestErrorsFromSQL ensures that errors from the underlying InternalExecutor
// are properly transmitted back to the client.
func TestErrorsFromSQL(t *testing.T) {
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	ie := s.InternalExecutor().(sqlutil.InternalExecutor)
	wrappedIE := &wrappedInternalExecutor{wrapped: ie}
	pts := ptstorage.New(s.ClusterSettings(), wrappedIE)

	wrappedIE.setErrFunc(func(string) error {
		return errors.New("boom")
	})
	rec := newRecord(s.Clock().Now(), "foo", []byte("bar"), tableSpan(42))
	require.EqualError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return pts.Protect(ctx, txn, &rec)
	}), fmt.Sprintf("failed to write record %v: boom", rec.ID))
	require.EqualError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := pts.GetRecord(ctx, txn, rec.ID)
		return err
	}), fmt.Sprintf("failed to read record %v: boom", rec.ID))
	require.EqualError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return pts.MarkVerified(ctx, txn, rec.ID)
	}), fmt.Sprintf("failed to mark record %v as verified: boom", rec.ID))
	require.EqualError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return pts.Release(ctx, txn, rec.ID)
	}), fmt.Sprintf("failed to release record %v: boom", rec.ID))
	require.EqualError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := pts.GetMetadata(ctx, txn)
		return err
	}), "failed to read metadata: boom")
	require.EqualError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := pts.GetState(ctx, txn)
		return err
	}), "failed to read metadata: boom")
	// Test that we get an error retrieving the records in GetState.
	// The preceding call tested the error while retriving the metadata in a
	// call to GetState.
	var seen bool
	wrappedIE.setErrFunc(func(string) error {
		if !seen {
			seen = true
			return nil
		}
		return errors.New("boom")
	})
	require.EqualError(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := pts.GetState(ctx, txn)
		return err
	}), "failed to read records: boom")
}

// wrappedInternalExecutor allows errors to be injected in SQL execution.
type wrappedInternalExecutor struct {
	wrapped sqlutil.InternalExecutor

	mu struct {
		syncutil.RWMutex
		errFunc func(statement string) error
	}
}

var _ sqlutil.InternalExecutor = &wrappedInternalExecutor{}

func (ie *wrappedInternalExecutor) Exec(
	ctx context.Context, opName string, txn *kv.Txn, statement string, params ...interface{},
) (int, error) {
	panic("unimplemented")
}

func (ie *wrappedInternalExecutor) ExecEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	o sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (int, error) {
	if f := ie.getErrFunc(); f != nil {
		if err := f(stmt); err != nil {
			return 0, err
		}
	}
	return ie.wrapped.ExecEx(ctx, opName, txn, o, stmt, qargs...)
}

func (ie *wrappedInternalExecutor) QueryRowEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	if f := ie.getErrFunc(); f != nil {
		if err := f(stmt); err != nil {
			return nil, err
		}
	}
	return ie.wrapped.QueryRowEx(ctx, opName, txn, session, stmt, qargs...)
}

func (ie *wrappedInternalExecutor) QueryRow(
	ctx context.Context, opName string, txn *kv.Txn, statement string, qargs ...interface{},
) (tree.Datums, error) {
	panic("not implemented")
}

func (ie *wrappedInternalExecutor) QueryRowExWithCols(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, colinfo.ResultColumns, error) {
	panic("not implemented")
}

func (ie *wrappedInternalExecutor) QueryBuffered(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) ([]tree.Datums, error) {
	panic("not implemented")
}

func (ie *wrappedInternalExecutor) QueryBufferedEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) ([]tree.Datums, error) {
	panic("not implemented")
}

func (ie *wrappedInternalExecutor) QueryIterator(
	ctx context.Context, opName string, txn *kv.Txn, stmt string, qargs ...interface{},
) (sqlutil.InternalRows, error) {
	panic("not implemented")
}

func (ie *wrappedInternalExecutor) QueryIteratorEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (sqlutil.InternalRows, error) {
	if f := ie.getErrFunc(); f != nil {
		if err := f(stmt); err != nil {
			return nil, err
		}
	}
	return ie.wrapped.QueryIteratorEx(ctx, opName, txn, session, stmt, qargs...)
}

func (ie *wrappedInternalExecutor) getErrFunc() func(statement string) error {
	ie.mu.RLock()
	defer ie.mu.RUnlock()
	return ie.mu.errFunc
}

func (ie *wrappedInternalExecutor) setErrFunc(f func(statement string) error) {
	ie.mu.Lock()
	defer ie.mu.Unlock()
	ie.mu.errFunc = f
}
