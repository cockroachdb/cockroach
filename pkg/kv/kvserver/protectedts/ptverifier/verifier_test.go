// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptverifier_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptverifier"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestVerifier tests the business logic of verification by mocking out the
// actual verification requests but using a real implementation of
// protectedts.Storage.
func TestVerifier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	s := tc.Server(0)
	var senderFunc atomic.Value
	senderFunc.Store(kv.SenderFunc(nil))
	ds := s.DistSenderI().(*kvcoord.DistSender)
	tsf := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx:        s.DB().AmbientContext,
			HeartbeatInterval: time.Second,
			Settings:          s.ClusterSettings(),
			Clock:             s.Clock(),
			Stopper:           s.Stopper(),
		},
		kv.SenderFunc(func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			if f := senderFunc.Load().(kv.SenderFunc); f != nil {
				return f(ctx, ba)
			}
			return ds.Send(ctx, ba)
		}),
	)

	pts := ptstorage.New(s.ClusterSettings(), s.InternalExecutor().(sqlutil.InternalExecutor))
	withDB := ptstorage.WithDatabase(pts, s.DB())
	db := kv.NewDB(s.DB().AmbientContext, tsf, s.Clock(), s.Stopper())
	ptv := ptverifier.New(db, pts)
	makeTableSpan := func(tableID uint32) roachpb.Span {
		k := keys.SystemSQLCodec.TablePrefix(tableID)
		return roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
	}

	createRecord := func(t *testing.T, tables ...uint32) *ptpb.Record {
		spans := make([]roachpb.Span, len(tables))
		for i, tid := range tables {
			spans[i] = makeTableSpan(tid)
		}
		r := ptpb.Record{
			ID:        uuid.MakeV4(),
			Timestamp: s.Clock().Now(),
			Mode:      ptpb.PROTECT_AFTER,
			Spans:     spans,
		}
		require.Nil(t, s.DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return pts.Protect(ctx, txn, &r)
		}))
		return &r
	}
	ensureVerified := func(t *testing.T, id uuid.UUID, verified bool) {
		got, err := withDB.GetRecord(ctx, nil, id)
		require.NoError(t, err)
		require.Equal(t, verified, got.Verified)
	}
	release := func(t *testing.T, id uuid.UUID) {
		require.NoError(t, withDB.Release(ctx, nil, id))
	}
	for _, c := range []struct {
		name string
		test func(t *testing.T)
	}{
		{
			name: "record doesn't exist",
			test: func(t *testing.T) {
				require.Regexp(t, protectedts.ErrNotExists.Error(),
					ptv.Verify(ctx, uuid.MakeV4()).Error())
			},
		},
		{
			name: "verification failed with injected error",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						return nil, roachpb.NewError(errors.New("boom"))
					}
					return ds.Send(ctx, ba)
				}))
				require.Regexp(t, "boom", ptv.Verify(ctx, r.ID).Error())
				ensureVerified(t, r.ID, false)
				release(t, r.ID)
			},
		},
		{
			name: "verification failed with injected response",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						var resp roachpb.BatchResponse
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{
							VerificationFailedRanges: []roachpb.AdminVerifyProtectedTimestampResponse_FailedRange{{
								RangeID:  42,
								StartKey: roachpb.RKey(r.Spans[0].Key),
								EndKey:   roachpb.RKey(r.Spans[0].EndKey),
							}},
						})
						return &resp, nil
					}
					return ds.Send(ctx, ba)
				}))
				require.Regexp(t, "protected ts verification error: failed to verify protection.*\n"+
					"range ID: 42, range span: /Table/42 - /Table/43",
					ptv.Verify(ctx, r.ID).Error())
				ensureVerified(t, r.ID, false)
				release(t, r.ID)
			},
		},
		{
			name: "verification failed with injected response over two spans",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42, 12)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						var resp roachpb.BatchResponse
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{
							VerificationFailedRanges: []roachpb.AdminVerifyProtectedTimestampResponse_FailedRange{{
								RangeID:  42,
								StartKey: roachpb.RKey(r.Spans[0].Key),
								EndKey:   roachpb.RKey(r.Spans[0].EndKey),
								Reason:   "foo",
							}},
						})
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{
							VerificationFailedRanges: []roachpb.AdminVerifyProtectedTimestampResponse_FailedRange{{
								RangeID:  12,
								StartKey: roachpb.RKey(r.Spans[1].Key),
								EndKey:   roachpb.RKey(r.Spans[1].EndKey),
								Reason:   "bar",
							}},
						})
						return &resp, nil
					}
					return ds.Send(ctx, ba)
				}))
				require.Regexp(t, "protected ts verification error: failed to verify protection.*\n"+
					"range ID: 42, "+
					"range span: /Table/42 - /Table/43: foo\nrange ID: 12, "+
					"range span: /Table/12 - /Table/13: bar",
					ptv.Verify(ctx, r.ID).Error())
				ensureVerified(t, r.ID, false)
				release(t, r.ID)
			},
		},
		{
			// TODO(adityamaru): Remove in 21.2.
			name: "verification failed with deprecated failed ranges response",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						var resp roachpb.BatchResponse
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{
							DeprecatedFailedRanges: []roachpb.RangeDescriptor{{
								RangeID:  42,
								StartKey: roachpb.RKey(r.Spans[0].Key),
								EndKey:   roachpb.RKey(r.Spans[0].EndKey),
							}},
						})
						return &resp, nil
					}
					return ds.Send(ctx, ba)
				}))
				require.Regexp(t, "protected ts verification error: failed to verify protection."+
					"*\nrange ID: 42, range span: /Table/42 - /Table/43",
					ptv.Verify(ctx, r.ID).Error())
				ensureVerified(t, r.ID, false)
				release(t, r.ID)
			},
		},
		{
			name: "verification succeeded",
			test: func(t *testing.T) {
				defer senderFunc.Store(senderFunc.Load())
				r := createRecord(t, 42)
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						var resp roachpb.BatchResponse
						resp.Add(&roachpb.AdminVerifyProtectedTimestampResponse{})
						return &resp, nil
					}
					return ds.Send(ctx, ba)
				}))
				require.NoError(t, ptv.Verify(ctx, r.ID))
				ensureVerified(t, r.ID, true)
				// Show that we don't send again once we've already verified.
				sawVerification := false
				senderFunc.Store(kv.SenderFunc(func(
					ctx context.Context, ba roachpb.BatchRequest,
				) (*roachpb.BatchResponse, *roachpb.Error) {
					if _, ok := ba.GetArg(roachpb.AdminVerifyProtectedTimestamp); ok {
						sawVerification = true
					}
					return ds.Send(ctx, ba)
				}))
				require.NoError(t, ptv.Verify(ctx, r.ID))
				require.False(t, sawVerification)
				release(t, r.ID)
			},
		},
	} {
		t.Run(c.name, c.test)
	}
}
