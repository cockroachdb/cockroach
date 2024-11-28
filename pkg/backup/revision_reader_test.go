// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestGetAllRevisionsCPULimiterPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	first := true
	var numRequests int
	srv, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
			TestingRequestFilter: func(ctx context.Context, request *kvpb.BatchRequest) *kvpb.Error {
				for _, ru := range request.Requests {
					if _, ok := ru.GetInner().(*kvpb.ExportRequest); ok {
						numRequests++
						h := admission.ElasticCPUWorkHandleFromContext(ctx)
						if h == nil {
							t.Fatalf("expected context to have CPU work handle")
						}
						h.TestingOverrideOverLimit(func() (bool, time.Duration) {
							if first {
								first = false
								return true, 0
							}
							return false, 0
						})
					}
				}
				return nil
			},
		}},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	beforeCreate := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE TABLE bar (b INT PRIMARY KEY)`)
	afterCreate := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	k := s.Codec().TablePrefix(keys.DescriptorTableID)
	tableSpan := roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
	allRevs := make(chan []VersionedValues)
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer close(allRevs)
		return GetAllRevisions(ctx, kvDB, tableSpan.Key, tableSpan.EndKey, beforeCreate, afterCreate, allRevs)
	})
	var numResponses int
	g.GoCtx(func(ctx context.Context) error {
		for range allRevs {
			numResponses++
		}
		return nil
	})
	require.NoError(t, g.Wait())
	require.Equal(t, 2, numRequests)
	require.Equal(t, 2, numResponses)
}
