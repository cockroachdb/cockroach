// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestBulkLowPriReadThrottling is a demo test that visualizes the effect of the
// `kv.bulk_low_pri_read.max_rate` cluster setting on low-priority scan
// throughput. It launches an open-loop scan workload with Poisson arrivals and
// prints observed read bandwidth every second. The first 30 seconds run with
// the default (unlimited) rate; the next 30 seconds run with a 10 MiB/s cap.
//
// Run with: ./dev test pkg/kv/kvserver -f TestBulkLowPriReadThrottling -v --show-logs --stream-output
func TestBulkLowPriReadThrottling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	kvDB := tc.Server(0).DB()
	sqlDB := sqlutils.MakeSQLRunner(tc.ServerConn(0))

	// ---------------------------------------------------------------
	// Populate data: 1024 keys × 1 KiB values = 1 MiB per full scan.
	// ---------------------------------------------------------------
	scratchKey := tc.ScratchRange(t)
	const numKeys = 1024
	const valSize = 1024
	keys := make([]roachpb.Key, numKeys)
	for i := range numKeys {
		key := make(roachpb.Key, len(scratchKey)+2)
		copy(key, scratchKey)
		key[len(scratchKey)] = byte(i >> 8)
		key[len(scratchKey)+1] = byte(i)
		keys[i] = key
		require.NoError(t, kvDB.Put(ctx, key, bytes.Repeat([]byte("v"), valSize)))
	}

	// ---------------------------------------------------------------
	// Scan workload: open-loop Poisson arrivals.
	// ---------------------------------------------------------------
	//  avgInterval – mean time between consecutive scan requests.
	//  avgKeys     – mean number of keys each scan covers; the actual
	//                count per request is drawn from a uniform
	//                distribution between 1 and 2 * avgKeys keys.
	// Expected Throughput: avgKeys * valSize / avgInterval = 40 MiB/s
	// ---------------------------------------------------------------
	const avgInterval = 5 * time.Millisecond
	const avgKeys = 200
	bytesScanned := atomic.Int64{}

	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
		for {
			// Poisson inter-arrival: exponentially distributed sleep.
			sleep := time.Duration(rng.ExpFloat64() * float64(avgInterval))
			select {
			case <-time.After(sleep):
			case <-ctx.Done():
				return
			}

			// Pick a random scan span.
			scanSize := 1 + rng.Intn(avgKeys*2)
			startIdx := rng.Intn(numKeys - scanSize)
			endIdx := startIdx + scanSize

			scanReq := &kvpb.ScanRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    keys[startIdx],
					EndKey: keys[endIdx-1].Next(),
				},
			}
			if br, pErr := kv.SendWrappedWithAdmission(
				ctx,
				kvDB.NonTransactionalSender(),
				kvpb.Header{},
				kvpb.AdmissionHeader{Priority: int32(admissionpb.BulkLowPri)},
				scanReq,
			); pErr != nil {
				require.ErrorIs(t, pErr.GoError(), ctx.Err())
			} else if br != nil {
				bytesScanned.Add(br.Header().NumBytes)
			}
		}
	}()

	// ---------------------------------------------------------------
	// Reporter: every second, print the bandwidth observed in the last
	// 500 milliseconds.
	// ---------------------------------------------------------------
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		prevBytes := bytesScanned.Load()
		prevTime := timeutil.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			currBytes := bytesScanned.Load()
			currTime := timeutil.Now()
			bw := float64(currBytes-prevBytes) / currTime.Sub(prevTime).Seconds()
			fmt.Printf("-------bandwidth: %6.2f MB/s\n", bw/(1024*1024))
			prevBytes = currBytes
			prevTime = currTime
		}
	}()

	// ---------------------------------------------------------------
	// Phase 1: 30 s with default rate (effectively unlimited).
	// ---------------------------------------------------------------
	fmt.Printf("=============== Phase 1: unlimited rate (30s) ===============\n")
	time.Sleep(30 * time.Second)

	// ---------------------------------------------------------------
	// Phase 2: 30 s with 10 MB/s rate limit.
	// ---------------------------------------------------------------
	const rateLimitMiB = 10
	fmt.Printf("=============== Phase 2: %d MB/s rate limit (30s) ===============\n", rateLimitMiB)
	sqlDB.Exec(t, fmt.Sprintf(
		"SET CLUSTER SETTING kv.bulk_low_pri_read.max_rate = '%dMiB'", rateLimitMiB,
	))
	time.Sleep(30 * time.Second)

	// Shut down.
	cancel()
	<-scanDone
	<-reportDone
}
