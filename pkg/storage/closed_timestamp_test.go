// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestClosedTimestampCanServe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("https://github.com/cockroachdb/cockroach/issues/28607")

	ctx := context.Background()
	const numNodes = 3

	tc := serverutils.StartTestCluster(t, numNodes, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db0 := tc.ServerConn(0)
	// Every 0.1s=100ms, try close out a timestamp ~300ms in the past.
	// We don't want to be more aggressive than that since it's also
	// a limit on how long transactions can run.
	if _, err := db0.Exec(`
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '300ms';
SET CLUSTER SETTING kv.closed_timestamp.close_fraction = 0.1/0.3;
SET CLUSTER SETTING kv.closed_timestamp.follower_reads_enabled = true;
CREATE DATABASE cttest;
CREATE TABLE cttest.kv (id INT PRIMARY KEY, value STRING);
`); err != nil {
		t.Fatal(err)
	}

	var rangeID roachpb.RangeID
	var startKey roachpb.Key
	var numReplicas int
	testutils.SucceedsSoon(t, func() error {
		if err := db0.QueryRow(
			`SELECT range_id, start_key, array_length(replicas, 1) FROM crdb_internal.ranges WHERE "table" = 'kv' AND "database" = 'cttest'`,
		).Scan(&rangeID, &startKey, &numReplicas); err != nil {
			return err
		}
		if numReplicas != 3 {
			return errors.New("not fully replicated yet")
		}
		return nil
	})

	desc, err := tc.LookupRange(startKey)
	if err != nil {
		t.Fatal(err)
	}

	// First, we perform an arbitrary lease transfer because that will turn the
	// lease into an epoch based one (the initial lease is likely expiration based
	// since the range just split off from the very first range which is expiration
	// based).
	var lh roachpb.ReplicationTarget
	testutils.SucceedsSoon(t, func() error {
		var err error
		lh, err = tc.FindRangeLeaseHolder(desc, nil)
		return err
	})

	for i := 0; i < numNodes; i++ {
		target := tc.Target(i)
		if target != lh {
			if err := tc.TransferRangeLease(desc, target); err != nil {
				t.Fatal(err)
			}
			break
		}
	}

	var repls []*storage.Replica
	testutils.SucceedsSoon(t, func() error {
		repls = nil
		for i := 0; i < numNodes; i++ {
			repl, err := tc.Server(i).GetStores().(*storage.Stores).GetReplicaForRangeID(desc.RangeID)
			if err != nil {
				return err
			}
			if repl != nil {
				repls = append(repls, repl)
			}
		}
		return nil
	})

	require.Equal(t, numReplicas, len(repls))

	// Wait until we see an epoch based lease on our chosen range. This should
	// happen fairly quickly since we just transferred a lease (as a means to make
	// it epoch based). If the lease transfer fails, we'll be sitting out the lease
	// expiration, which is on the order of seconds. Not great, but good enough since
	// the transfer basically always works.
	for ok := false; !ok; time.Sleep(10 * time.Millisecond) {
		for _, repl := range repls {
			lease, _ := repl.GetLease()
			if lease.Epoch != 0 {
				ok = true
				break
			}
		}
	}

	if _, err := db0.Exec(`INSERT INTO cttest.kv VALUES(1, $1)`, "foo"); err != nil {
		t.Fatal(err)
	}

	var baRead roachpb.BatchRequest
	baRead.Header.RangeID = desc.RangeID
	r := &roachpb.ScanRequest{}
	r.Key = desc.StartKey.AsRawKey()
	r.EndKey = desc.EndKey.AsRawKey()
	baRead.Add(r)
	baRead.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	// The read should succeed once enough time (~300ms, but it's difficult to
	// assert on that) has passed - on all replicas!
	testutils.SucceedsSoon(t, func() error {
		for _, repl := range repls {
			resp, pErr := repl.Send(ctx, baRead)
			if pErr != nil {
				switch tErr := pErr.GetDetail().(type) {
				case *roachpb.NotLeaseHolderError:
					return tErr
				case *roachpb.RangeNotFoundError:
					// Can happen during upreplication.
					return tErr
				default:
					t.Fatal(errors.Wrapf(pErr.GoError(), "on %s", repl))
				}
			}
			rows := resp.Responses[0].GetInner().(*roachpb.ScanResponse).Rows
			// Should see the write.
			if len(rows) != 1 {
				t.Fatalf("expected one row, but got %d", len(rows))
			}
		}
		return nil
	})

	// We just served a follower read. As a sanity check, make sure that we can't write at
	// that same timestamp.
	{
		var baWrite roachpb.BatchRequest
		r := &roachpb.DeleteRequest{}
		r.Key = desc.StartKey.AsRawKey()
		txn := roachpb.MakeTransaction("testwrite", r.Key, roachpb.NormalUserPriority, enginepb.SERIALIZABLE, baRead.Timestamp, 100)
		baWrite.Txn = &txn
		baWrite.Add(r)
		baWrite.RangeID = repls[0].RangeID

		var found bool
		for _, repl := range repls {
			resp, pErr := repl.Send(ctx, baWrite)
			if _, ok := pErr.GoError().(*roachpb.NotLeaseHolderError); ok {
				continue
			} else if pErr != nil {
				t.Fatal(pErr)
			}
			found = true
			if !baRead.Timestamp.Less(resp.Txn.Timestamp) || resp.Txn.OrigTimestamp == resp.Txn.Timestamp {
				t.Fatal("timestamp did not get bumped")
			}
			break
		}
		if !found {
			t.Fatal("unable to send to any replica")
		}
	}
}
