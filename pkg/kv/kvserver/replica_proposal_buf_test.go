// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft"
	"golang.org/x/sync/errgroup"
)

// testProposer is a testing implementation of proposer.
type testProposer struct {
	syncutil.RWMutex
	ds         destroyStatus
	lai        uint64
	enqueued   int
	registered int
}

func (t *testProposer) locker() sync.Locker {
	return &t.RWMutex
}

func (t *testProposer) rlocker() sync.Locker {
	return t.RWMutex.RLocker()
}

func (t *testProposer) replicaID() roachpb.ReplicaID {
	return 1
}

func (t *testProposer) destroyed() destroyStatus {
	return t.ds
}

func (t *testProposer) leaseAppliedIndex() uint64 {
	return t.lai
}

func (t *testProposer) enqueueUpdateCheck() {
	t.enqueued++
}

func (t *testProposer) withGroupLocked(fn func(*raft.RawNode) error) error {
	// Pass nil for the RawNode, which FlushLockedWithRaftGroup supports.
	return fn(nil)
}

func (t *testProposer) registerProposalLocked(p *ProposalData) {
	t.registered++
}

func newPropData(leaseReq bool) (*ProposalData, []byte) {
	var ba roachpb.BatchRequest
	if leaseReq {
		ba.Add(&roachpb.RequestLeaseRequest{})
	} else {
		ba.Add(&roachpb.PutRequest{})
	}
	return &ProposalData{
		command: &kvserverpb.RaftCommand{},
		Request: &ba,
	}, make([]byte, 0, kvserverpb.MaxRaftCommandFooterSize())
}

// TestProposalBuffer tests the basic behavior of the Raft proposal buffer.
func TestProposalBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var p testProposer
	var b propBuf
	b.Init(&p)

	// Insert propBufArrayMinSize proposals. The buffer should not be flushed.
	num := propBufArrayMinSize
	for i := 0; i < num; i++ {
		leaseReq := i == 3
		pd, data := newPropData(leaseReq)
		mlai, err := b.Insert(pd, data)
		require.Nil(t, err)
		if leaseReq {
			expMlai := uint64(i)
			require.Equal(t, uint64(0), mlai)
			require.Equal(t, expMlai, pd.command.MaxLeaseIndex)
			require.Equal(t, expMlai, b.LastAssignedLeaseIndexRLocked())
		} else {
			expMlai := uint64(i + 1)
			require.Equal(t, expMlai, mlai)
			require.Equal(t, expMlai, pd.command.MaxLeaseIndex)
			require.Equal(t, expMlai, b.LastAssignedLeaseIndexRLocked())
		}
		require.Equal(t, i+1, b.Len())
		require.Equal(t, 1, p.enqueued)
		require.Equal(t, 0, p.registered)
	}

	// Insert another proposal. This causes the buffer to flush. Doing so
	// results in a lease applied index being skipped, which is harmless.
	// Remember that the lease request above did not receive a lease index.
	pd, data := newPropData(false)
	mlai, err := b.Insert(pd, data)
	require.Nil(t, err)
	expMlai := uint64(num + 1)
	require.Equal(t, expMlai, mlai)
	require.Equal(t, expMlai, pd.command.MaxLeaseIndex)
	require.Equal(t, expMlai, b.LastAssignedLeaseIndexRLocked())
	require.Equal(t, 1, b.Len())
	require.Equal(t, 2, p.enqueued)
	require.Equal(t, num, p.registered)
	require.Equal(t, uint64(num), b.liBase)
	require.Equal(t, 2*propBufArrayMinSize, b.arr.len())

	// Increase the proposer's applied lease index and flush. The buffer's
	// lease index offset should jump up.
	p.lai = 10
	require.Nil(t, b.flushLocked())
	require.Equal(t, 0, b.Len())
	require.Equal(t, 2, p.enqueued)
	require.Equal(t, num+1, p.registered)
	require.Equal(t, p.lai, b.liBase)

	// Insert one more proposal. The lease applied index should adjust to
	// the increase accordingly.
	mlai, err = b.Insert(pd, data)
	require.Nil(t, err)
	expMlai = p.lai + 1
	require.Equal(t, expMlai, mlai)
	require.Equal(t, expMlai, pd.command.MaxLeaseIndex)
	require.Equal(t, expMlai, b.LastAssignedLeaseIndexRLocked())
	require.Equal(t, 1, b.Len())
	require.Equal(t, 3, p.enqueued)
	require.Equal(t, num+1, p.registered)

	// Flush the buffer repeatedly until its array shrinks. We've already
	// flushed once above, so start iterating at 1.
	for i := 1; i < propBufArrayShrinkDelay; i++ {
		require.Equal(t, 2*propBufArrayMinSize, b.arr.len())
		require.Nil(t, b.flushLocked())
	}
	require.Equal(t, propBufArrayMinSize, b.arr.len())
}

// TestProposalBufferConcurrentWithDestroy tests the concurrency properties of
// the Raft proposal buffer.
func TestProposalBufferConcurrentWithDestroy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var p testProposer
	var b propBuf
	b.Init(&p)

	mlais := make(map[uint64]struct{})
	dsErr := errors.New("destroyed")

	// Run 20 concurrent producers.
	var g errgroup.Group
	const concurrency = 20
	for i := 0; i < concurrency; i++ {
		g.Go(func() error {
			for {
				pd, data := newPropData(false)
				mlai, err := b.Insert(pd, data)
				if err != nil {
					if errors.Is(err, dsErr) {
						return nil
					}
					return errors.Wrap(err, "Insert")
				}
				p.Lock()
				if _, ok := mlais[mlai]; ok {
					p.Unlock()
					return errors.New("max lease index collision")
				}
				mlais[mlai] = struct{}{}
				p.Unlock()
			}
		})
	}

	// Run a concurrent consumer.
	g.Go(func() error {
		for {
			if stop, err := func() (bool, error) {
				p.Lock()
				defer p.Unlock()
				if !p.ds.IsAlive() {
					return true, nil
				}
				if err := b.flushLocked(); err != nil {
					return true, errors.Wrap(err, "flushLocked")
				}
				return false, nil
			}(); stop {
				return err
			}
		}
	})

	// Wait for a random duration before destroying.
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)

	// Destroy the proposer. All producers and consumers should notice.
	p.Lock()
	p.ds.Set(dsErr, destroyReasonRemoved)
	p.Unlock()

	require.Nil(t, g.Wait())
	t.Logf("%d successful proposals before destroy", len(mlais))
}

// TestProposalBufferRegistersAllOnProposalError tests that all proposals in the
// proposal buffer are registered with the proposer when the buffer is flushed,
// even if an error is seen when proposing a batch of entries.
func TestProposalBufferRegistersAllOnProposalError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var p testProposer
	var b propBuf
	b.Init(&p)

	num := propBufArrayMinSize
	for i := 0; i < num; i++ {
		pd, data := newPropData(false)
		_, err := b.Insert(pd, data)
		require.Nil(t, err)
	}
	require.Equal(t, num, b.Len())

	propNum := 0
	propErr := errors.New("failed proposal")
	b.testing.submitProposalFilter = func(*ProposalData) (drop bool, err error) {
		propNum++
		require.Equal(t, propNum, p.registered)
		if propNum == 2 {
			return false, propErr
		}
		return false, nil
	}
	err := b.flushLocked()
	require.Equal(t, propErr, err)
	require.Equal(t, num, p.registered)
}

// TestProposalBufferRegistrationWithInsertionErrors tests that if during
// proposal insertion we reserve array indexes but are unable to actually insert
// them due to errors, we simply ignore said indexes when flushing proposals.
func TestProposalBufferRegistrationWithInsertionErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var p testProposer
	var b propBuf
	b.Init(&p)

	num := propBufArrayMinSize / 2
	for i := 0; i < num; i++ {
		pd, data := newPropData(i%2 == 0)
		_, err := b.Insert(pd, data)
		require.Nil(t, err)
	}

	var insertErr = errors.New("failed insertion")
	b.testing.leaseIndexFilter = func(*ProposalData) (indexOverride uint64, err error) {
		return 0, insertErr
	}

	for i := 0; i < num; i++ {
		pd, data := newPropData(i%2 == 0)
		_, err := b.Insert(pd, data)
		require.Equal(t, insertErr, err)
	}
	require.Equal(t, 2*num, b.Len())

	require.Nil(t, b.flushLocked())

	require.Equal(t, 0, b.Len())
	require.Equal(t, num, p.registered)
}

// TestPropBufCnt tests the basic behavior of the counter maintained by the
// proposal buffer.
func TestPropBufCnt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var count propBufCnt
	const numReqs = 10

	reqLeaseInc := makePropBufCntReq(true)
	reqLeaseNoInc := makePropBufCntReq(false)

	for i := 0; i < numReqs; i++ {
		count.update(reqLeaseInc)
	}

	res := count.read()
	assert.Equal(t, numReqs, res.arrayLen())
	assert.Equal(t, numReqs-1, res.arrayIndex())
	assert.Equal(t, uint64(numReqs), res.leaseIndexOffset())

	for i := 0; i < numReqs; i++ {
		count.update(reqLeaseNoInc)
	}

	res = count.read()
	assert.Equal(t, 2*numReqs, res.arrayLen())
	assert.Equal(t, (2*numReqs)-1, res.arrayIndex())
	assert.Equal(t, uint64(numReqs), res.leaseIndexOffset())

	count.clear()
	res = count.read()
	assert.Equal(t, 0, res.arrayLen())
	assert.Equal(t, -1, res.arrayIndex())
	assert.Equal(t, uint64(0), res.leaseIndexOffset())
}
