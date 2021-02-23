// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colcontainer_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/colcontainerutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/marusama/semaphore"
	"github.com/stretchr/testify/require"
)

type fdCountingFSFile struct {
	fs.File
	onCloseCb func()
}

func (f *fdCountingFSFile) Close() error {
	if err := f.File.Close(); err != nil {
		return err
	}
	f.onCloseCb()
	return nil
}

type fdCountingFS struct {
	fs.FS
	writeFDs int
	readFDs  int
}

// assertOpenFDs is a helper function that checks that sem has the correct count
// of open file descriptors, and the fs' open file descriptors match up with the
// given expected number.
func (f *fdCountingFS) assertOpenFDs(
	t *testing.T, sem semaphore.Semaphore, expectedWriteFDs, expectedReadFDs int,
) {
	t.Helper()
	require.Equal(t, expectedWriteFDs+expectedReadFDs, sem.GetCount())
	require.Equal(t, expectedWriteFDs, f.writeFDs)
	require.Equal(t, expectedReadFDs, f.readFDs)
}

func (f *fdCountingFS) Create(name string) (fs.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	f.writeFDs++
	return &fdCountingFSFile{File: file, onCloseCb: func() { f.writeFDs-- }}, nil
}

func (f *fdCountingFS) CreateWithSync(name string, bytesPerSync int) (fs.File, error) {
	file, err := f.FS.CreateWithSync(name, bytesPerSync)
	if err != nil {
		return nil, err
	}
	f.writeFDs++
	return &fdCountingFSFile{File: file, onCloseCb: func() { f.writeFDs-- }}, nil
}

func (f *fdCountingFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	f.readFDs++
	return &fdCountingFSFile{File: file, onCloseCb: func() { f.readFDs-- }}, nil
}

// TestPartitionedDiskQueue tests interesting scenarios that are different from
// the simulated external algorithms below and don't make sense to add to that
// test.
func TestPartitionedDiskQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		ctx   = context.Background()
		typs  = []*types.T{types.Int}
		batch = testAllocator.NewMemBatchWithMaxCapacity(typs)
		sem   = &colexecop.TestingSemaphore{}
	)
	batch.SetLength(coldata.BatchSize())

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	countingFS := &fdCountingFS{FS: queueCfg.FS}
	queueCfg.FS = countingFS

	t.Run("ReopenReadPartition", func(t *testing.T) {
		p := colcontainer.NewPartitionedDiskQueue(typs, queueCfg, sem, colcontainer.PartitionerStrategyDefault, testDiskAcc)

		countingFS.assertOpenFDs(t, sem, 0, 0)
		require.NoError(t, p.Enqueue(ctx, 0, batch))
		countingFS.assertOpenFDs(t, sem, 1, 0)
		require.NoError(t, p.Enqueue(ctx, 0, batch))
		countingFS.assertOpenFDs(t, sem, 1, 0)
		require.NoError(t, p.Dequeue(ctx, 0, batch))
		require.True(t, batch.Length() != 0)
		countingFS.assertOpenFDs(t, sem, 0, 1)
		// There is still one batch to dequeue. Close all read files.
		require.NoError(t, p.CloseAllOpenReadFileDescriptors())
		countingFS.assertOpenFDs(t, sem, 0, 0)
		require.NoError(t, p.Dequeue(ctx, 0, batch))
		require.True(t, batch.Length() != 0)
		// Here we do a manual check, since this is the case in which the semaphore
		// will report an extra file open (the read happens from the in-memory
		// buffer, not disk).
		require.Equal(t, 1, sem.GetCount())
		require.Equal(t, 0, countingFS.writeFDs+countingFS.readFDs)

		// However, now the partition should be empty if Dequeued from again.
		require.NoError(t, p.Dequeue(ctx, 0, batch))
		require.True(t, batch.Length() == 0)
		// And the file descriptor should be automatically closed.
		countingFS.assertOpenFDs(t, sem, 0, 0)

		require.NoError(t, p.Close(ctx))
		countingFS.assertOpenFDs(t, sem, 0, 0)
	})

}

func TestPartitionedDiskQueueSimulatedExternal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var (
		ctx    = context.Background()
		typs   = []*types.T{types.Int}
		batch  = testAllocator.NewMemBatchWithMaxCapacity(typs)
		rng, _ = randutil.NewPseudoRand()
		// maxPartitions is in [1, 10]. The maximum partitions on a single level.
		maxPartitions = 1 + rng.Intn(10)
		// numRepartitions is in [1, 5].
		numRepartitions = 1 + rng.Intn(5)
	)
	batch.SetLength(coldata.BatchSize())

	queueCfg, cleanup := colcontainerutils.NewTestingDiskQueueCfg(t, true /* inMem */)
	defer cleanup()

	// Wrap the FS with an FS that counts the file descriptors, to assert that
	// they line up with the semaphore's count.
	countingFS := &fdCountingFS{FS: queueCfg.FS}
	queueCfg.FS = countingFS

	// Sort simulates the use of a PartitionedDiskQueue during an external sort.
	t.Run(fmt.Sprintf("Sort/maxPartitions=%d/numRepartitions=%d", maxPartitions, numRepartitions), func(t *testing.T) {
		queueCfg.CacheMode = colcontainer.DiskQueueCacheModeReuseCache
		queueCfg.SetDefaultBufferSizeBytesForCacheMode()
		// Creating a new testing semaphore will assert that no more than
		// maxPartitions+1 are created. The +1 is the file descriptor of the
		// new partition being written to when closedForWrites from maxPartitions
		// and writing the merged result to a single new partition.
		sem := colexecop.NewTestingSemaphore(maxPartitions + 1)
		p := colcontainer.NewPartitionedDiskQueue(typs, queueCfg, sem, colcontainer.PartitionerStrategyCloseOnNewPartition, testDiskAcc)

		// Define sortRepartition to be able to call this helper function
		// recursively.
		var sortRepartition func(int, int)
		sortRepartition = func(curPartitionIdx, numRepartitionsLeft int) {
			if numRepartitionsLeft == 0 {
				return
			}

			firstPartitionIdx := curPartitionIdx
			// Create maxPartitions partitions.
			for ; curPartitionIdx < firstPartitionIdx+maxPartitions; curPartitionIdx++ {
				require.NoError(t, p.Enqueue(ctx, curPartitionIdx, batch))
				// Assert that there is only one write file descriptor open at a time.
				countingFS.assertOpenFDs(t, sem, 1, 0)
			}

			// Make sure that an Enqueue attempt on a previously closed partition
			// fails.
			if maxPartitions > 1 {
				require.Error(t, p.Enqueue(ctx, firstPartitionIdx, batch))
			}

			// Closing all open read descriptors will still leave us with one
			// write descriptor, since we only ever wrote.
			require.NoError(t, p.CloseAllOpenReadFileDescriptors())
			countingFS.assertOpenFDs(t, sem, 1, 0)
			// Closing all write descriptors will close all descriptors.
			require.NoError(t, p.CloseAllOpenWriteFileDescriptors(ctx))
			countingFS.assertOpenFDs(t, sem, 0, 0)

			// Now, we simulate a repartition. Open all partitions for reads.
			for readPartitionIdx := firstPartitionIdx; readPartitionIdx < firstPartitionIdx+maxPartitions; readPartitionIdx++ {
				require.NoError(t, p.Dequeue(ctx, readPartitionIdx, batch))
				// Make sure the number of file descriptors increases and all of these
				// files are read file descriptors.
				countingFS.assertOpenFDs(t, sem, 0, (readPartitionIdx-firstPartitionIdx)+1)
			}

			// Now, we simulate a write of the merged partitions.
			curPartitionIdx++
			require.NoError(t, p.Enqueue(ctx, curPartitionIdx, batch))
			// All file descriptors should still be open in addition to the new write
			// file descriptor.
			countingFS.assertOpenFDs(t, sem, 1, maxPartitions)

			// Simulate closing all read partitions.
			require.NoError(t, p.CloseAllOpenReadFileDescriptors())
			// Only the write file descriptor should remain open. Note that this
			// file descriptor should be closed on the next new partition, i.e. the
			// next iteration (if any, otherwise p.Close should close it) of this loop.
			countingFS.assertOpenFDs(t, sem, 1, 0)
			// Call CloseInactiveReadPartitions to reclaim space.
			require.NoError(t, p.CloseInactiveReadPartitions(ctx))
			// Try to enqueue to a partition that was just closed.
			require.Error(t, p.Enqueue(ctx, firstPartitionIdx, batch))
			countingFS.assertOpenFDs(t, sem, 1, 0)

			numRepartitionsLeft--
			sortRepartition(curPartitionIdx, numRepartitionsLeft)
		}

		sortRepartition(0, numRepartitions)
		require.NoError(t, p.Close(ctx))
		countingFS.assertOpenFDs(t, sem, 0, 0)
	})

	t.Run(fmt.Sprintf("HashJoin/maxPartitions=%d/numRepartitions=%d", maxPartitions, numRepartitions), func(t *testing.T) {
		queueCfg.CacheMode = colcontainer.DiskQueueCacheModeClearAndReuseCache
		queueCfg.SetDefaultBufferSizeBytesForCacheMode()
		// Double maxPartitions to get an even number, half for the left input, half
		// for the right input. We'll consider the even index the left side and the
		// next partition index the right side.
		maxPartitions *= 2

		// The limit for a hash join is maxPartitions + 2. maxPartitions is the
		// number of partitions partitioned to and 2 represents the file descriptors
		// for the left and right side in the case of a repartition.
		sem := colexecop.NewTestingSemaphore(maxPartitions + 2)
		p := colcontainer.NewPartitionedDiskQueue(typs, queueCfg, sem, colcontainer.PartitionerStrategyDefault, testDiskAcc)

		// joinRepartition will perform the partitioning that happens during a hash
		// join. expectedRepartitionReadFDs are the read file descriptors that are
		// expected to be open during a repartitioning step. 0 in the first call,
		// 2 otherwise (left + right side).
		var joinRepartition func(int, int, int, int)
		joinRepartition = func(curPartitionIdx, readPartitionIdx, numRepartitionsLeft, expectedRepartitionReadFDs int) {
			if numRepartitionsLeft == 0 {
				return
			}

			firstPartitionIdx := curPartitionIdx
			// Partitioning phase.
			partitionIdxs := make([]int, maxPartitions)
			for i := 0; i < maxPartitions; i, curPartitionIdx = i+1, curPartitionIdx+1 {
				partitionIdxs[i] = curPartitionIdx
			}
			// Since we set these partitions randomly, simulate that.
			rng.Shuffle(len(partitionIdxs), func(i, j int) {
				partitionIdxs[i], partitionIdxs[j] = partitionIdxs[j], partitionIdxs[i]
			})

			for i, idx := range partitionIdxs {
				require.NoError(t, p.Enqueue(ctx, idx, batch))
				// Assert that the open file descriptors keep increasing, this is the
				// default partitioner strategy behavior.
				countingFS.assertOpenFDs(t, sem, i+1, expectedRepartitionReadFDs)
			}

			// The input has been partitioned. All file descriptors should be closed.
			require.NoError(t, p.CloseAllOpenWriteFileDescriptors(ctx))
			countingFS.assertOpenFDs(t, sem, 0, expectedRepartitionReadFDs)
			require.NoError(t, p.CloseAllOpenReadFileDescriptors())
			countingFS.assertOpenFDs(t, sem, 0, 0)
			require.NoError(t, p.CloseInactiveReadPartitions(ctx))
			countingFS.assertOpenFDs(t, sem, 0, 0)
			// Now that we closed (read: deleted) the partitions read to repartition,
			// it should be illegal to enqueue to that index.
			if expectedRepartitionReadFDs > 0 {
				require.Error(t, p.Dequeue(ctx, readPartitionIdx, batch))
			}

			// Now we simulate that one partition has been found to be too large. Read
			// the first two partitions (left + right side) and assert that these file
			// descriptors are open.
			require.NoError(t, p.Dequeue(ctx, firstPartitionIdx, batch))
			// We shouldn't have Dequeued an empty batch.
			require.True(t, batch.Length() != 0)
			require.NoError(t, p.Dequeue(ctx, firstPartitionIdx+1, batch))
			// We shouldn't have Dequeued an empty batch.
			require.True(t, batch.Length() != 0)
			countingFS.assertOpenFDs(t, sem, 0, 2)

			// Increment curPartitionIdx to the next available slot.
			curPartitionIdx++

			// Now we repartition these two partitions.
			numRepartitionsLeft--
			joinRepartition(curPartitionIdx, firstPartitionIdx, numRepartitionsLeft, 2)
		}

		joinRepartition(0, 0, numRepartitions, 0)
	})
}
