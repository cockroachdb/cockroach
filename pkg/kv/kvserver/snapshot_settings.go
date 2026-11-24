// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
)

// MaxSnapshotSSTableSize is the maximum size of an sstable containing MVCC/user keys
// in a snapshot before we truncate and write a new snapshot sstable.
var MaxSnapshotSSTableSize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_rebalance.max_sst_size",
	"maximum size of a rebalance or recovery SST size",
	int64(metamorphic.ConstantWithTestRange(
		"kv.snapshot_rebalance.max_sst_size",
		128<<20, /* defaultValue */
		32<<10,  /* metamorphic min */
		512<<20, /* metamorphic max */
	)), // 128 MB default
	settings.NonNegativeInt,
)

// rebalanceSnapshotRate is the rate at which all snapshots are sent.
var rebalanceSnapshotRate = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_rebalance.max_rate",
	"the rate limit (bytes/sec) to use for rebalance and upreplication snapshots",
	32<<20, // 32mb/s
	settings.ByteSizeWithMinimum(minSnapshotRate),
	settings.WithPublic,
)

// snapshotSenderBatchSize is the size that key-value batches are allowed to
// grow to during Range snapshots before being sent to the receiver. This limit
// places an upper-bound on the memory footprint of the sender of a Range
// snapshot. It is also the granularity of rate limiting.
var snapshotSenderBatchSize = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_sender.batch_size",
	"size of key-value batches sent over the network during snapshots",
	256<<10, // 256 KB
	settings.PositiveInt,
)

// snapshotReservationQueueTimeoutFraction is the maximum fraction of a Range
// snapshot's total timeout that it is allowed to spend queued on the receiver
// waiting for a reservation.
//
// Enforcement of this snapshotApplyQueue-scoped timeout is intended to prevent
// starvation of snapshots in cases where a queue of snapshots waiting for
// reservations builds and no single snapshot acquires the semaphore with
// sufficient time to complete, but each holds the semaphore long enough to
// ensure that later snapshots in the queue encounter this same situation. This
// is a case of FIFO queuing + timeouts leading to starvation. By rejecting
// snapshot attempts earlier, we ensure that those that do acquire the semaphore
// have sufficient time to complete.
//
// Consider the following motivating example:
//
// With a 60s timeout set by the snapshotQueue/replicateQueue for each snapshot,
// 45s needed to actually stream the data, and a willingness to wait for as long
// as it takes to get the reservation (i.e. this fraction = 1.0) there can be
// starvation. Each snapshot spends so much time waiting for the reservation
// that it will itself fail during sending, while the next snapshot wastes
// enough time waiting for us that it will itself fail, ad infinitum:
//
//	t   | snap1 snap2 snap3 snap4 snap5 ...
//	----+------------------------------------
//	0   | send
//	15  |       queue queue
//	30  |                   queue
//	45  | ok    send
//	60  |                         queue
//	75  |       fail  fail  send
//	90  |                   fail  send
//	105 |
//	120 |                         fail
//	135 |
//
// If we limit the amount of time we are willing to wait for a reservation to
// something that is small enough to, on success, give us enough time to
// actually stream the data, no starvation can occur. For example, with a 60s
// timeout, 45s needed to stream the data, we can wait at most 15s for a
// reservation and still avoid starvation:
//
//	t   | snap1 snap2 snap3 snap4 snap5 ...
//	----+------------------------------------
//	0   | send
//	15  |       queue queue
//	30  |       fail  fail  send
//	45  |
//	60  | ok                      queue
//	75  |                   ok    send
//	90  |
//	105 |
//	120 |                         ok
//	135 |
//
// In practice, the snapshot reservation logic (reserveReceiveSnapshot) doesn't know
// how long sending the snapshot will actually take. But it knows the timeout it
// has been given by the snapshotQueue/replicateQueue, which serves as an upper
// bound, under the assumption that snapshots can make progress in the absence
// of starvation.
//
// Without the reservation timeout fraction, if the product of the number of
// concurrent snapshots and the average streaming time exceeded this timeout,
// the starvation scenario could occur, since the average queuing time would
// exceed the timeout. With the reservation limit, progress will be made as long
// as the average streaming time is less than the guaranteed processing time for
// any snapshot that succeeds in acquiring a reservation:
//
//	guaranteed_processing_time = (1 - reservation_queue_timeout_fraction) x timeout
//
// The timeout for the snapshot and replicate queues bottoms out at 60s (by
// default, see kv.queue.process.guaranteed_time_budget). Given a default
// reservation queue timeout fraction of 0.4, this translates to a guaranteed
// processing time of 36s for any snapshot attempt that manages to acquire a
// reservation. This means that a 512MiB snapshot will succeed if sent at a rate
// of 14MiB/s or above.
//
// Lower configured snapshot rate limits quickly lead to a much higher timeout
// since we apply a liberal multiplier (permittedRangeScanSlowdown). Concretely,
// we move past the 1-minute timeout once the rate limit is set to anything less
// than 10*range_size/guaranteed_budget(in MiB/s), which comes out to ~85MiB/s
// for a 512MiB range and the default 1m budget. In other words, the queue uses
// sumptuous timeouts, and so we'll also be excessively lenient with how long
// we're willing to wait for a reservation (but not to the point of allowing the
// starvation scenario). As long as the nodes between the cluster can transfer
// at around ~14MiB/s, even a misconfiguration of the rate limit won't cause
// issues and where it does, the setting can be set to 1.0, effectively
// reverting to the old behavior.
var snapshotReservationQueueTimeoutFraction = settings.RegisterFloatSetting(
	settings.SystemOnly,
	"kv.snapshot_receiver.reservation_queue_timeout_fraction",
	"the fraction of a snapshot's total timeout that it is allowed to spend "+
		"queued on the receiver waiting for a reservation",
	0.4,
	settings.FloatInRange(0.25, 1.0),
)

// snapshotSSTWriteSyncRate is the size of chunks to write before fsync-ing.
// The default of 2 MiB was chosen to be in line with the behavior in bulk-io.
// See sstWriteSyncRate.
var snapshotSSTWriteSyncRate = settings.RegisterByteSizeSetting(
	settings.SystemOnly,
	"kv.snapshot_sst.sync_size",
	"threshold after which snapshot SST writes must fsync",
	kvserverbase.BulkIOWriteBurst,
	settings.PositiveInt,
)

// snapshotChecksumVerification enables/disables value checksums verification
// when receiving snapshots.
var snapshotChecksumVerification = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.snapshot_receiver.checksum_verification.enabled",
	"verify value checksums on receiving a raft snapshot",
	true,
)
