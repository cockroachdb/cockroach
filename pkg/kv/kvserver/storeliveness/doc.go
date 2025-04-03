// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This package defines and implements a distributed  heartbeat-based failure
// detection mechanism. It is used by Raft and powers leader leases.
//
// 1. API
//
// Store liveness provides a simple API that allows callers to ask if the local
// store considers another store to be alive. The signal whether a store is
// alive or not is expressed in terms of pair-wise directed support between
// stores.
//
// Store liveness support has two components:
//
// - epoch: a monotonically increasing integer tracking the current
//   uninterrupted period of support.
//
// - expiration: a timestamp denoting the end of the support period for this
//   epoch, unless it's extended before support for the epoch is withdrawn.
//
// The store liveness API defined in `fabric.go` provides the following methods:
//
// - `SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool)`: returns the current
//   epoch and whether support is provided for the specified store.
//
// - `SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp)`: returns the
//   current epoch and expiration timestamp of support received from the
//   specified store.
//
//
// 2. Algorithm Overview
//
// A local store tracks support received from and provided to remote stores.
// Stores request and provide support using an RPC-based heartbeat mechanism.
//
// 2.1. Requesting Support
//
// A local store requests support from a remote store by sending a heartbeat
// message containing an epoch and expiration. Epochs are monotonically
// increasing, and expirations are timestamps drawn from the requester's clock
// and extended to a few seconds into the future. See `requester_state.go` for
// implementation details.
//
// 2.2. Providing Support
//
// A local store also receives support requests from remote stores. The local
// store can decide to either provide support or not based on the epoch in the
// support request. If the epoch is greater than of equal to the one the local
// store has recorded for the remote store, it records/extends the expiration
// with the requested one. Otherwise, it rejects the support request and doesn't
// change the support state for the remote store. Regardless of the outcome, the
// local store responds with the current support state for the remote store. See
// `supporter_state.go` for implementation details.
//
// 2.3. Withdrawing Support
//
// A local store periodically checks if the support it provides for remote
// stores has expired. When it determines that an expiration timestamp for a
// given remote store is in the past, it increments the remote store's epoch and
// sets the expiration to zero. This effectively withdraws support for the
// remote store. By incrementing the epoch, it ensures that providing support
// for a given epoch is a one-way transition. See `supporter_state.go` for
// implementation details.
//
// 2.4. Other State Management
//
// A local store also increments its own epoch upon restart to ensure that any
// support received prior to the restart is invalidated. This is not strictly
// necessary for the store liveness correctness, but it's useful for the Raft
// and leader lease properties. Support provided for other stores is not
// withdrawn upon restart, as the local store has made a promise to provide
// support until the expiration time.
//
// In order to ensure many of the properties in the next section, the algorithm
// also persists a lot of the support state to disk. See
// `storelivenesspb/service.proto` for details on why each individual field
// needs to be persisted or not. Notably, support received from other stores is
// not persisted since all of that support will be invalidated upon restart (by
// incrementing the epoch).
//
// Store liveness support is established on a per-need basis, as opposed to
// between all pairs of stores in the cluster. The need for support is driven
// by Raft (more on this in the Usage/Raft section). When support from a remote
// store is not requested for some time, the local store stops heartbeating the
// remote store.
//
//
// 3. Properties and Guarantees
//
// The main store liveness guarantees are:
//
// - Support Durability: If a store had support for another store in a given
//   epoch, either support is still upheld or has expired according to the
//   support provider's clock.
//
// - Support Disjointness: For any requester and supporter, no two support
//   intervals overlap in time.
//
// These and other safety guarantees are formally specified
// and verified using TLA+ in `docs/tla-plus/StoreLiveness/StoreLiveness.tla`.
//
// The store liveness algorithm remains correct in the presence of the following
// faults:
//
// - Node crashes and restarts: State is persisted to disk to handle restarts.
//   See `storelivenesspb/service.proto` for details on how each persisted field
//   is used.
//
// - Disk stalls: Calls to `SupportFor` and `SupportFrom` do not perform disk
//   reads or writes, so no store liveness callers stall. Moreover, heartbeating
//   requires a disk write, so a store with a stalled disk cannot request
//   support.
//
// - Partitions and partial partitions: Pairwise support between stores helps
//   react to any partitions by withdrawing support from unreachable stores.
//
// - Message loss and re-ordering: Lost heartbeats are simply resent on the next
//   iteration of the heartbeat loop, and lost heartbeat responses are replaced
//   by the responses to subsequent heartbeats. Neither hurts correctness, but
//   could hurt liveness as support could expire in the meantime. Re-ordered
//   messages are handled by ignoring stale messages (with a lower epoch or
//   expiration).
//
// - Clock skew and drift: Clock skew and drift do not affect the correctness of
//   store liveness but could hurt liveness if, for example, a support
//   requester's clock is much slower than the support provider's clocks. For
//   the support disjointness property, we need to propagate clock readings
//   across messages and forward the receiver's clock in order to preserve
//   causality (see `MessageBatch.Now` in `storelivenesspb/service.proto`).
//
// - Clock regressions upon startup: While clocks are usually assumed to be
//   monotonically increasing even across restarts, store liveness doesn't rely
//   on this property. See the comment for `SupporterMeta.MaxWithdrawn` in
//   `storelivenesspb/service.proto`.
//
// 4. Usage
//
// As a general-purpose failure detection layer, store liveness can be
// queried by any component at or below the store level. However, the primary
// consumer of store liveness signal is Raft in service of leader leases.
//
// 4.1. Raft and Leader Leases
//
// Raft uses fortification to strengthen leader election with a promise of store
// liveness support. A Raft leader is considered fortified when it has been
// elected as the leader via Raft, and its store has received support from the
// stores of a quorum of peers. The earliest support expiration among these
// peers (the quorum supported expiration or QSE) serves as a promise that these
// peers will not campaign or vote for a new leader until that time. This
// promise guarantees the fortified leader that it hasn't been replaced by
// another peer, and can thus serve as the leaseholder until the QSE.
//
// A leader uses the ability to evaluate support from a quorum in two ways:
//
// - To determine whether to step down as leader.
// - To determine when a leader lease associated with the leader's term expires.
//
// Conceptually, leader fortification can be thought of as a one-time Raft-level
// heartbeat broadcast which hands the responsibility of heartbeating the
// leader's liveness over to the store liveness layer. See
// `raft/tracker/fortificationtracker.go` for more details on the interaction
// between store liveness and Raft.
//
// Note that the expiration timestamps that store liveness exposes are used as
// timestamps from the MVCC domain, and not from the real time domain. So, it's
// important that these expirations always come from the support requester's
// clock, which in the leader lease layer is potentially the leaseholder that
// uses the expiration as a signal for lease expiration. This also implies that
// when the local store has support up to time t from another store, it doesn't
// matter exactly when the other store withdraws support after its clock exceeds
// time t; both stores have agreed that the promise for support applies up to
// MVCC time t (e.g. proposals with timestamps less than t).
//
// 5. Key Components
//
// 5.1. SupportManager
//
// The `SupportManager` orchestrates store liveness operations. It handles:
//
// - Sending periodic heartbeats to other stores.
// - Processing incoming heartbeat messages.
// - Periodically checking for support withdrawal.
// - Adding and removing stores from the support data structures.
//
// The `SupportManager` delegates these operations to the
// `supporterStateHandler` and `requesterStateHandler` components, which
// implement the actual logic for maintaining correct store liveness state.
//
// 5.2. Transport
//
// The `Transport` handles network communication for store liveness messages.
// This component is instantiated at the node level and maintains message
// handlers for the individual stores on the node. It provides:
//
// - Asynchronous message sending and receiving. `Transport` stores outgoing
//   messages in per-destination-node queues, while incoming messages are
//   streamed and routed to per-store receive queues in each store's
//   `SupportManager`.
//
// - Message batching for efficiency. Outgoing messages are batched by
//   accumulating them in the send queues for a short duration (10ms). This
//   process is also aided by synchronizing the sending of heartbeats across all
//   stores on a given node.
//
// 5.3. Configuration
//
// Store liveness can be enabled/disabled using the `kv.store_liveness.enabled`
// cluster setting. When the setting is disabled, store liveness will not send
// heartbeats, but will still respond to support requests by other stores, as
// well as calls to `SupportFor` and `SupportFrom`. This is required for
// correctness.
//
// Additionally, `config.go` defines tunable configuration parameters for the
// various timeouts and intervals that store liveness uses. Other intervals
// (like support duration and heartbeat interval) are defined in
// `RaftConfig.StoreLivenessDurations()` in `base/config.go` in order to stay
// more closely tuned to Raft parameters.
//
// 5.4. Observability
//
// `TransportMetrics` and `SupportManagerMetrics` in `metrics.go` expose various
// metrics for monitoring store liveness status, including heartbeat success and
// failure rates, support relationship statistics (support provided and
// received), transport-level metrics, and more.
//
// Store liveness support state is also exposed via `inspectz` endpoints
// (defined in `inspectz/inspectz.go`):
//
// - `inspectz/storeliveness/supportFor`: A list of all stores for which the
//   local store provides support, including epochs and expiration times.
//
// - `inspectz/storeliveness/supportFrom`: A list of all stores from which the
//   local store receives support, including epochs and expiration times.
//
// At the default logging level, store liveness logs important state transitions
// like starting to heartbeat a store, providing/receiving support for the first
// time, and withdrawing support. At vmodule level 2, it logs additional info
// like the number of heartbeats sent and messages handled. At vmodule level 3,
// it logs all support extensions as well.
//
// 5.5. Testing
//
// The package includes comprehensive tests in:
//
// - `support_manager_test.go`: Unit tests for the `SupportManager`.
//
// - `transport_test.go`: Unit tests for the `Transport`.
//
// - `store_liveness_test.go`: Data-driven unit tests for the store liveness
//   logic  implementation.
//
// - `multi_store_test.go`: End-to-end tests that validate the store liveness
//   behavior in the happy case and the three major fault patterns: node
//   restarts, disk stalls, and partial network partitions.

package storeliveness
