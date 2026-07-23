// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package intentresolver

import "github.com/cockroachdb/cockroach/pkg/settings"

// Admission control and locking (including intents):
//
// Admission control (AC) has a work scheduling mechanism based on (tenantID,
// priority, createTime), where the priority values are defined in
// admissionpb.WorkPriority, and can be one of three values for user txns
// {UserLowPri, NormalPri, UserHighPri} (NB: these priorities are different
// from enginepb.TxnPriority used inside concurrency control). All internal
// background work in CockroachDB uses a priority < NormalPri. The high level
// goal is to provide performance isolation of different queries by one of two
// methods:
// - Using AC's tenantID, for fair sharing. AC's tenantID can be decoupled
//   from the serverless concept of tenantID (though this is not done yet), so
//   we consider this more general setup below.
// - Using different priorities.
//
// A challenge here is that AC queueing is barely aware of concurrency
// control, so we have a lock priority inversion problem. Our initial solution
// addressed this in a crude manner: (a) using LockingPri (which is greater
// than UserHighPri) for work from txns that have already acquired locks,
// regardless of the txn's original priority, (b) not subjecting intent
// resolution to AC. But (b) caused overload (see
// https://github.com/cockroachdb/cockroach/issues/97108). And (a) violates
// our isolation goals: e.g. a user A could be running write txns at
// UserLowPri on some set of tables T_A, while another user B could be running
// txns at NormalPri on an unrelated set of tables T_B, and user A will
// interfere with user B since A's txns will get elevated to LockingPri.
//
// The priority inversion problem is complicated from a software structure
// perspective, since it requires taking information from the lock table and
// feeding it into AC queues. And there are more fundamental difficulties:
//
// - Distributed priority inversion: The AC queue on node n1 could have low
//   priority work from txn T1 waiting behind high priority work from txn T2,
//   and low priority work from txn T3 waiting on a lock already held by T1 in
//   the lock table. There is no need for T1 to skip past T2 in the AC queue
//   since T3 is similarly low priority, so the lock release will be delayed.
//   But on node n2, a lock held by T3 could have T2 waiting for it to be
//   released (and no AC queueing). Without distributed knowledge, we cannot
//   notice this transitive dependency of T2 on T1, and therefore the need to
//   elevate the priority of T1.
//
// - Importance inversion: Using tenantID to isolate different workloads could
//   have tenant U1 with lower shares holding a lock and tenant U2 with higher
//   shares waiting on that lock. That is, the general problem cannot be
//   captured solely using priorities.
//
// We define the performance isolation goals of AC in a more limited manner,
// to sidestep this problem.
//
// Performance isolation between user txns can be achieved using different
// tenantID or different priority only for txns that touch different parts (in
// some partitioning) of the key space, with a few exceptions:
//
// - Lower importance (smaller tenant share or lower priority) non-locking
//   read txns can share the key space with higher importance read or write
//   txns.
//
// - One can switch between lower importance and higher importance (and vice
//   versa) txns on a part of the key space. If there is time overlap during
//   this switch where both lower and higher importance txns are running,
//   priority inversion can happen.
//
// These goals are achieved by the following set of mechanisms:
//
// - Txns that already hold locks/intents use an AC priority that is logically
//   +1 of the original txn priority. Since there are gaps between the
//   different user priorities (UserLowPri=-50, NormalPri=0, UserHighPri=50),
//   work in one part of the key space that executes with NormalPri+1 will not
//   get prioritized over another part of the key space executing at
//   UserHighPri. And when different tenantIDs are being used for performance
//   isolation, the priority is irrelevant. The actual implementation uses a
//   step that is bigger than +1. See
//   admissionpb.AdjustedPriorityWhenHoldingLocks.
//
// - Intent resolution is subject to AC. The baseline (tenantID, priority,
//   createTime) of intent resolution is that of the requester that is trying
//   to resolve the intent. Adopting the same idea of +1, the intent
//   resolution executes at priority+1.
//
// We consider some examples.
//
// Example 1: Consider the case of txns T1, T2, T3, where T2 is a lower
// priority (say -50) read-only transaction, while T1, T3, have the same
// priority, which is higher than T2, say 0. T1 has an intent at key k1, on
// which T2 is waiting. T1 commits, and T2 attempts to resolve this intent
// with an admission priority of -30, and is waiting for admission. Then T3
// also starts waiting on the intent for key k1. It tries to resolve with a
// priority of +10, so can get through the admission queues faster.
//
// Example 2: Similar to the previous case but T1 has not committed by the
// time T3 starts waiting on the intent at k1. Say T1 wants to create another
// intent at k2. T1 will use a priority of +10, which allows it to finish
// acquiring intents and commit faster.
//
// In the above examples, no one is waiting on an intent from the lower
// priority txn T3, since our performance isolation goals do not permit T3 to
// be a writing transaction. However, there are some internal transactional
// writes, like index backfills and TTL writes, that use a lower priority. We
// consider this in the remaining examples.
//
// Example 3: T1 and T3 are writers with priority 0, and T2 is a writer with
// priority -50. T2 has an intent on key k1 that is being waited on by T1. T2
// commits. The intent resolution done by T1 will use priority +10, which is
// desirable (it can get ahead of the intent resolution done by T2 itself,
// which will run at priority -30).
//
// Example 4: T1 and T3 have priority -50, and T2 has priority 0. On an
// overloaded node (so admission control queueing), T1 has an intent on key k1
// that is being waited on by T3. T1 has another request on this node that is
// waiting in the admission queue (priority -30) behind a request from T2
// (priority 0; T2's request is for an unrelated key). There is no priority
// inversion yet. On a different node with no overload, T3 has an intent on
// key k2 that is being waited on by T2. This shows a distributed priority
// inversion since T2 is waiting on T3 which is waiting on T1, and T1's
// request is behind T2's request in the admission queue of the first node.
// This could be addressed if the internal work (txns T1 and T3) knew the
// priority of the user-facing work (txn T2), say specified in a SpanConfig,
// and used that same priority after acquiring locks or when doing intent
// resolution. Since we don't yet have this capability, we have a hack in
// admissionpb.AdjustedPriorityWhenHoldingLocks that bumps the priority of
// lock holding or lock resolving work up to 0 and then applies the logical +1
// to the priority. In this example, it will cause T1 and T3 to have priority
// +10 once they acquire locks or when they are resolving locks, eliminating
// the priority inversion.
//
// Example 5: T1 and T3 have priority 0, and T2 has priority -50. T2 starts
// waiting on an intent of txn T1 at key k1. T3 starts waiting after T2 in the
// lock table queue for the same intent. T1 commits. The lock table logic (at
// the time of writing this comment) will pick the first waiter as the
// designated resolver, so intent resolution will use the priority of T2. Even
// if this designated waiter logic were not there, and intent resolution was
// done by both T2 and T3, we know that T2 is ahead in the lock table queue so
// will acquire the lock first. And if T2 wants to acquire more locks after
// the one on k1 we could have a priority inversion in that T3 is waiting on a
// lock held by T2, while T2 is starved by other priority 0 work. The same
// proper fix as example 4 applies here, and so does the same hack -- T2 will
// resolve using priority +10 and will acquire subsequent intents with
// priority +10.

// sendImmediatelyBypassAdmissionControl sets the admission control behavior
// for the less commonly used sendImmediately option. Since that option is
// used when a waiter on the lock table is trying to resolve one intent, and
// the waiter has already been admitted, the default is to bypass admission
// control.
var sendImmediatelyBypassAdmissionControl = settings.RegisterBoolSetting(
	settings.SystemOnly, "kv.intent_resolver.send_immediately.bypass_admission_control.enabled",
	"determines whether the sendImmediately option on intent resolution bypasses admission control",
	true)

// batchBypassAdmissionControl sets the admission control behavior for the
// more common case of batched intent resolution. By default, this does not
// bypass admission control.
var batchBypassAdmissionControl = settings.RegisterBoolSetting(
	settings.SystemOnly, "kv.intent_resolver.batch.bypass_admission_control.enabled",
	"determined whether batched intent resolution bypasses admission control", false)
