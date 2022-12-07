- Feature Name: Admission controls for follower writes
- Status: draft
- Start Date: 2022-12-07
- Authors: Andrew Baptist, Irfan Sharif, Sumeer Bhola
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

Follower writes can defeat local admission control today. Specifically attempts to maintain the health of the Pebble LSM with minimal delays to high-priority traffic. Follower writes can not be subject to local admission control or delays due to the head-of-the-line sequential nature of Raft. This proposal adds a feedback mechanism across nodes that allows the leaseholder to delay and prioritize proposals to a range before proposal evaluation. This change prevents both the impact of severely inverted LSMs without admission control and the unacceptable increase in foreground latency with admission control enabled.

Two new concepts are introduced:
 * *Remote Tracking Window (RTW)* - Tracks the amount of additional data that can be sent to a remote store without overloading it.
 * *Absorption Rate* - The sustainable rate at which a store can absorb incoming requests. This is an enhancement to the existing `store_token_estimator`.


# Motivation

The underlying storage system of CockroachDB is a Log Structured Merge-Tree (LSM). An LSM typically has a pyramid shape with a small number of SSTables at the top, and an increasing number at each level below.  Data flows from the top of the LSM down to the bottom through a series of compactions. An inverted LSM, means there are a high number of SSTables at the top/L0 level. This causes high read amplification and unacceptable latency to a user's workload. Recovering from a severely impacted LSM takes minutes to hours depending on the level. There have been numerous support cases due to LSM inversion throughout the history of Cockroach Labs.

There are two primary reasons why an LSM becomes inverted. The first is due to a foreground write workload that is unsustainable for the system. If proposals are accepted by Raft faster than they can be flushed to lower levels, the LSM will not be able to maintain its structure and eventually will become inverted. The second reason is due to large bulk operations, such as index creations or schema changes, that generate new SSTables from existing data on the system at a prodigious rate. In this case, depending on the size of the background operation and where the SSTables are injected, the LSM can become temporarily inverted. 

A design goal of admission control is to help with this problem by slowing down the rate of incoming proposals to match the rate of the compaction and flushing rate of the LSM. Admission control can only slow down the leaseholder, and a majority of writes to the storage layer are follower writes. In a perfectly balanced system, all leaseholders will see the signaling simultaneously, and cooperatively keep the LSM of all stores in the system in check. Unfortunately, due to local hotspots in both user traffic and bulk operations, followers can be significantly more inverted than the leaseholder.

Another consideration for the design is to handle the noisy neighbor multi-tenant problem correctly. In a multi-tenant system, it is acceptable for a single tenant to add self-imposed latency by writing at an unsustainable rate. They should not appreciably slow down other tenants on the system. Additionally, a tenant running a bulk job should not slow down either themselves or other tenants on the system. Acknowledging that there are some shared resources that are not fully accounted for, this proposal will not achieve perfect tenant isolation, but it will be significantly improved from the situation today.

# Technical design

This proposal introduces two new concepts to admission control, the Remote Tracking Window (RTW) and the store absorption rate.  The RTW is maintained on the sender, and the absorption rate is maintained on the receiver. On followers, proposals are always accepted and processed immediately, however, the RTW allows the sender to moderate its rate, and the absorption rate is used on the receiver to return tokens to senders.

In the current system, there are two primary places where requests are queued. The first is in admission control queuing, potentially waiting for the CPU or IO permits at the local store. In a healthy system, there is typically no delay as most of the time admission control is not active. After admission and evaluation, the request may also need to wait on locks and latches. Lock and latch contention is rare for most use cases as the locks and latches are fine-grained and most systems have low row-level contention. This design calls for modifying the AC queuing before evaluation to take into account the health of followers. The AC CPU queue is not modified in this design, but the IO queue is.

The RTW is a set of permits kept on the sender side of proposals (leaseholder) to allow the sender to make intelligent decisions before proposal evaluation. The receiver does not release the permits back to the sender until it can fully absorb the impact of the work. Using this cooperative mechanism, the sender will detect a follower who is falling behind and queue requests to those followers pre-admission.

Each leaseholder in the system tracks an RTW for every follower store that it writes to, including itself. The RTW has permits for each follower and will delay sending a request if there are no permits left. Before evaluation, it is difficult to know the exact number of tokens that are required for a writer so we do not deduct tokens here. Instead, we only check if the RTW has at least one token available and if so, will allow the write to proceed to the CPU queue. Post-evaluation and before sending a request to a follower, we do have the exact size of the request and deduct the tokens at that point.

On the receiver, the goal is to match the incoming proposal rate to the internal rate of recovering permits by compacting the LSM and moving things to lower levels.  Internally it estimates a rate of incoming requests, `Absorption Rate`,  that it can accept while keeping the LSM healthy (details below).  When a proposal is received, it is always evaluated immediately and logically returns two responses to the sender. The first `BatchResponse` is the same that is returned today once the data is durable on disk. The second, `BatchAbsorbed`, is sent when the impact of the evaluation has been fully absorbed by the system. In cases where the LSM is healthy, the messages are coalesced, using a new boolean field `absorbed` on `BatchResponse`. In the case where the receiver is not healthy, the `BatchAbsorbed` is sent once the impact of the request has been fully absorbed.

The absorption rate is currently implemented as a linear model in `store_token_estimator.go`. The `availableIOTokens` is set as infinite when the system is not overloaded and is set to a value when the system is overloaded to prevent additional overload. As part of this work, a function will be applied to the LSM overload so that available tokens begin dropping well before the store hits the threshold of IO overload.

This mechanism alone will keep the LSM from being inverted on follower nodes, however, to meet the additional goals of minimizing noisy neighbors two additional enhancements are needed on the sender RTW and receiver Absorption Rate. 

On the receiver side, the key insight is that the requests which are signaled as absorbed can be different from the messages which are actually absorbed. The receiver can prioritize signaling absorption on high-priority messages in a mechanism that is fair across tenants and stores. The existing mechanism of AC already handles prioritization both between tenants and between messages within a tenant using the Priority field on the AC header. By breaking down the `Absorption Rate` between tenants, it will preferentially choose tenants that write less. Additionally, the receiver can be fair between stores as well. The initial implementation will break the rate fairly between all tenants and stores, however, this could be weighted in the future. Within a tenant, higher-priority messages are always marked as absorbed before lower-priority messages. While this doesn't affect the receiver at all, it can be a signal to the sender's RTW to allow it to preferentially send high-priority work.

On the sender side, a similar mechanism is used to fairly allocate its RTW between tenants. The per-store RTW is divided by the tenants on the system and requests with higher priority are processed before lower-priority requests. To ensure that bulk operations do not impact user operations, additional protection is granted where lower priority requests are not able to use more than a predefined fraction (e.g. 1/2) of the full RTW (the `elastic_threshold`). Once the RTW to a store is exhausted additional proposals that would go to a store are queued until the RTW for that store is available. To prevent a noisy neighbor problem on the sender side, a fraction (e.g. 1/2) of the RTW is allocated per tenant (the `tenant_reservation`), and the rest is available for all tenants. This balances the need to allow bursts on an otherwise healthy system with the need to prevent a single burst from blocking other tenants' traffic. 

With this proposal implemented, two other fail-safes in the system need to be re-evaluated. the quota pool and follower pausing. 

The quota pool on ranges should be removed once this is implemented. The quota pool solves a similar problem but needs to correctly process feedback about the absorption rate, and it kicks in too late to be useful. The quota pool is also on a per-range basis rather than a store level.

Gray failures are not handled well with this feature alone. Once a node hits a gray failure, its absorption rate will drop. If it drops lower than the cumulative incoming rate, the RTW on the senders will drop to zero. This has the impact that eventually all other stores in the system will stall proposals that touch this store. Integration with the allocator to move replicas off this node is required long term, but to prevent short-term problems term follower pausing is required. Ideally, follower pausing will engage when it believes the remote store's absorption rate is artificially low, not just that the RTW is full. Testing will be required to tune this correctly. 

## Drawbacks

Even in a perfectly healthy system, there is a time delay between when a node sends a message (and deducts it from its RTW) and when the receiver can acknowledge back. This can be as high as the bandwidth-delay product between nodes. Assuming that a node can normally sustain 1 GB/s of traffic and the RTT is 200 ms, then it would require a window of 200 MB to guarantee that we are work conserving. 


## Rationale and Alternatives

Several alternatives were explored when considering this proposal.

* Pausing after proposal evaluation. This can cause priority inversion and possible deadlocks due to the single-threaded mechanism of Raft. Even if this is separated by range, the complexities of multi-range transactions make this untenable.

* Similar to TCP congestion control, allowing the sender to estimate the size of the RTW dynamically by allowing the receiver to reject proposals if they would otherwise go over a threshold. This was not ideal because it would greatly complicate the consensus mechanism of our system. 

* Storing a separate window for each combination of store \* tenant. This would have allowed more isolation between tenants but has two drawbacks. First, the aggregate size of the RTW for all stores becomes large if there are a large number of active tenants. Second, the system is more responsive to general overload if multiple tenants are causing the overload by having a shared reservation. Still some open questions about this: How does this handle a large number of active tenants? Does the `tenant_reservation` for the RTW handle this well enough on a non-overloaded system? On an overloaded system is it still fair enough?

* The absorption window could also include other types of queueing (for instance CPU queuing ). In customer systems, the CPU is frequently more of the bottleneck than IO, however, we are not doing this initially for two reasons. 1) CPU queues clear quickly (ms) vs Storage IO queues (minutes) so the timing of absorption messages is too slow to react to CPU changes. 2) High CPU queues will naturally delay proposal evaluation on the follower since "everything" is slowed down. This is not ideal, but might be acceptable for now. Testing is required to determine how systems that have "high CPU followers'' react. The more immediate solution is likely to move leaseholders off these nodes, but due to leaseholder preference, this may not be possible.

# Unresolved questions

* It is unclear if the handling of gray failures will be sufficient. This will require a fair bit of testing before changes are made to the quota pool, follower pausing, or any other mechanisms.

* Is it better to estimate the deduction from the RTW pre-evaluation or wait until post-evaluation and have a more accurate deduction (or fix the estimate)? Do we need to use grant chaining if we don't do the pre-evaluation deduction?

* For a system with a lot of stores, is the static per receiver RTW sufficient? Alternatively, the RTW could be sized to have an aggregate per receiver that is communicated from the receiver to senders and updated when there are new stores that begin sending to this store.


