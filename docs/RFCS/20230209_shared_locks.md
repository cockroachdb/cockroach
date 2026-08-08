- Feature Name: Shared Locks
- Status: draft
- Start Date: 2023-02-09
- Authors: Arul Ajmani, Nathan VanBenschoten
- RFC PR: TODO(arul)
- Cockroach Issue: https://github.com/cockroachdb/cockroach/issues/91545

# Summary

SQL databases support multiple forms of row-level locks, which are locks held by
transactions on individual rows. The most common forms of these locks are shared
locks and exclusive locks. Shared locks are to reader locks as exclusive locks
are to writer locks in a reader-writer mutex.

This RFC proposes introducing SHARED key-level locks in the key-value layer and
exposing them through the `SELECT FOR SHARE` SQL statement. This will enable the
use of SHARED locks externally through `SELECT FOR SHARE` statements. It will
also enable their use internally for operations such as foreign key checks under
weaker isolation levels (see: Read Committed).

# Motivation

When transactions run under Serializable isolation, correctness is guaranteed by
virtue of the isolation level. This isn’t true when it comes to weaker isolation
levels. Users use explicit SHARED locks (via `SELECT FOR SHARE`) to enforce
correctness in their application logic when running under a weaker isolation
level such as Read Committed. Similarly, weaker isolation levels also require
SHARED locks for correctness of system level data consistency checks, such as
foreign key or uniqueness checks.

Shared locks also provide benefits to serializable transactions. For example, it
allows those crafting queries to protect their refresh spans. Doing so
guarantees transactions whose write timestamp gets pushed can commit without
getting starved. Tools such as SELECT FOR SHARE also give users fine grained
control to manage their fairness vs. predictability continuum. This will enable
us to make changes in our transaction protocol in the future, while still giving
users the ability to deal with isolated regressions as a result.

Lastly, implementing SHARED locks and surfacing them through SELECT FOR SHARE
brings us closer to the ORM/PG compatibility dream. Today, CRDB accepts SELECT
FOR SHARE statements, but these are treated as a no-op internally. This breaks
user expectations, which is something we have seen in customer escalations.

# Technical design

## KV API

When SQL issues read requests (eg. `Get`, `Scan`, `RevScan`) to KV, it specifies
a locking strength for the request. Currently, the only supported values are
Exclusive and None (which corresponds to a non-locking read).

All read requests in KV are sequenced through the `concurrency.Manager`. This
involves acquiring latches over spans read and waiting on any conflicting locks.
Locking reads perform an extra step once they have finished evaluation – they
add locks for all keys they scanned in the lock table before returning. These
locks are released only when the transaction is finalized, i.e, it either
commits or aborts.

Users of the KV API (SQL) will now be able to specify `Shared` as a valid
locking strength for `Get`/`Scan`/`RevScan` requests. Read requests that
correspond to a `SELECT FOR SHARE` statement will make use of this locking
strength. Conveniently, SQL already plumbs down locking strength from parsing,
planning, and execution into the `row.KV{,Batch}Fetcher`, which is the entity
responsible for invoking the KV API. Today, `Shared` lock strengths are ignored
at this level because the KV API does not support it. Enabling `SELECT FOR
SHARE` would imply using a `Shared` lock strength, instead of `None`, when
instantiating a `KV{,Batch}Fetcher` for the statement. Doing so will ensure keys
scanned are correctly protected with locking strength `Shared` for the duration
of the transaction.

## KV Internal Data Structures

Supporting SHARED locks within the KV layer will require changes to multiple
concurrency control related data structures. This includes changes in how locks
will be stored, how concurrent requests will interact with each other, and how
conflict resolution will be performed.

### Locking Strengths

The KV layer allows transactions to acquire locks at a per-key granularity. As
of the time of writing, the only locking strength supported when acquiring locks
is `Exclusive`. Intent locking strength corresponds to a key which has a write
intent on it; None corresponds to an in-flight non-locking read. Shared and
Update lock strengths are currently unsupported, however supporting Shared locks
and `SELECT FOR SHARE` entails changing the former.

Lock strengths are used to resolve conflicts between a held lock and a
concurrent request that overlaps with that lock. A request must wait for the
lock to be released if the two conflict. Otherwise, the request is considered
compatible, and can proceed. The lock compatibility matrix below details such
interactions. A cell with an X means that the two lock modes conflict.

```
+------------+---------+-----------+-----------+-------------+----------+
|            |   None  |  Shared   |  Update   |  Exclusive  |  Intent  |
+------------+---------+-----------+-----------+-------------+----------+
| None       |         |           |           |      X^*    |    X^†   |
+------------+---------+-----------+-----------+-------------+----------+
| Shared     |         |           |           |      X      |    X     |
+------------+---------+-----------+-----------+-------------+----------+
| Update     |         |           |     X     |      X      |    X     |
+------------+---------+-----------+-----------+-------------+----------+
| Exclusive  |   X^*   |     X     |     X     |      X      |    X     |
+------------+---------+-----------+-----------+-------------+----------+
| Intent     |   X^†   |     X     |     X     |      X      |    X     |
+------------+---------+-----------+-----------+-------------+----------+

[†] non-locking reads only conflict with an Intent if their read timestamp is
at or above the Intent's timestamp.

[*] conflict behavior is configurable using a cluster setting; defaults to 
"conflicts" if the exclusive lock is held by a serializable transaction; defaults
to "not conflicts" for exclusive locks held by transactions of lower isolation
levels.
```

As evident by the compatibility matrix above, Shared locks are compatible with
each other. This means keys need to support multiple locks (from different
transactions) at the same time. The invariant we maintain here is that all locks
acquired on a key must be compatible with each other.


### Lock re-acquisition

Transactions are allowed to re-acquire an already held lock with a different
lock strength. There isn’t much benefit to re-acquiring a lock at a lower or
equal lock strength in a transaction from the user's perspective. However, to
the KV layer, doing so is a no-op. The lock continues to be held with the
original lock strength.

A transaction may want to selectively promote a lock it previously acquired with
a weaker strength to a stronger one. The simplest motivating example for this is
a transaction that acquires an `Exclusive` lock as part of a `SELECT FOR
UPDATE`, and then proceeds to actually perform the UPDATE (and thus writes an
`Intent`). One could also imagine a scenario where a transaction promotes a
Shared lock to something stronger, by writing to the key or selecting it again
as a FOR UPDATE.

We allow transactions to promote previously acquired locks by acquiring the lock
with higher strength. However, doing so needs to comply with the compatibility
rules illustrated above – in particular, the new (higher) lock strength must not
conflict with any other locks held on that key by other transactions. If it
does, the request must wait until those locks held by other transactions are
released before promoting its lock to a higher strength.

### Interaction with Savepoint Rollbacks

Consider the case where a transaction acquires a lock, declares a savepoint, and
promotes the acquired lock to something stronger. The lock promotion should be
reverted in case the savepoint is rolled back. A transaction would need to
implicitly demote the strength a lock is held with in such cases.

The rollback semantics are entirely sane because a lock can only ever be rolled
back to something weaker, given the re-acquisition semantics described above.
This means the rollback will never increase the level of protection provided on
the key by the lock.

CockroachDBs savepoint implementation made a [deliberate design
decision](https://github.com/cockroachdb/cockroach/blob/e3abcd5dcea63ad50d4f1b35628538e33e320748/docs/RFCS/20191014_savepoints.md#relationship-with-row-locks)
to not eagerly rollback locks acquired/promoted under savepoints. This is a
deviation from Postgres rollback semantics, and a compatibility gap we may want
to close at some point. With an eye towards such a future, we would want to keep
information about the history of lock acquisitions by a transaction on a
particular key.

### Lock Representation

Locks are stored in a per-range lock table which is maintained on the range’s
leaseholder. Within the lock table, each lock is represented in a lockState
structure. At a high level, a lockState contains information about the lock
holder and a (possibly empty) wait-queue on that lock.

Prior to the introduction of Shared locks there was an implicit assumption that
all locks were held with Exclusive lock strength. A key could only be locked by
a single transaction at any given point in time. By extension, there was a 1:1
mapping between keys and locks. This will no longer be true, as we will have
multiple locks on the same key. However, we want to ensure there continues to be
only one wait queue for a given key. All this, and the desire to keep some
history of lock acquisitions to enable savepoint rollbacks, motivates the
following changes to how locks are stored in the lock table.

```go
type lockedKey struct {
    key     key
    // Invariant: all locks have compatible strengths.
    locks   map<txnID, txnLocks>
    // Invariant: all reservations in the lock wait queue have compatible 
    // strengths with each other and all the locks held on the key. 
    queue   lockWaitQueue
}

// txnLocks tracks information about all locks held by a particular transaction.
// We only track minimal information to achieve the goals described above,
// instead of tracking the entire history of all locks held.
type txnLocks struct {
    // Invariant: we only store lock information for the most recent epoch
    //            of a transaction (that we are aware of). 
    // Invariant: txn.WriteTimestamp == intent timestamp if an intent is 
    //            written at this key.
    txn enginepb.TxnMeta 
    // maxDurability stores the highest durability with which a lock with any
    //strength was acquired by the transaction. 
    maxDurability
    // Invariant: only the lowest sequence number that has not been rolled back
    //            at which a lock was acquired with a particular lock strength
    //            is stored in the infos array.
    //
    // The highest strength with which a transaction has locked a key is the 
    // highest lock strength with an associated non-zero sequence number in the 
    // infos array.
    infos [lock.MaxLockStrength + 1]TxnSeq
}

// anyReplicated is used to ensure we trigger intent resolution only when 
// replicated locks were held on a key.
func (txnLocks) anyReplicated() bool   { ... }

// toLockMode is used when resolving conflicts between incoming requests and
// held locks. 
// 
// The strength of the returned lock.Mode corresponds to the highest strength
// with which a transaction has locked a key, as it is sufficient to perform 
// conflict resolution using just this strength (as opposed to all strengths
// with which a lock is held).
func (txnLocks) toLockMode() lock.Mode { ... }

type lock.Mode struct {
   strength lock.Strength
   ts       hlc.Timestamp
   iso      isolation.Level
}

func locksConflict (l1, l2 lock.Mode) bool
```

#### Savepoint Rollback

We only track the lowest sequence number (that has not been rolled back) at
which a lock was acquired with a particular lock strength. If a savepoint is
rolled back to an equal or higher sequence number, we modify what we are
tracking for the given lock strength. However, if it is rolled back to a lower
sequence number, we simply zero out the associated entry in the `infos` array.

Note that we do not store any durability history for locks acquired by a given
transaction -- we only store the highest durability across different lock
strengths. This field is used to determine whether we need to issue
ResolveIntent requests or not. As a result, if a durable lock is released on
savepoint rollback, we could end up issuing an unnecessary ResolveIntent
request. If this proves to be a problem in practice, we could increase the
resolution for durability history in our tracking.

### Request Sequencing

Incoming requests are sequenced through the concurrency manager. A request first
acquires latches, which provide isolation between concurrent requests, for the
duration of its evaluation. This is followed by scanning the lock table to
perform conflict resolution with concurrent transactions. Lastly, if the request
is a locking request, a lock is acquired before releasing latches.

#### Latch Acquisition

Incoming requests are required to acquire latches over the keyspans they intend
to touch at the request’s timestamp. As of the time of writing, two types of
latches exist – reach latches and write latches. They each have different
conflict resolution semantics. For example, read latches permit read/write
requests at higher timestamps; write latches do not. Write latches also conflict
with all other write requests, regardless of timestamp.

Now that we’ve convinced ourselves these semantics are sane, it’s time to
introduce non-MVCC latches. Non-MVCC latches have no associated timestamp
(timestamp = 0) and they are considered to overlap with all timestamps. This
means non-MVCC read latches conflict with writes at all timestamps, and non-MVCC
write latches conflict with all concurrent reads and writes. The read latching
behavior here might be a bit surprising – naively, one would not expect a read
latch at timestamp 0 conflict with anything at all.

Requests that acquire shared locks protect its transaction against writes at
higher timestamp. As such, the current read latch semantics are not sufficient
to provide such isolation. We instead want shared locks to conflict with all
incoming writes – not just those below the read requests timestamp. We can
achieve this in one of two ways – either by having shared lock requests acquire
a non-MVCC read latch or by acquiring a read latch at the maximum timestamp.
We’ll do the latter, as it’s a more intuitive way to represent a read latch that
conflicts with all writers.

The discussion above begs a different question – does declaring latches as
read/write operations make sense in a world with multiple locking strengths?
Alternatively, we could represent the strength of latches using lock.Modes (pair
of lock.Strength, timestamp) and use this to define conflicts? This isn’t in
scope of this RFC, however.

The latching semantics proposed here will need tweaking once we introduce
[replicated locks other than
intents](https://github.com/cockroachdb/cockroach/issues/100193). In particular,
we will need to serialize requests from the same transaction trying to acquire
shared locks, while still allowing different transactions to acquire shared
locks concurrently. This can be done by acquiring a (write) latch on a local,
transaction specific key (say /Local/LockTable/<key>/<txnID>).


#### Lock Table Sequencing

Once a request has acquired latches, it is sequenced through the lock table,
where it scans all (if any) locks that overlap with its span and determines if
there is a conflict.

The introduction of shared locks does not change much here. There may be more
than one lock on a key, and the sequencing request must ensure it is compatible
(see lock modes) with all of these locks before proceeding.

If the request conflicts with any of the locks held on a key, it must either
join a pre-existing wait queue or establish a new one.

### Lock Wait-Queue

Requests that run into conflicting locks form a queue behind it. They wait in
the queue until either the lock is released or it is “pushed out of the way”.
Requests also perform liveness checks on the lock holder and try to detect
deadlocks with the lock holder transaction. Only one waiter needs to perform
liveness checks, whereas each waiting request needs to detect deadlocks (as
deadlocks are a function of the transaction that holds the lock and the
transaction the waiting request belongs to).

The mechanism by which all this happens is through periodic `PushTxn` requests.
A `PushTxn` request involves a pusher transaction (one belonging to the waiting
request) and a pushee transaction (the transaction that holds the lock). The
introduction of shared locks means that multiple transactions can hold locks
over a key at any given time. As such, we have multiple candidate pushee
transactions in the lock’s wait queue. It is sufficient for us to pick any one
transaction that holds the shared lock and treat it as the pushee, effectively
waiting on it. We can continue to pick transactions to wait on randomly until
all transactions have released their locks. To convince ourselves this is
copacetic, let's walk through various touchpoints in the lock table waiter that
are impacted.

#### Liveness Detection

After some delay, the first waiter (termed distinguished waiter) in the lock
wait queue is responsible for pushing the lock holder’s transaction with the
intention of detecting coordinator crashes. Now that a key can be locked by
multiple transactions, any of their transaction coordinators can crash, and
waiters must detect this. Consider the case where only one of the holders’
transaction coordinator dies. If it's the transaction we randomly chose to push,
we'll detect it as a result of the push. However, if a different transaction's
coordinator crashes, it will only be detected once we pick it as the transaction
to push. This is guaranteed to happen by construction, as all other lock
holders' coordinators are active. As such, they will either release their locks 
on finalization (explicit commit or abort) or they will get aborted by deadlock
detection. We are also guaranteed that the transaction whose coordinator crashed
will be picked for a push eventually, as because there is a waiter, the number
of transactions that are able to acquire the shared lock cannot grow
unboundedly. The exact semantics of this are described in the section on
"queueing vs. joint reservations" below.

This argument can be extended to more than one holders’ coordinators crashing.

#### Deadlock Detection

CRDBs distributed deadlock detection works using a combination of `PushTxn` and
`QueryTxn` requests. After some delay, which is longer than the liveness push
delay, all waiters push the lock holder’s transaction. If the lock holder’s
transaction is still pending, and can’t be “pushed out of the way”, the pushee
waits for the transaction to be finalized. While doing so, it periodically
queries its own transaction record using a `QueryTxn` request which returns a
list of dependencies. Deadlock detection is then a simple case of finding a
dependency cycle, with an ever increasing dependency view.

The argument for why deadlock detection continues to work is very similar to the
liveness one above. The only difference is that instead of detecting a failed
coordinator, the result of the `PushTxn` is that one of the two transactions
that form the deadlock gets aborted.

#### Reservations

Reservations are a mechanism by which the lock table prevents incoming requests
from racing against requests that may have been waiting on a lock once it is
released. Once a lock is released, the first locking waiter is able to reserve
the lock and proceed with its scan of the lock table, possibly waiting and
reserving other locks. Once the request completes its scan of the lock table and
finishes evaluation, it can replace its reservation with an actual lock. The
reservation, as long as it’s around (more on this later), is what prevents other
requests from acquiring the lock.

Before the introduction of Shared locks, there could only be a single
transaction holding a single lock on a key; as such, there could either be a
lock on a key or a reservation on it. This invariant no longer holds; a held
Shared lock can be reserved by requests from other transactions that are trying
to lock the same key with a compatible locking strength.

As there can be multiple shared lock holders on a key, it follows that there can
be multiple reservations on a lock as well. We previously did not support such
joint reservations, which will have to change.

There can also be one or more lock holders and one or more reservations on the
same key at the same time, all with a shared locking strength. This can occur
when multiple shared lock requests proceed concurrently after an exclusive lock
is released. It can also occur when a shared lock request encounters an existing
shared lock on the same key.

##### Reservation Breaking

Every request that scans the lock table is assigned a sequence number to it.
Sequence numbers correspond to request age; a lower sequence number indicates an
older request. Sequence numbers are used to prevent range-local deadlocks in our
lock table implementation. This is required because locks in the lock table are
not guaranteed to be reserved in any specific order, which is susceptible to
deadlocks. To prevent this, locks reserved by requests with higher sequence
numbers can have their reservation “broken” by requests with lower sequence
numbers.

Following from this idea, we have another invariant in the lock table – a
reservation must have a sequence number lower than that of any other locking
waiters. With joint reservations, we will extend this idea to cover all
reservation holders. This means that if an incompatible locking request comes
along and finds a reservation, it may have to partially break it – all
reservation holders with sequence number greater than the incoming request must
relinquish their reservation and become waiters. In doing so, they must wait
behind the incoming locking request, as it has a lower sequence number.

##### Queuing vs. Joint Reservations

Consider the case where a lock is reserved by a request and a concurrent locking
request (from a different transaction) sees this. It needs to decide whether to
acquire a joint reservation and continue its scan, or to join the queue of
waiting locking requests. Joint reservations can only be acquired if the
incoming request is compatible with the reservation. This necessitates tracking
lock strength information when reserving a lock.

However, an incoming request cannot blindly acquire a (possibly joint)
reservation even if it is compatible with the pre-existing locks/reservations on
a key. To determine whether it needs to queue or whether it can safely acquire a
reservation, the request must check the list of waiting locking requests. If the
list is empty, or if the lowest sequence number from the list of waiters is
higher than the request’s sequence number, it can acquire a reservation.
Otherwise, it needs to wait to prevent the same deadlock scenario that motivated
partial reservation breaking.

#### Lock Wait Queue Representation

The discussion from the preceding sections motivates the following changes and
invariants to the per-key lock wait queue structure.

```go
type lockWaitQueue struct {
   // Ordering: increasing seq numbers.
   // Invariant: maxSeqNum(reservations) < waitingLockingRequests[0]
   // Invariant: lock strengths of all lockTableGuards in here should be 
  //             compatible.    
   reservations list<lockTableGuard>
   
   // Ordering: increasing order of seq numbers.
   waitingLockingRequestsAndNonTransactionalWrites list<queuedGuard> 
  
   // Invariant: empty when all locks on the associated key are held with           
   //strength < Exclusive.
   waitingNonLockingRequests list<lockTableGuard>
} 

type lockTableGuard struct {
  // Used to determine compatibility between reservation/lock holder and incoming 
  //requests. 
  strength lock.Strength 
  
  // Proxy for request age; lower sequence number == older request.
  seqNum uint64
}
```

### Releasing SHARED locks

We’ll reuse resolve intent requests to release shared locks. Despite the name,
resolve intent requests are agnostic to the type of lock being resolved. We
already use them for Exclusive locks.

Resolve intent request acquire write latches at the transaction’s minimum commit
timestamp. Without changing this behavior, concurrent (possibly locking) read
requests with lower timestamps would be allowed to run. This shouldn’t present a
hazard.

The latching behavior of resolve intent requests also means that two different
requests (transactions) would not be able to release SHARED locks concurrently.
This shouldn’t be impactful while shared locks are only held in memory; however,
there’s a concurrent initiative to [replicate locks other than
intents](https://github.com/cockroachdb/cockroach/issues/100193). The lack of
concurrency when resolving durable shared locks on the same key will hurt; we
may  want to revisit latching, in the context of releasing shared locks, as we
design that.

## Drawbacks

None.

# Unresolved questions

## Should we implement SELECT FOR {NO KEY UPDATE, KEY SHARE}?

SELECT FOR NO KEY UPDATE and SELECT FOR KEY SHARE are two variants of SELECT FOR
UPDATE with special locking conflict rules. These variants are described in
https://github.com/cockroachdb/cockroach/issues/52420.

For now, we do not plan to add support for these locking strengths. Instead, we
will remain focused on SELECT FOR SHARE, because it is all that is needed for
weaker isolation levels. However, the code structure changes necessary to
support SHARED locks will lay the groundwork for additional locking strengths.
Namely, conflict resolution will no longer assume that all locks conflict and
will instead be taught about a table-driven locking conflict policy that leaves
room for compatible locks which can be held concurrently on the same row.

# Appendix

## PG Barging behavior

Consider a construction where a SHARED lock is held on a key by a transaction
and a concurrent request attempts to acquire a non-compatible lock (eg.
EXCLUSIVE). Given the concurrent request is not compatible with the SHARED lock,
it will wait. In Postgres, if another concurrent request comes along and tries
to acquire a SHARED lock, it will be able to do so successfully. This barging
behavior of the concurrent SHARED lock request has the potential to starve out
the waiting EXCLUSIVE lock requester if we imagine a constant stream of SHARED
lock requesters.

We'll deviate from this behavior. If we find that there's a queue formed on a
lock, i.e., not all lock requests could be admitted, a newer incoming request 
will block instead of going through. This is required to prevent range-local
deadlocks in our lock table implementation -- see the section about reservations
and queueing vs. joint reservations. Doing so also prevents starvation.

It is also not clear whether there is much benefit to matching PG here, or if
the PG behaviour is [intentional to begin
with.](https://postgrespro.com/list/id/CANxv=LFB9v10Ftt=yD7-RBSOYzCO8_s8VThf39KgF1ZDafYUCA@mail.gmail.com)
