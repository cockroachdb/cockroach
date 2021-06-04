- Feature Name: Distributed token bucket for tenant cost control
- Status: draft/in-progress/completed/rejected/obsolete/postponed
- Start Date: 2021-06-04
- Authors: Radu Berinde, Andy Kimball
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

#### What is being proposed

Design of a subsystem relevant in the multi-tenant setting ("serverless") which
rate limits tenant KV operations in conformance to a budget target.

#### Why (short reason)

Serverless users will pay per usage and need to be able to set an upper bound on
how much they are willing to pay in a billing cycle. Free tier users must be
rate limited in order to curtail resource usage.

#### How (short plan)

To encompass the various resources used, we have created an abstract unit called
a "request unit". There will be a direct conversion from RUs to a dollar amount
that is part of a tenant's bill. Request units are determined based on CPU usage
in the SQL pods and on the KV requests from the tenant. For more details, see
the "Managing Serverless Resource Usage" RFC
([internal-only](https://github.com/cockroachlabs/managed-service/blob/master/docs/RFCS/20210603_serverless_usage.md)).

Each tenant will have a number of RUs available to use as needed, plus a
"baseline" rate of replenishment. These map directly to the concept of a
[token bucket](https://en.wikipedia.org/wiki/Token_bucket).
The values are determined by a higher-level billing algorithm that is beyond the
scope of this RFC and need to be configurable on-the-fly.

The enforcement of RU consumption will happen inside the SQL pods, at the point
where KV requests are sent. The SQL pods communicate with centralized state (for
that tenant) which lives in a system table inside the system tenant. The
centralized state also tracks and reports the consumption of RUs.

#### Impact

The new subsystem will only be enabled on our multi-tenant clusters. The
subsystem involves overhead related to communicating with and updating the
centralized state for each tenant.

Note that admission control for preventing overload in the host cluster or for
reducing the variability in user-visible performance (depending on other load on
the host cluster) are not in the scope of this RFC.

# Motivation

The higher-level motivation is covered by the "Managing Serverless Resource
Usage" RFC ([internal-only](https://github.com/cockroachlabs/managed-service/blob/master/docs/RFCS/20210603_serverless_usage.md)).
This RFC covers the implementation details of a subsystem necessary to enforce
resource usage.

# Technical design

## Overview

We implement a "global" (distributed) token bucket for each tenant; the metadata
for all these buckets lives in a system table on the system tenant. The global
bucket is configured with:
 - an initial amount of "burst" tokens (RUs)
 - a refill rate (RUs/second)
 - a limit on how many unused tokens (RUs) we can accumulate as burst. Refill is
   essentially paused when the bucket has more tokens than the limit.

Each SQL pod implements a local token bucket and uses it for admission control
at the KV client level (i.e. before sending out KV requests to the host
cluster). Each KV request consumes tokens and might need to wait until there are
enough tokens available. CPU usage on the pod also consumes tokens, but there is
no mechanism to limit CPU usage; we account for it after-the-fact, by
periodically (e.g. every second) retrieving the recent CPU usage and removing
tokens from the bucket. This can put the bucket in debt, which will need to be
paid before we can issue more KV requests.

The local bucket is periodically filled with tokens from the global bucket, or
assigned a fraction of the global fill rate. This happens via API calls through
the KV connector (see `kvtenant.Connector`). Each call is implemented as a
transaction involving the system table.

The global bucket keeps track of cumulative RU usage, which is reported (on a
per-tenant basis) to Prometheus. All nodes on the host cluster expose per-tenant
RU usage metrics which reflect the total usage since beginning of time.
Whichever node services a global bucket request (through the KV connector)
updates its instance of the metrics for that tenant. The current usage is the
maximum value across all host cluster nodes.

## Distributed bucket algorithm

We describe the algorithm in terms of "nodes" (a tenant's SQL pods)
communicating with a centralized ("global") token bucket. The tokens are RUs.
Note that we describe a single instance of the algorithm; in reality there is a
separate instance for each tenant.

Each node requests tokens from the global bucket as needed:

 - when the node starts up, it immediately requests a small initial amount. The
   node doesn't need to block on this request, it could start using tokens (up
   to this small initial amount) right away.

 - when the node is getting close to running out of tokens (e.g. 1 second at the
   current usage rate). The amount of tokens that the node requests is
   calculated (based on the recent usage) so that it lasts for a target time
   period (e.g. 10 seconds). This *target request period* is the main
   configuration knob for the algorithm, reflecting the trade-off between a) how
   frequently each node makes requests to the global bucket and b) how fast we
   can respond to changing workload distribution among multiple nodes.

The global bucket behavior corresponds to the desired functionality: we want to
allow customers to freely use resources without limitation up to a given
fraction of their budget ("burst tokens"), after which we want to rate-limit
resource consumption to a given rate ("refill rate"). So the global bucket
operates in two modes:
 - if there are sufficient "burst" tokens already in the bucket, the tokens are
   granted for immediate use;
 - otherwise, we grant the tokens over a specified period of time, according to
   a fraction of the global refill rate. Fewer tokens than requested can be
   granted so that the period does not exceed the target request period.

Once a request is completed, the node adjusts its local token bucket
accordingly, either adding burst tokens or setting up a refill rate.

The global refill rate is distributed among the nodes using a shares-based
system. Each node calculates a share value and the global bucket maintains the
sum of the current share values. The fraction of rate granted to a node is the
ratio between that node's shares and the total shares. The shares reflect the
load at each node (more details in the shares calculation section).

Note that the global bucket can transition between the two modes arbitrarily;
even if it ran out of burst tokens, it can accumulate tokens enough to where it
can start granting them for immediate use again. The global bucket state does
not affect the shares tracking sub-system; we always update and keep track of
shares even if we are not currently using them.

### Global bucket state

At any point in time, the global bucket maintains these values:
 - the current amount of tokens (RUs); this value can be negative. It is
   immediately adjusted when tokens are granted to the nodes (even when they are
   granted over a time period).
 - the current sum of shares
 - the total amount of tokens used (since the tenant was created); this value
   reflects RUs that were used with certainty. In abnormal cases (like node
   crashes), the error is always on the side of under-counting.

### Request input and output values

We describe the input and output values of a request to the global bucket.

```go
type TenantConsumption struct {
  RU float64
  ReadRequests uint64
  ReadBytes uint64
  WriteRequests uint64
  WriteBytes uint64
}

type TokenBucketRequest struct {
  // Previous shares value for the node, adjusted to account for shares decay
  // (see the Shares Decay section).
  // 0 if the node just started up.
  PreviousShares float64

  // New shares value for the node.
  NewShares float64

  // Amount of requested tokens.
  Tokens float64

  // TargetRequestPeriod configuration knob.
  TargetRequestPeriod float64

  // Consumption that occured since this node's last request.
  ConsumptionSinceLastRequest TenantConsumption
}
```


```go
type TokenBucketResponse struct {
  // Timestamp (on the host cluser) at which the bucket update became effective.
  // Used to account for the shares decay when calculating PreviousShares for
  // the next request.
  OperationTimestamp time.Time

  // If zero, the granted tokens can be used immediately, without restriction.
  // If set, the granted tokens become available at a constant rate over this
  // time period. E.g. if we are granted 1000 tokens and the trickle duration is
  // 10 seconds, the tokens become available at a rate of 100tokens/sec for the
  // next 10 seconds.
  // TrickleTime is at most the TargetRequestPeriod.
  TrickleTime time.Duration

  // Amount of tokens granted. In most cases, this is equal to the requested
  // tokens; the only exception is when TrickleTime would otherwise
  // exceed TargetRequestPeriod, in which case the number of tokens are reduced.
  GrantedTokens float64

  // Maximum number of unused tokens that we can accumulate over the trickle
  // time; once we reach the limit we have to start discarding tokens. Used to
  // limit burst usage for the free tier. Only set if TrickleTime is set.
  MaxBurstTokens float64
}
```

### Shares calculation

The shares have two terms:
 - load estimation. This is an exponentially weighted moving average of the load
   at the node, in RUs requested per second.
 - backlog term. This term ensures that nodes which have accumulated a longer
   backlog will "catch up" to the other nodes. It is a sum to which each queued
   operation contributes its RUs, exponentially scaled with the age of the
   operation. The idea is that the older operations become dominant in the
   calculations of the shares. Without this age-dependent scaling, differences
   in the top-of-the line can persist between nodes that receive workloads at
   different rates.
   
   For example, let's say node 1 has 10s worth of backlog of a constant 100
   RUs/s workload and node 2 has 5s worth of backlog of a constant 200RU/s.  The
   behavior of an ideal token bucket would actually be to block the second node
   altogether for 5s and give the first node everything until it catches up. If
   we just sum up the RUs, the two nodes get the same share and the
   head-of-queue discrepancy can persist for a long time. The exponential is a
   compromise between the two where the more a node is behind other nodes, the
   more of a share it gets.

Note that there can be periods where shares consistently grow (or shrink) for a
period of time, e.g. when we keep accumulating backlog. Whenever we
calculate a refill rate for a node, we are using somewhat stale values for the
shares of the other nodes. This can lead to systematic errors where we give out
too much or too little rate in aggregation. When we give out too little, the
bucket will automatically compensate by accumulating tokens and using them as
"burst". When we give out too much, the bucket will accumulate debt.

### Debt handling

The local buckets can be "in debt", due to operations that can only be accounted
for after the fact (e.g. size of reads, CPU usage). The debt is accounted for in
the number of tokens that a node requests from the global bucket. When a local
bucket is in debt, KV operations are blocked until the debt is paid. This
includes reads which have a size-independent cost component that is requested
up-front. An improvement here is to allow the debt to be paid over a time
period (e.g. 1 second) from the refill rate, to avoid "stop" and "start"
behavior (especially w.r.t periodic collection of CPU usage).

The global bucket can also accumulate debt (as explained in the previous
section). To correct for this, debt causes a reduction in the effective rate
(which is portioned to the nodes according to the shares) so that the debt would
be paid out over the next target request period.

Note that because we are pre-distributing tokens over the target request period,
it is expected for the global bucket to be in debt of up to
`<refill rate> * <target request period>`. For the purpose of reducing the
effective rate, we only treat anything above this threshold as systematic debt.

#### Shares decay

In clean shutdown situations, a node will make a final request to the bucket to
report the latest usage and to set its share term to 0. However, in unclean
shutdown situations, the share term would linger on, reducing the effective rate
to the other nodes. To address this problem, the share terms decay
exponentially with time, at a rate chosen such that the decrease is small over
one target request period but becomes significant after enough time has passed.

To correctly account for the delay when adjusting the shares sum, each request
passes the previous shares value along with the timestamp at which that share
value was incorporated into the sum. That allows adjusting the value according
to the decay that it was subjected to.


### Knobs

We list the configuration parameters for the algorithm, in decreasing order of
importance:
 - Target request period. This directly impacts how fast our rate portioning
   responds to changes in load, an in general how closely we approximate an
   ideal token bucket. It also directly affects the number of global bucket
   operations. Default value: between `10s` and `30s`.
 - Exponentially-weight moving average factor for per-node load average
   (assuming we average once per second).  Default value: `0.5`.
 - Backlog time scale. This value controls the exponent in the backlog term in
   the shares calculation: each work item contribution is scaled by
   `e^(<age>/<backlog_time_scale>)`. Default value: `10s`.
 - Backlog factor. This scales the backlog term, controlling the relative
   importance of the load estimation term and the backlog term. Default value:
   `0.01`.
 - Initial amount. This is the initial amount of tokens that a node requests
   when it starts up. The node can start using these right away and does not
   wait for the bucket request to complete.
 - Min / Max refill amount. These are just safeguards to keep sane behavior in
   potential corner-cases.

In the proposed system, all these configuration knobs "live" on the tenant side.

### Prototype

We created a simulation and visualization framework in order to prototype this
algorithm.  The input is a workload, which is a set of requested RU/s graphs
(one per node). The output is a set of granted RU/s graphs. The simulation
implements this algorithm and compares against an ideal distributed token
bucket.

The simulation can be accessed at:
[https://raduberinde.github.io/distbucket-v21-06-14/?workload=steps](https://raduberinde.github.io/distbucket-v21-06-14/?workload=steps).

The page shows the granted workload over time for both the prototyped algorithm
and an ideal (instantly synchronized) token bucket. We also show the graphs of
total consumption between the two, to see if there is any systematic (long-term)
divergence.

The workload can be changed from the dropdown or the yaml can be manually
edited. Each workload function is a sum of terms; the supported terms can be
inferred from the
[code](https://github.com/RaduBerinde/raduberinde.github.io/blob/master/distbucket-v21-06-14/lib/data.go#L77).

The code is available here:
[https://github.com/RaduBerinde/raduberinde.github.io/blob/master/distbucket-v21-06-14/lib/dist_token_bucket_3.go](https://github.com/RaduBerinde/raduberinde.github.io/blob/master/distbucket-v21-06-14/lib/dist_token_bucket_3.go).

## System table

The system table uses a "ledger" approach; each change to the state of a global
tenant bucket appends a row to the table. Each row has a unique operation UUID
and a sequence number which is unique within that tenant. The operation UUID
allows detection of duplicate requests, to ensure idempotency (in case a request
needs to be retried). The sequence numbers allow locating the latest entry for
each tenant.

Schema for the system table:
```sql
CREATE TABLE system.tenant_usage (
  tenant_id INT NOT NULL,

  -- Sequence number. Each update to the bucket state has a new sequence number
  -- and there are no gaps.
  seq INT NOT NULL,

  -- Operation ID, to detect duplicate (retried) requests.
  op_id UUID NOT NULL,

  -- Bucket configuration.
  ru_burst_limit FLOAT NOT NULL,
  ru_refill_rate FLOAT NOT NULL,

  -- Current amount of RUs in the bucket.
  ru_current FLOAT NOT NULL,

  -- Current sum of the shares values for all nodes.
  current_share_sum FLOAT NOT NULL,

  -- Cumulative usage statistics.
  total_ru_usage FLOAT NOT NULL,
  total_read_requests INT NOT NULL,
  total_read_bytes INT NOT NULL,
  total_write_requests INT NOT NULL,
  total_write_bytes INT NOT NULL,
  total_sql_pod_cpu_seconds FLOAT NOT NULL, -- TODO: Maybe milliseconds and INT8?

  PRIMARY KEY (tenant_id, seq DESC),
  UNIQUE (tenant_id, op_id) -- could also be UNIQUE(op_id)
);
```

An operation to the bucket is implemented with a transaction:
```sql
-- Generate random op_id.
BEGIN;
-- Get the latest state (with the largest sequence number).
SELECT FOR UPDATE * FROM tenant_usage WHERE tenant_id=.. ORDER BY seq DESC LIMIT 1;
-- Calculate new state.
-- Set the new state, using the next sequence number.
INSERT INTO tenant_usage (tenant_id, seq, ...) (tenant_id, seq+1, ...);
END;
```

Any failed request is retried with the same `op_id`. If the previous request
went through, the transaction will error out with a conflict on `op_id`,
allowing special handling of this case.

We perform periodic garbage collection to keep the table from growing. We aim to
keep only the latest N entries (e.g. N = 1000). Whenever our sequence number is
divisible by N, we trigger an asynchronous garbage collection operation with, which
runs this statemenet:

```sql
DELETE FROM tenant_usage WHERE tenant_id=.. AND seq > curr_seq - N AND seq <= curr_seq - 2*N;
```

The "rows affected" value is consulted; it should normally be `N-1`, because
`curr_seq - 2*N` should have been previously cleaned up. If it is `N`, then the
table has more rows than expected (e.g. last cleanup failed). In this case, we
repeat the same process with `seq > curr_seq - 3*N AND seq <= curr_seq - 2*N`.

This described process avoids having to scan through the tombstones of all
previously deleted rows.

> TODO: it's unfortunate that we will have to talk to two ranges on each
> operation. Could we achieve this with a single index? We could reserve part of
> the UUID space for the sequence number, allowing one index to serve a
> dual-purpose, but things would become messy.

> Another possible variation would be to have a single `(tenant_id, op_id)`
> primary index and *also* store the current state and sequence number in
> `(tenant_id, '00000000-0000-0000-0000-000000000000')`. That would not require a
> secondary index, but would generate larger KVs as we store all fields twice.

## KV Connector API

The KV Connector API is used by the SQL pods to make requests to the distributed
bucket. This API is implemented on the host cluster side by performing a
transaction on `tenant_usage`. The request/response structs are described above.
The UUID argument identifies the each request, allowing to retry calls that may
have gone through on the host cluster side without losing tokens or
double-counting consumption.

```go
type TenantTokenBucketProvider interface {
   Request(
     ctx context.Context,
     uuid util.UUID,
     req TokenBucketRequest,
   ) (TokenBucketResponse, error)
```

## Configuration API

The limits for a tenant are periodically reconfigured by a higher-level usage
controller running in our CockroachCloud infrastructure, as outlined in the
"Managing Serverless Resource Usage" RFC
([internal-only](https://github.com/cockroachlabs/managed-service/blob/master/docs/RFCS/20210603_serverless_usage.md#usage-controller-cc-4116)).

The proposed configuration interface is via an internal SQL function; we can
easily implement other endpoints on the system tenant as necessary.

```sql
SELECT crdb_internal.update_tenant_resource_limits(
  tenant_id,                    -- INT
  operation_uuid,               -- UUID
  available_request_units,      -- FLOAT
  refill_rate,                  -- FLOAT
  max_burst_request_units       -- FLOAT
  as_of,                        -- TIMESTAMP
  as_of_consumed_request_units, -- FLOAT
)
```

The statement completes after a new row is inserted in the system table.

The configuration parameters are `available_request_units`, `refill_rate`,
`max_burst_request_units`.

The rest of the parameters exist for technical purposes:

 - `operation_uuid` allows us to guarantee idempotency of the operation, if a
   reconfiguration request is retried. This is not a strong requirement (the
   consequences of repeating a reconfiguration requests are small) but our
   schema makes it easy to provide.

 - `as_of` is the time at which the consumption state was consulted by the usage
   controller and `as_of_consumed_request_units` is the total consumption value
   at that time.  The `as_of` timestamp originates from the host cluster (it
   corresponds to the time when that total consumption value was emitted as a
   statistic). These parameters allow us to accurately deal with the reality
   that the usage controller's operation is not instant. Specifically:
    * we adjust the available request units by the delta between
      the current consumption value and `as_of_consumed_request_units`
    * we account for the refill that would have happened in the time since
      `as_of`.

This API should be initially called as part of creation of a new tenant (with
`as_of_consumed_request_units = 0`). The system can start with some reasonable
defaults (e.g. free tier settings) until that happens.

In the initial implementation, this function will be called separately for each
tenant. We will investigate batching opportunities at a later point.

## Resilience considerations

The system must have reasonable behavior if the bucket range becomes temporarily
inaccessible. To achieve this, in the short term each node continues operating
at the previous rate of consumption. Longer term, the rate can decay over time
toward 10% of the previous rate of consumption. If/when a pod can be aware of
how many total pods are in the tenant cluster, the rate can decay to 1/N of the
total refill rate (where N is the number of SQL pods).

## Performance considerations

The system table could get a high amount of aggregate traffic. If we have 5,000
SQL pods across all tenants, we expect 500 operations per second (for a 10s
target request period).

Load-based splitting of ranges would reduce the range sizes; we could also force
split points between the tenants.

The minimum refill amount can be tuned to reduce the frequency of bucket
operations during periods of very low activity on a single node. Note that the
share of the node will decay in time but this should not be a problem as the
share should effectively be zero anyway.

## Drawbacks

- Complexity

- Periodic transactions on the host cluster

## Rationale and Alternatives

We have explored a number of existing systems for cloud rate limiting. Most of
these use a gossip-style algorithm to disseminate relative usage information.
This is not necessary in our system which provides consistent access to shared
state.

An alternative to using transactions as the only centralization point would be
to elect one "leader" pod which implements the distributed bucket logic and
which all other pods communicate with (perhaps even in a streaming fashion).
This SQL pod would update the persistent state (at a reduced frequency), using
the same mechanism (KV connector). The difficulty with this approach is around
implementing election reliably; this could be done by keeping track of a "lease"
in the bucket state (in the system table). There will likely be other usecases
for having a pod leader; if that happens we can implement this idea as an
improvement on top of the proposed system (most parts would not need to change).

# Unresolved questions
