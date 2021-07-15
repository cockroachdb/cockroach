- Feature Name: Quality of Service & Admission Control
- Status: draft
- Start Date: 2019-07-08
- Authors: Andrew Werner
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: #38076

# Table of Contents

  * [Table of Contents](#table-of-contents)
  * [Summary](#summary)
  * [Motivation](#motivation)
    * [Constrained Resources](#constrained-resources)
    * [Goals](#goals)
  * [Guide-level explanation](#guide-level-explanation)
    * [Quality of service level overview](#quality-of-service-level-overview)
    * [Admission Controller Overview](#admission-controller-overview)
    * [Hierarchy of admission controllers](#hierarchy-of-admission-controllers)
  * [Reference-level explanation](#reference-level-explanation)
    * [Quality of Service (`qos.Level`)](#quality-of-service-qoslevel)
    * [Admission Control (`admission.Controller`)](#admission-control-admissioncontroller)
    * [Admission Controllers](#admission-controllers)
      * [Storage Read Controller](#storage-read-controller)
      * [Higher Level Admission Controllers](#higher-level-admission-controllers)
    * [Visibility](#visibility)
    * [Drawbacks](#drawbacks)
  * [Rationale and Alternatives](#rationale-and-alternatives)
  * [Unresolved Questions and Further work](#unresolved-questions-and-further-work)

# Summary

This RFC addresses issues related to overload, a state where a cluster
experiences more load than it has capacity to handle. This document proposes
mechanisms to detect and warn operators of overload as well as a quality of
service definition for CockroachDB and an associated admission control
subsystem to coordinate best-effort resource prioritization. The goal of the 
combined system is to protect critical system functions and degrade gracefully
in the face of overload to give operators the opportunity to scale out their 
cluster or remove load.

This project has both taken longer and up to now has been less successful than
had been hoped. There are glimpses of positivity but more work needs to be done
to justify the complexity. The immediate goal of mitigating the node liveness
outages as motivated by 
[#35535](https://github.com/cockroachdb/cockroach/issue/35535) 
has been partially achieved through the use of a separate gRPC connection. The
work presented here represents a possible framework which may be used to
manage overload and the context in which it was developed. Unlike many other
RFCs, this document is being posted with many questions as a mechanism to
communicate about ongoing work. 

# Motivation

In order to build trust in our customers CockroachDB needs to be reliable.
Databases can experience workloads which demand more resources than are
available. Increases in workload can happen for a variety of reasons such as
but not limited to viral growth, DDoS attacks, new product launches, software
bugs, schema changes, analytics queries, database import / restore.
Cockroach experiences several particularly painful failure modes for customers
due to overload

 - Node liveness failures
 - Unresponsive sql shell
 - Errors and stale data in Admin UI
 - OOM/Crashes
 - Variance of throughput
 - Increased latency which can lead to request timeouts and thus increased
  failure rates
 - Increased failure rates due to failure to run distributed SQL flows

## Constrained Resources

This document would be lacking if it didn't outline the different types of
resources which may constrain a cockroach cluster and the ways in which
insufficient resources cause problems. Resource exhaustion is the fundamental
cause of overload. Resource management in CRDB is made more complex by the
system's shared-process architecture and use of the go runtime. Often
constrained resources are multiplexed and accounted for using operating system
primitives. Systems which control memory allocation and scheduling can achieve
fine grained resource management within a single process but the go
implementation precludes this level of control. Given these challenges this
document proposes a system which responds to heuristics indicating resource 
exhaustion rather than one which intends to predict or prevent that exhaustion.

### CPU
  
Insufficient compute resources lead to increased processing time. This
exhaustion can lead to dramatically increased times to perform operations which
ought to happen very quickly. CPU overload is made more complex in the context
of CRDB due its the shared-process architecture inside of the go runtime. Go
provides very limited visibility the state of the run queue. Runnable goroutines
which greatly outnumber the number of physical cores can lead to long scheduling
delays. Experience shows that the problem can be exacerbated when goroutines
need to synchronize on a shared resource behind a queue. The head of the queue
may be scheduling in a reasonable amount of time (~10s of ms) but it may take a
long time to unblock work further into the queue even if each queue element only
has a small amount of work to perform.

### IO

Durably storing data is vital to the operation of CockroachDB. Foreground
data replication and command application must contend with background
compactions, read traffic, backups and restoration as well as disk spilling.
IO devices can be overwhelmed in two major ways: disk bandwidth and IO
operations. When IO resources are overload latency can increase dramatically
and in some settings can stall altogether. IO saturation is generally rare
outside of restore scenarios but can be catastrophic.

### Network

Network saturation can be leading to very high latency sending data on the 
wire. Fortunately today's modern network architectures 10GB
ethernet and full bisection bandwidth between nodes in a region are common. This
data rate generally outstrips the aggregate so in general cloud deployments 
I'd expect saturation should be rare. Some on-prem deployments might run on old
1GB or dual 1GB connections and may be more exposed to network saturation.

### Memory

Memory has the most dramatic failure mode. Insufficient memory leads to fatal 
process failure. Unfortunately it is the most singularly difficult to address 
with a system of dynamic back pressure. Properly preventing overcommitment of
memory requires accounting and budgeting. CockroachDB has a mechanism to track
and allocate memory associated with query execution but the system is inexact,
incomplete, and error prone. The memory monitoring infrastructure remains unused
in the core layers of the system. Memory allocated at these layers may be 
retained and unaccounted for at higher levels. The author hopes that the new
vectorized execution engine will lead to more exact accounting of memory for
data read from storage.

## Goals

The admission control system exists to ensure that Cockroach clusters
gracefully degrade in the face of overload such that they remain basically
available and operable. 

1) Servers stay available and responsive to administrative commands regardless
   of client load.
1) Admission control decisions are observable, understandable, and explainable.
1) System performance degrades gracefully in the face of overload.
   - Requests don't get stuck in the system for "too long" (client defined)
   - Rejection rates increase rather than latencies when the system cannot
     process incoming load (if clients set timeouts).
1) Traffic is dropped in a predictable and controllable way.
1) System remains robust in the face of bursts (i.e. not brittle or
   over-responsive).
   - System doesn't reject requests too quickly in the face of increased load
     which ultimately it can handle
   - Rejected requests are painful for users and should actually relate to
     an overload condition. Traffic shape over short timescales should not
     lead to rejection, such a system would be over-reactive.
   - System provides control over the trade off between latency and throughput.
1) Admission control subsystem does not adversely affect latency in normal
   conditions.

These goals need to be contextualized. The primary goal is most often violated
due to node liveness failures and by OOM crashes. The former
has been somewhat addressed by network isolation 
(see [#39172](https://github.com/cockroachdb/cockroach/pull/39172)). This RFC 
realistically pushes much further into keeping a responsive admin UI and 
increasing DBA control in the face of overload. It has become somewhat less 
focused on effective node survival and more focused on issues of graceful 
degradation. This proposal should be read with a critical eye as it
introduces complexity and risks in excess of more targeted mitigation due to its
reliance on heuristics and feedback.

### Non-goals

1) Admission control for mutations.
    - Once a transaction has laid down an intent, we shall not touch it.
    - Queries which start by writing a record then proceed to do a bunch of
      expensive work are (hopefully) not a common case.
    - Dealing with writing transactions would increase the complexity
      of this project as it would necessitate interacting with contention
      mechanisms and could introduce deadlocks. 
1) Transport level quality of service.
    - #39172 introduced a mechanism to use a separate gRPC connection for 
      system traffic. This change mitigates some causes of failure due to 
1) Strict resource isolation or accounting.
    - Improving resource usage and modeling throughput and cost will be a
      critical and important part of admission control and its evaluation
      but get accounting level resource usage on a per-statement basis is
      well beyond the scope of this project.
    - Isolation seems very difficult given the CockroachDB architecture.
    - Vectorized execution should help provide a more sane foundation for 
      execution memory accounting. More work must be done to properly account
      for memory use within the core layers of the system.

# Guide-level explanation

When a system experiences overload, it either must accept increases in latency
or choose to delay or reject processing until resources become
available. Intuitively, if a system receives requests in excess of its
capacity then work will accumulate inside the system. In the extreme just this
buildup alone might lead to resource exhaustion, but before that point, the
buildup will necessitate an increase in client-observed latency due 
to queueing. If, in the face of overload, uniform increases in latency are
unacceptable then some requests will have to suffer. While latency increases
might seem generally acceptable and failed queries might seem unacceptable, 
many services expect a certain level of service and operate with request 
timeouts; in cases where statements carry timeouts, extreme delay
translates directly to failure. Subjecting some percent of queries timeout and
failure while providing low-latency service to the rest is often preferable to
providing high latency service to all requests. 

The admission control subsystem is tasked with the twin challenges of deciding 
when the system is overloaded and some requests are going to be stalled or 
rejected and then coordinating the realization of that decision. The quality 
of service level provides the means to differentiate requests when making 
admission control decisions providing a best-effort resource prioritization 
mechanism. The admission control subsystem seeks to detect and respond to 
overload at the lowest levels of the system where its impacts are most 
apparent and to propagate a response upwards to the cluster as the extent of 
overload outstrips the mitigation capacity of local node. This architecture
additionally enables some degree of localized overload to be handled locally
without affecting the rest of the cluster.

The key enabling technologies are the quality of service (qos) level and the
admission controller which combine throughout the database layers to form the
admission control system. Requests carry a qos level through all layers of
processing and a hierarchy of admission controllers decide whether a request 
shall be admitted, blocked, or rejected given the current state of that
controller. Admission controllers are arranged in a hierarchy which coordinate
to prune load and protect high-class and critical system functions from load
induced by lower-class user requests. Admission control decisions made in
low-level layers of the system then drive state changes in the admission
controllers of above layers.

This coordination seeks to protect high-class traffic from overload where it
occurs and works to propagate protection upwards when it cannot be sufficiently
mitigated locally. The design has a number of components directly inspired by
WeChat's [DAGOR] admission control system but departs in a number of meaningful
ways. That system is free to assume that clients will back off in
the face of server errors indicating they should, a luxury not available on
our pgwire client connection. Instead we are forced to use stalling of client
connections as a form of load shedding. Another challenge is the high degree
of variance in runtime and cost of individual queries relative to high-volume
HTTP APIs. Lastly those systems tend to be built using isolated microservices
which can directly correlate their resource usage and performance profile with
their admission rate. Nevertheless, that system and learnings from other
simpler overload management approaches like [CoDel] for IP routers and the
learnings from [FoundationDB load balancing talk] have provided valuable
jumping off points.

## Quality of service level overview

The first item for definition is a quality of service level which the system
uses to differentiate between requests. The design of the `qos.Level` (and the
entire system) was heavily inspired by WeChat's [DAGOR] paper which shares many
goals with this work. Details regarding how quality of service levels are
communicated and used in code is deferred until 
[later](#quality-of-service-level-implementation).

Fundamentally just like [DAGOR]'s priority level, the qos.Level is represented
as a tuple space of (class, shard) where class defines an intention of quality 
of service and shard subdivides classes in a useful way to be discussed below.
These terms can be thought of roughly identically to the (business priority, 
user priority) of the [DAGOR] vernacular.

### Quality of service level illustration

Let's look at an illustration of the quality of service level space where an
arrow which is used to represent the level. This visualization implies that 
there are 4 shards per class and three classes. In practice we may use
a different number of shards but the use of 4 here and in later illustrations
helps keeps the size manageable while providing enough room for to communicate
the concepts.

```
[ High    ][ Default ][ Low     ]
[ 3 2 1 0 ][ 3 2 1 0 ][ 3 2 1 0 ]
    ↑(High, 2)
```

Here we see that the arrow points to `(High, 2)`. Levels in this visualization
are ordered from right to left where the rightmost value is the lowest level 
and the leftmost value is the highest.

```
[ High    ][ Default ][ Low     ]
[ 3 2 1 0 ][ 3 2 1 0 ][ 3 2 1 0 ]
                 ↑(Default, 1)
```
```
[ High    ][ Default ][ Low     ]
[ 3 2 1 0 ][ 3 2 1 0 ][ 3 2 1 0 ]
                        ↑(Low, 3)
```

This second illustrations refers to `(Default, 2)` and the third refers to 
`(Low, 3)`.
In terms of ordering, these three values respect the following compound
inequality:
`(High, 2) > (Default, 1) > (Low, 3)`.

### Level assignment

A qos level controls how a request gets treated, but how is it assigned?
As explained above, the qos level has two portions, the class and the shard.
Each client connection has a number of properties such as user and database and
qos level is just another property of a connection. By default, client
connections are set to the `Default` class except for `root` users which carry
a `High` class level. Shards are assigned based uniformly based on a unique
property of the connection. 

Sharding is useful to help with long-running queries which might touch multiple
overloaded servers. In the face of client timeouts, spreading delay over many
queries increases the likelihood that more hit the timeout. This isn't
obviously true, but let's say that we expect being overloaded to be rare and
that the delay due to overload to be not insignificant on human timescales,
once a request has seen delaying due to overload, it's already going to be
"slow". It seems like a good idea to contain the slowness.

Maybe that's not that defensible. More defensible maybe is the idea that
the more requests we have hanging around the system for longer, the more likely
that their incomplete memory accounting will interact poorly. This too is
probably a weak argument.

Lastly, it's simple and it allows fine-grained decisions without having to 
implement efficient work-stealing queues. 

## Admission Controller Overview

The enabling technology is the `admission.Controller`. The admission controller
is a discrete-time, stateful data structure which decides how a request at a
qos level should be treated. The controller may choose one of three actions
for a request: admit, block, or reject. The admission controller maintains an
admission and rejection level which it uses to make its decision. Any request
at or above the current admission level is admitted. Any request at or below
the current rejection level is rejected. Requests which carry a level between
the admission and rejection levels are blocked until it should be either
admitted or rejected or is canceled. The admission level is driven by an
overload signal which can be configured independently for each instance.

The controller breaks time into discrete intervals which change during an
event termed a "tick". Intervals are defined by a duration or a number of
requests. When a tick occurs, the controller consults its overload signal to
determine what should happen to the admission level. The new level is set based
on a policy which combines the information from the overload signal with data
collected during processing to set a new admission level. If the admission
level falls, blocked requests which are now at or above the admission level
will be released.

```
Interval 2
[ High    ][ Default ][ Low     ]    ⇑: admission level
[ 3 2 1 0 ][ 3 2 1 0 ][ 3 2 1 0 ]    ⬆: rejection level
             ⇑              ⬆
 | Tick!                    
 | Not overloaded, admission level drops.
 V                          

Interval 3
[ High    ][ Default ][ Low     ]
[ 3 2 1 0 ][ 3 2 1 0 ][ 3 2 1 0 ]
                 ⇑          ⬆
```

The rejection mechanism determines when local mitigation is not sufficient to
cope with overload. The rejection level is set based on the volume of requests
which are blocked. Each admission controller is configured with a maximum
number of requests which it will block. Initially rejection is disabled. If a
new request comes in and should block but determines that it would exceed the
maximum, the rejection level rises, rejecting previously blocked requests until
space has been made for this new request. If, at the end of an interval, the
number of blocked requests is less than half of the total capacity rejection is
disabled (below the lowest level).

## Hierarchy of admission controllers

Admission controllers are arranged in a hierarchy such that the controller with
which a request first interacts is the slowest controller to respond to
overload. The below diagram is a simplified picture of how the layers
of the system interact. Each numbered admission controller responds to an 
overload signal due to the controller beneath it. The lowest-level controller
lives in the store and responds to an overload signal driven by observed
performance characteristics.

```
  ---------------        ____
 | Connection    |     /      \ (for distributed execution)
  ---------------     /        V
 | Parse & Plan  |   /   --------------------
  ---------------   /   | Flow Scheduler (3) |
 | Execution (4) |_/     --------------------
  --------------- \     | Running Flow       |
           _______/    / --------------------
         /     _______/
        |    /                 __
        V   V                 /  \
 ----------------------      /    V
 | Table Reader       |     /   -------------  
 ----------------------    /    | Store (1) |
 | DistSender (2)     |   /     -------------
 ----------------------  /      | Replica   |
 | sendPartialBatch() |_/       -------------
 ----------------------         | Engine    |
                                -------------
```

# Reference-level explanation

## Quality of Service (`qos.Level`)

The above explained quality of service level is represented in code as the
`qos.Level` which has two fields `Class` and `Shard` which are each a `uint8`.
This RFC does not take a stance on how many shards should be used but for
diagrams will define `NumShards` as 128. This section will explain the details
of serialization and propagation.

### Level assignment

An obvious question this RFC needs to answer is how is a qos level associated
with a statement.
There are a number of different approaches which were explored.

* SQL Hints
* Connection setting
* User roles
* Database name
* Application name
* Statement fingerprint

This RFC proposes uses user roles to determine the `qos.Class` for a given
connection. The root user will always carry a high Class.  The prototype as 
written uses a hard-coded application name to imply 
low class. Other mechanisms such as on-line classification of statement 
fingerprints are left for a later date.

### Serialization

The initial wire serialization uses four bytes as a hexadecimal string
encoding. This is not an optimal encoding but the two-byte overhead relative
to the optimal encoding seems not too costly. Given the small number of values 
of qos levels, the serializations are interned for allocation-free encoding.

Encoded values Class and Shard values are spread over the uint8 space in
the second and first low-order bytes respectively of the returned uint32.
A uint32 is returned despite the fact that 16 high-order bits will always be
zero because uint32 is the smallest value which can be encoded in protocol
buffers or used with atomics.

To demonstrate the way encoding works, consider the following example of
encoding the Class byte. If NumClasses == 3, Class values will map to an
encoded byte as follows:
```
  [0 (ClassLow), 1 (ClassDefault), 2 (ClassHigh)] -(encode)-> [0x01, 0x80, 0xFF]
```
Suppose a later version of CockroachDB were to support five Class values.
In that case the Class encodings will look like the following:
```
  [0, 1, 2, 3, 4] -(encode)-> [0x03, 0x42, 0x81, 0xC0, 0xFF]
```
If that newer version were then to communicate with an older binary which
only knows about the three classes then it will decode the above Classes as
follows:
```
  [0x03, 0x42, 0x81, 0xC0, 0xFF] -(decode)-> [0, 1, 1, 2, 2]
```
This encoding format allows binaries with different interpretations of Class
to interoperate while being able to rely on a uniform in-memory
representation of Level.

### `rpc` Middleware

It is critical that a `qos.Level` propagate through the entire distributed
execution of a statement. This propagation is somewhat complicated by
distributed nature of execution. If a query were to assume a different qos 
level in different layers of the system or on different machines, the sharding
mechanism would be rendered ineffective. In order to ease propagation, 
middleware is added to the `rpc.Context` that reads a `qos.Level` stored in a
`context.Context` with `qos.ContextWithLevel` and propagates it on the wire as
a gRPC metadata header. The server middleware then detects that header, decodes
it, and stores its decoded value in the `context.Context` on the receiver.

This leads to a less invasive and easier to use design than explicitly passing 
a level in every rpc message. The cost is that both the client and the server 
must allocate memory in order to interact with the metadata header. These 
allocations might be made less expensive with better gRPC APIs but as of 
writing such middleware incurs at least 2 allocations on both sides.

## Admission Control (`admission.Controller`)

The admission controller is a general purpose data structure intended to be
reused at several layers of the system. The `admission.Controller` maintains
state to determines the fate of a request of a given `qos.Level`. 
The primary interface to the `Controller` is `AdmitAt()`.

```go
// AdmitAt attempts to admit a request at the specified level and time.
// Admit at may return ErrRejected if the rejection level at any point rises to
// or above l. Calls may block on calls to AdmitAt if requested level is below
// the current AdmissionLevel. Context cancellations will unblock the caller 
// and propagate an error.
func (c *Controller) AdmitAt(context.Context, qos.Level, time.Time) error
```

The `Controller`'s behavior is defined by its configuration at construction
time. Every `TickInterval` a controller will query its `OverloadSignal` to 
determine how its `AdmissionLevel` ought to change for the next interval.
Calls to `AdmitAt` carry a timestamp and inform the `Controller` of the passage
of time. If the `OverloadSignal` reports that the system is overloaded. One 
design principle of this document is to separate measurement from
action. The intention is that loose coupling between monitoring and stateful
decision-making will lead to an architecture which lends itself to high 
visibility even as components and algorithms change. The overload signal's
abstract nature allows it to hook into monitoring and metrics infrastructure 
internally and ideally lead to concurrent innovation in that space.


```go
// OverloadSignal is queried by a Controller when an interval ends to determine
// how the admission level should change. If the Controller is overloaded it
// will not raise the AdmissionLevel above max. To keep the level the same 
// return (true, cur).
type OverloadSignal func(cur qos.Level) (overloaded bool, max qos.Level)
```

If the overload signal at the end of an interval reports that the system is
overloaded then the `Controller` will increase the `AdmissionLevel` in an 
attempt to decrease traffic in the next interval by `PruneRate`. Conversely, if
the system is not overloaded the level will decrease in order to increase 
traffic by `GrowRate`. In order to determine the appropriate next level the 
controller maintains history about the previous intervals. The history is 
maintained using simple histograms represented as arrays of integers which are 
interacted with atomically.

The number of intervals of history to keep is a constant currently set
to three. Determining an appropriate value for memory as well as tick interval,
prune rate, and grow rate is an open question. Generally the prune rate should
be larger than the grow rate leading to a classic saw tooth curve as the system
probes and backs off from the true level. 

```go
// Config configures a Controller.
type Config struct {

	// TickInterval is the maximum amount of time between ticks.
	// Ticks may also occur if the maximum number of requests per interval
	// is exceeded.
	TickInterval time.Duration

	// OverloadSignal is used during ticks to determine whether the admission
	// level should rise or fall.
	OverloadSignal OverloadSignal

	// MaxBlocked determines the maximum number of requests which may be blocked.
	// If a request would lead to more than this number of requests being blocked
	// the rejection level will increase.
	MaxBlocked int64

	// The PruneRate and GrowRate work by examining the traffic which occurred
	// in the previous interval scaled by the scale factor and then moving the
	// admission level up or down to match the new target.

	// PruneRate determines the rate at which traffic will be pruned when the
	// system is overloaded.
	PruneRate float64

	// PruneRate determines the rate at which traffic will add when the system
	// is not overloaded.
	GrowRate float64
}
```

For introspection, the object also offers access to its current `AdmissionLevel`
and `RejectionLevel`.

### Examples 

The below examples show a serialization of the state of an 
`admission.Controller`. The left-most column indicates the shard 
corresponding to the row. The remaining 3 columns correspond to the three 
classes. The numbers at or above the solid arrow show how many requests were 
admitted in the current memory at the indicated level. The numbers between the 
solid arrow and the unfilled arrow indicate how many requests are currently 
blocked at the indicated level. The numbers at or below the unfilled arrow 
indicates how many requests were rejected in the current memory. Contiguous 
rows with all  zero values are elided with `...`.

#### Simple example with cancellation

The controller begins at the lowest level.

```
---|  h   |  d   |  l   |
127|  0.00|  0.00|  0.00|
...
0  |  0.00|  0.00|➡ 0.00|
int: 0; ad: 0; bl: 0; rej: 0
```
Three requests come in and succeed in the current interval.
```
AdmitAt(d:127, 0.0) == <nil>.
AdmitAt(d:0, 0.0) == <nil>.
AdmitAt(d:1, 0.0) == <nil>.
```
They appear as admitted requests in the controller's state.
```
---|  h   |  d   |  l   |
127|  0.00|  1.00|  0.00|
...
1  |  0.00|  1.00|  0.00|
0  |  0.00|  1.00|➡ 0.00|
int: 1; ad: 3; bl: 0; rej: 0
```
The overload signal becomes true and time advances to 0.1.
The admission level rises to `d:1` and `AdmitAt(d:0, 0.1)` blocks
```
---|  h   |  d   |  l   |
127|  0.00|  0.00|  0.00|
...
1  |  0.00|➡ 0.00|  0.00|
0  |  0.00|  1.00|  0.00|
int: 2; ad: 0; bl: 1; rej: 0
```
Cancel the request at d:0 so it is no longer blocked.
Observe that the request is no longer blocked.
```
---|  h   |  d   |  l   |
127|  0.00|  0.00|  0.00|
...
1  |  0.00|➡ 0.00|  0.00|
0  |  0.00|   0.00|  0.00|
int: 2; ad: 0; bl: 0; rej: 0// 

```

#### Example with rejection

Admit 1000 requests uniformly between `d:1` and `d:10` at t1.
After adding all of the requests the controller state looks like this:

```
---|  h   |  d   |  l   |
127|  0.00|  0.00|  0.00|
...
10 |  0.00|  100 |  0.00|
9  |  0.00|  100 |  0.00|
8  |  0.00|  100 |  0.00|
7  |  0.00|  100 |  0.00|
6  |  0.00|  100 |  0.00|
5  |  0.00|  100 |  0.00|
4  |  0.00|  100 |  0.00|
3  |  0.00|  100 |  0.00|
2  |  0.00|  100 |  0.00|
1  |  0.00|  100 |  0.00|
0  |  0.00|  0.00|➡ 0.00|
int: 1; ad: 1000; bl: 0; rej: 0
```
Set the overload signal to true and Admit a request at t2, leading to 
a tick.
```
AdmitAt(d:127, t0) = <nil>.
```
Notice that the admission level has risen to `d:2`.
```
---|  h   |  d   |  l   |
127|  0.00|  1.00|  0.00|
...
2  |  0.00|➡ 0.00|  0.00|
...
0  |  0.00|  0.00|  0.00|
int: 2; ad: 1; bl: 0; rej: 0
```
Attempt to admit 1000 requests uniformly between `d:1` and `d:10` at t2.
901 should be admitted and 100 should be blocked.
```
---|  h   |  d   |  l   |
127|  0.00|  1.00|  0.00|
...
10 |  0.00|  100 |  0.00|
9  |  0.00|  100 |  0.00|
8  |  0.00|  100 |  0.00|
7  |  0.00|  100 |  0.00|
6  |  0.00|  100 |  0.00|
5  |  0.00|  100 |  0.00|
4  |  0.00|  100 |  0.00|
3  |  0.00|  100 |  0.00|
2  |  0.00|➡ 100 |  0.00|
1  |  0.00|  100 |  0.00|
0  |  0.00|  0.00|  0.00|
int: 2; ad: 901; bl: 100; rej: 0
```
Set the overload signal to false.
Admit a request at t3 leading to a tick moves the admission level down
to `d:1`.
```
---|  h   |  d   |  l   |
127|  0.00|  1.00|  0.00|
...
1  |  0.00|➡ 100 |  0.00|
0  |  0.00|  0.00|  0.00|
int: 3; ad: 101; bl: 0; rej: 0
```
Also in t3 add more requests than can fit in the waitQueue (1000).
This is done by adding 99 requests to `d:0` and 100 to each level from `l:10`
down to `l:1`.
This will lead to requests at l:1 being rejected.
```
---|  h   |  d   |  l   |
127|  0.00|  1.00|  0.00|
...
10 |  0.00|  0.00|  100 |
9  |  0.00|  0.00|  100 |
8  |  0.00|  0.00|  100 |
7  |  0.00|  0.00|  100 |
6  |  0.00|  0.00|  100 |
5  |  0.00|  0.00|  100 |
4  |  0.00|  0.00|  100 |
3  |  0.00|  0.00|  100 |
2  |  0.00|  0.00|  100 |
1  |  0.00|➡ 100 |⇨ 100 |
0  |  0.00|  99.0|  0.00|
int: 3; ad: 101; bl: 999; rej: 100
```

### Additional Considerations

In addition to a `TickInterval` the [DAGOR] paper indicated that their 
intervals additionally maintained a maximum number of requests. The admission
controller additionally carries a `MaxReqsPerInterval` configuration. When
supplied the `Controller` additionally requires access to a clock so that it 
can maintain the next tick time after ticking due to requests.

The `Controller` only has memory about the number of requests which have been
admitted/blocked/rejected from a given level; it doesn't know anything about
the sizes of those requests. To cope with cases where requests in different 
classes carry different weights the `Config` exposes a `ScaleFactor` function
which allows the client to provide a multiplier for requests by class. With 
this the prune and growth algorithm can weight history in different classes
differently.

## Admission Controllers

As introduced in the guide-level explanation, the admission control 
architecture presented here uses a hierarchy of admission controllers which 
maintain state on which `qos.Level`s will be admitted, blocked, or rejected.
Each controller is driven by signals from the levels beneath it except the 
bottommost level which is driven by a signal based on performance 
characteristics. This section will walk through each level from the bottom up. 
The levels are as follows:

* storage read controller
* kv DistSender
* distsql Flow Controller
* sql connExecutor

### Storage Read Controller

The bottommost level for admission control lives in the store on the read path.
The reason for selecting the read path as opposed to the write path is that
writes interact with the distributed transactional protocol. Interfering with
writing transactions risks introducing deadlocks and other unintended 
consequences on system behavior. This limitation may prove to be severe in some
setting but the working conjecture is that those will be rare. Another 
motivation for focusing on admission control at the read path is that the 
majority or expensive work in the system is proportional to the size of data
being processed and the majority of data which is processed is read from the
system. Certainly there are cases where large volumes of data may be generated
and not read and such cases indicate that there is value in controlling the rate
of processing at the level of execution. Execution level controls are left for
further work and are discussed below.

Given the justification for controlling the amount of data flowing out of 
storage as a mechanism to meter load throughout the system, let's explore the
specific design.

#### Quotas and Rate Limits

There are two main approaches to controlling the work rate in a system: quotas
and rate limits. Rate limits often make sense in domains where throughput is 
the quantity being measured. I'm honestly not clear on when to use rate limits 
over quotas. I suspect may be quotas may be difficult or expensive to build 
which attempting to fairly multiplex a resource over many users over time, 
especially when the quantity being allocated is a throughput. 
I also suspect that they come up frequently in the networking domain where
concurrency is zero and thus rate limits are sort of equivalent. 

From a queuing theory perspective quotas are a more powerful tool for reasoning
about the system as a whole. Quotas define the maximum number of work items
inside some portion of the system at a time. This enables a Little's law
analysis of the system in aggregate. Little's law says simply:

`L = N * W`

Where `L` is the number of work items currently in the system, `N` is the
arrival rate of new work items and `W` is the average time to process a work
item. It's important to note that all of these are averages. This use of 
averages imparts linearity [Linearity of expectation] on the analysis; a system
can be decomposed into its subsystems for which this same equality must hold.
In complex systems such as ours it is often very difficult to measure the 
number of items in any of a large number of subsystems. Fortunately it's
generally straightforward to measure the average duration and arrival rates. 
Additionally, the use of quota pools provides a tool to further a Little's Law 
analysis. If we have a system which dynamically controls the number of work 
items we allow into the processing portion of the system then we necessarily
are going to be increasing the waiting time above it.

The hope is that by managing a combination of quotas and queue sizes and 
measuring and managing latency we can gain intuition about the appropriate
request rate `N` we can support. Or conversely, given a request rate and quota
sizes we can understand what the delay in the system will look like. It is
important to keep in mind that the Little's Law only applies for stable systems
whereby all incident work can be processed, otherwise `W` will tend towards
infinity.

#### The `readcontroller`

The bottommost admission controller lives in the read path of the `storage` 
package. This object is driven by a number of signals. This admission 
controller is exposed to the read path through a subpackage fittingly named 
`readcontroller`. 

The read controller exposes a small surface area to the replica read path.

```go
// Admit is going to first check what should happen to this request given its
// qos.Level and the current admission and rejection level. Then, assuming it
// should be admitted, it attempts to acquire quota from the readquota. If the
// queue is full for read quota then the request is blocked for the remainder of
// this interval to repeat the process later.
func (c *Controller) Admit(context.Context, *roachpb.BatchRequest) (
  _ *Acquisition, err error,
)
```

The `Acquisition` then should be released when the read completes. In order to
support this API the `admission.Controller` was extended to additionally 
include a `Block(context.Context, qos.Level) error` method which will block a 
request regardless of its level. Blocked requests may become unblocked when 
the interval rotates. If a request is rejected a new `roachpb.Error` currently
named `ReadRejectedError` is communicated back to the client and will lead to
retries in the `DistSender`.

#### Overload Signal

Throughout the life of this project a wide variety of overload signals have
been used. This history is charted in greater detail below. The earliest of was
based on queueing delay acquiring read quota as inspired by [CoDel]. In general
this approach remains in use though at times has been supplemented with other
metrics. In the current design queueing delay once again features as the 
primary overload signal. That being said, this queuing delay is informed by a 
dynamic concurrency limit which is informed by those other metrics.

Earlier prototypes discussed below carried a constant read quota which was
debited based on an estimate of the expected size of a read request. The quota
was surrounded by an infinite-size FIFO queue. This approach proved to be 
effective while queueing occurred solely due to CPU saturation and 
synchronization overhead on the implementation of the read quota. After
optimizing the read quota the queuing delay measure is only useful if the
quota bounds concurrency. At this point, before moving on to dynamically
managing quota sizes a number of additional overload signals were explored. 

##### T-Digests and Online Distribution Monitoring

This section is something of an aside on measurement and methodologies which
have been used to try to detect overload.

One of the author's hypotheses was that divergence between a long-run 
distribution and a shorter run measurement can indicate an anomaly which
warrants a response. There are several ways to measure a distribution over
time. A simple one is an exponential moving average. HDRHistograms at
trailing points in time could also work but are quite large and were not used.

The author spent a good bit (perhaps too much) time experimenting with 
[t-digest](https://github.com/ajwerner/tidgest) sketches to track 
distributions of a variety of quantities including
latency and throughput of reads, both including and not including queuing and 
latching time. Throughput seems to be a particularly noisy signal. Latency has
the problem of being multi-modal as workload move out of RAM. This data
structure was utilized both with an exponential decay model (which isn't quite
sane) and a hierarchical trailing window model.

The goal of the tracking was to detect anomalies by looking for divergence
between short-term and long-term time periods. Several techniques were explored
including looking at divergence of fixed metrics like p90, trailing mean and 
trimmed mean, as well as the 
[Kolmogorov-Smirnov](https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test) 
metric with hopes that it might be a more robust
metric for multi-modal distributions (I was inspired by 
[Anomaly Detection on Golden Signals] to use a more robust metric than just
a delta). The metric was interesting but was likely
too sensitive to the approximations made by the t-digest in the center of the
distribution. I wonder if a reasonable correction could be made.

This developed measurement toolkit provides a basis for inputs to the dynamic
quota size explored currently and may be handy for other adaptive subsystems in
the future. 

##### Dynamic concurrency limits

While the above measurements didn't produce a general, reliable overload 
signal they provide a foundation to tune the allowed concurrency and
queue depth in the `readcontroller`. 

One mechanism used to experiment with different concurrency is a manually 
controlled cluster setting. Changing the read concurrency can control the 
total load on the system. For KV-style workloads small quotas can limit CPU 
usage and and throughput. Using artificially small quotas is useful to verify 
that the remainder of the admission control system successfully converges to 
admission control levels that maintain reasonable latency and propagate 
appropriate.

An interesting result came from running high throughput, CPU-intensive, 
OLAP-style workloads with manually controlled quota is that a quota which
permitted at most one concurrent request resulted in the highest system
throughput and two concurrent requests can nearly saturate all machines. This
to me implies that there may be a need to control the concurrency limits of
execution rather or in addition to controlling the concurrency of reads.
This is discussed in the further work section.

The basic idea is to break time into intervals just like with the admission
controller and within those intervals to enforce a quota size and maximum queue
length. The overload signal then is driven on queueing delay and queue 
overflow. The quota is increased if latencies remain good and some queuing 
occurs. It is diminished if queues are overflowing or latencies are high. In
order to deal with underflow, quota is rapidly increased in cases where few
requests are processed and latencies are good.

### Higher Level Admission Controllers

The `DistSender` admission controller is a good bit simpler than the 
`readcontroller`. Its determination of overload is solely based on the rate of
`ReadRejectedError`s. If the rate exceeds some threshold (currently set to 5%) 
then the system is marked as overloaded but the maximum level is capped at the 
highest level rejection which has been seen. The hope is to only ever possibly
block queries at the DistSender which might be blocked downstream. This however
is not currently a valid assumption as the DistSender does not track where 
errors originate. There is further work to enable the dist sender and SQL to 
anticipate downstream overload based on the state of the server to which 
traffic is intended (an optimization implemented in [DAGOR] and eminently 
available in the DistSender as well as post physical planning). 

The SQL admission controller kicks in post logical planning prior to physical
planning and execution. It carries an unbounded size blocking queue. Its
overload signal is driven by the rate of rejections from the local `DistSender`
controller with a similar policy.

The last theorized admission controller is in the flow scheduler. The flow
scheduler today has an ill-considered queuing policy that, when saturated,
leads to inscrutable errors. That queue exists for cases where large volumes of
short lives queries bump up against the fixed concurrency limit of the flow 
scheduler. That whole subsystem might benefit from a very similar treatment to
that offered to the read controller. See 
[#34229](https://github.com/cockroachdb/cockroach/issues/34229).

## Visibility

The `admission.Controller` exposes a metrics struct which can be used to track
the current levels as gauges as well as the increases, decreases, admissions, 
rejections, blocks, unblocks and cancellations as counters. State of the 
`readcontroller.Controller` is also exposed as metrics.
These make for easy additions to the Admin UI as a dashboard. 

This work proposes additionally that qos levels and time spent blocked should 
propagate through the execution engine to populate data for use in the 
statements and (when it exists) transactions pages.

## Drawbacks

There is a severe risk that some workloads will observe a lower maximum
throughput. Tail latency may be adversely affected in cases where the system
could successfully process additional load.

The overload signals proposed by this system are far from perfect. In some
cases stalled queries may be worse than uniformly increased latency.

# Rationale and Alternatives

### Targeted mitigation

While a general admission control and quality of service framework may provide
value by increasing operator control over degradation and protecting system
critical functionality, it introduces pervasive complexity. Furthermore it does
not target the remaining greatest cause of instability, OOMs. It is possible
that studying and addressing specific customer issues will lead to a handful of
smaller fixes that may ultimately meet the goals of this project.

Another issue on the radar is the harm to node-liveness due to IO saturation
caused by bulk-io operations. Currently we reduce the concurrency of bulk io
operations to avoid saturating disks. Creating a mechanism to dynamically 
throttle the rate of bulk io work in response to disk latency would allow faster
operations and work to protect node liveness (see 
[#19699](https://github.com/cockroachdb/cockroach/issues/19699)).

### Multiprocessing and OS Isolation

One approach to relieve some of the burden of an overload management system to
protecting critical system functionality of CockroachDB might be to run 
different layers in different processes and, where possible, to use OS 
functionality to prioritize a lower level process over client processing.

One could imagine a separate process which serves client connections and deals
with distributed SQL execution which communicates over UNIX domain sockets or
equivalent to a main process which operates as today. Root SQL sessions and 
admin UI queries might continue to hit the main process while all other client
traffic and dist SQL traffic might go through the child process. On linux, where
possible the child process may nice itself to yield priority to the main 
process. This has the added benefit of isolating memory and thus OOM failures
due to inexact accounting during execution from critical system functions.

Such an approach might be both complex and costly from the standpoint of 
performance but not obviously more so than this proposal.

# Unresolved Questions and Further Work

* Tuning
  * What is a good way to determine the right concurrency for each component?
  * What are the right timescales and rate changes for each component?
  * What are the right sizes for blocking queues?

* Overload signals
  * Why exactly did the quota pool work as well as it does?
  * When experimenting with overload extremely high latency was observed on
    the heartbeat RPC. This coupled with the past experience indicates that the
    time it takes for an RPC to make it to the wire from may be a good signal
    for overload. Using contexts and the gRPC stats handler should provide the
    necessary hooks to determine how long it takes for a message to be fully
    sent. This needs further investigation.

* Dynamic execution concurrency
  * Experimentation under high concurrency distributed execution revealed that
    an optimal concurrency for such workload at one. My hunch is that this
    indicates a need to reduce concurrency in the execution engine either
    additionally or instead of in the storage read layer as this proposal
    explores.

## Open ended related questions

* How do the dynamic configurations of concurrency of different components help
  or hurt the effectiveness of the system as a whole?

* How does the problem of localized overload related to the problem of degraded
  nodes or even heterogenous clusters?

* How should client provided timeouts and detection of errors due to timeout
  feed in to overload signals?

# RFC evolution

This RFC is the product of many months of research and experimentation. This
section explores the original motivation for this work and its evolution.

The earliest motivation for admission control was driven by customer experience;
customers in the wild found that bursts of OLAP-style queries could not only 
starve foreground traffic but also can take down entire clusters. This specific
failure was due to several compounding issues. We determined that one cause of 
problems was due to  a deadlock in distributed execution
([#35931](https://github.com/cockroachdb/cockroach/pull/35931)) which was
tickled in cases where flows are canceled. This cancellation happened especially
frequently due to gRPC connection failure due to 
[grpc/grpc-go#1882](https://github.com/grpc/grpc-go/issues/1882) which was 
somewhat alleviated by
[#39041](https://github.com/cockroachdb/cockroach/pull/39041) but seems to
remain a problem under severe overload. These connection failures combine poorly
with the overzealous use of the circuit breaker pattern during flow setup 
[#39135](https://github.com/cockroachdb/cockroach/pull/39135). Another
observation was that node liveness starvation was marked by extremely long
delays between when RPCs should be sent and when they are received. This has
been severely mitigated by using a separate connection
[#39172](https://github.com/cockroachdb/cockroach/pull/39172). Initial
prototypes were singularly focused on addressing this class of overload failure
which have now been largely mitigated. In order to be considered, this proposal
should make serious strides towards achieving later goals given that the
motivation for the primary goal is now rare.

## Online capacity estimation using optimizer costs and random early detection

The earliest prototype was built on a free-friday with the customer workload
in mind specifically. The motivating goal was to prevent node liveness
failures in the face of relatively unbounded concurrency of expensive queries.
When building this first prototype, starvation of small queries was not overly
considered.

This initial prototype made some overly simplifying assumptions. One such
assumption was that each node acts as a gateway for roughly equal amounts of
load on the system and that that load is spread uniformly. Armed with this
assumption the prototype attempted to estimate total capacity of the system
online and permit only that volume of query traffic to be processed at a time.

One tenant of this approach was that traffic should be pruned as close to the
gateway as possible. The challenge with this is that the gateway has the least
amount of information about the state of the cluster as a whole. Furthermore
the gateway lacks clarity about the capacity of the system.

The initial prototype worked by putting two quotas in place: one in the kv
read path and one at the SQL layer just prior to execution but after planning.
The idea was that if we are overloaded we can detect that at the KV layer and
change decision making at the SQL execution layer.

The prototype made several additional assumptions:

1) A small constant number of queries cannot overload the cluster
    - Or if two or three instances of a query can overload a cluster then 
      that query is out of scope for this prototype.
2) In the face of massive change in workload it is acceptable to stall query
   execution some constant multiple of the runtime of the new queries.
    - This may be unrealistic.
3) The proportion of query load due to a given gateway node (in terms of cost)
   remains constant.

These assumptions all may be invalid but they were the original jumping off
point.

The idea was that using the historical cluster performance we can track online
the capacity of the cluster to execute queries. An obvious question then is
about the right unit for this capacity. The only unit considered prior to
execution was optimizer cost. It might be interesting to explore using
historical statistics for a given statement fingerprint to make an execution
cost decision.  

The hope was that each node could maintain a quota of available capacity. Each
statement would acquire an allocation of quota and upon completion return that
quota to the pool. Quota capacity can be estimated by looking at how many
queries the current node believes have been in the system historically. This
can be estimated by using Little's Law whereby we assume the system is in
steady state such that the long-term rate of arrival is equal to the rate of
completion. Given this we can estimate the the amount of cost in the system by
multiplying the average time per cost by the average rate of arrival of cost.

The next challenge was to determine when the quota has exceeded the true
capacity of the system. The hope was that we could use feedback from the KV
layer to determine this level. The basic architecture of the KV feedback
mechanism relates to the later proposal but is simpler; requests are rejected
if the queue is full and may be rejected if the queue is getting full. There is
no notion of qos class or being blocked. The DistSender retries rejected
requests with exponential back-off forever. The DistSender also notes the
failures which is observed by the SQL layer to adjust the quota.

This mechanism proved to be extremely effective but not for the reasons the 
author anticipated. The initial justification for a read quota was due to 
attributed to RAM. A read request must allocate memory for its response. Up to 
this point there is no accounting for those bytes. Unfortunately there is no a 
priori information about the size of the response for a given read (though it 
is likely the case that the planning information carries reasonably good 
approximations of how many rows a scan request is likely to contain).

The driver of queueing of course is a quota; only a fixed number of reads
should be outstanding at a time. In order to determine this number the read
quota kept a fixed-size memory pool which had a hard-coded value. When requests
arrived they approximated how much quota they would need based on the
historical size of reads. The original mechanism for this approximation was to
keep two numbers, the current estimate and the trailing average. When read
requests completed they record the size of their response into the exponential
trailing average. If a read is under-estimated, the current estimate is
increased by a multiplicative factor (1.5). Periodically the current estimate
is moved towards the trailing average. This roughly leads to a system which
estimates reads as near the recent maximum size. It has an advantage of
responding quickly to workload changes. Generally I considered this
over-estimation to be okay. The system would track trailing average rate of
queries and would set a maximum queue size to be some multiple (<1) of the
expected rate over the interval. As the queue approached its maximum, requests
would be rejected with an error randomly at a rate which increased as the
queue filled up (hence the RED inspiration). These rejections were then
retried indefinitely at the `DistSender` with a slow exponential back-off. 

At one point I experimented with an extra flag to prevent retried requests from
experience the random early detection rejections as inspired by the 
[FoundationDB load balancing talk]. This probably should have been coupled
with shortening queues further but alas.

There were a number of problems with this approach.

1) How do you set the initial SQL cost capacity estimate?

Over time, as queries complete with no rejections, the capacity will creep up.
This warmup phase means that right when a node starts it was very conservative
about what it would allow to execute. Still, there's an issue about what to do
with a query which arrives and has a cost greater than the entire quota. In 
order to deal with this I added a notion of "large" queries which act as a sort
of quota canary. The system always allows up to a constant number (3) of large 
queries to execute and did not count these queries towards the quota. In order
to qualify as large a statement needed to have a cost at least as large as
half the total quota.

2) A small number of large queries can hold off all small queries for a very
long time.

Say we can successfully concurrently process 3 large OLAP-style queries.
Over time our quota will converge to around 3x their cost. As the system
calibrates it may let in more than these 3 queries, say 6 or even more. At that
point the system is in quota debt based on its current estimate and it won't
let new queries begin execution until that debt has been paid. This means all
queries which arrive after we've over-committed block for a very long time.

Maybe this could have been mitigated somewhat with a LIFO queueing discipline
and some cost debt system. It wasn't explored further.

3) Exponential back-off from the DistSender can be really bad for latency.
4) Random early detection can adversely affect queries before overload is 
   reached.
5) Optimizer cost is hard to reason about.

## qos levels and the admission controller without blocking

Random early detection was deemed problematic because it leads to many retries 
which can have a major impact on tail latency when things weren't too bad. 
Wide area links exacerbate the client-side retry cost. 

At this point we began to have customer calls and started talking about the 
need for qos levels. My concerns about subsequent overload continued to grow 
and customer expression of a desire for control over quality of service became
clear.

This information led to the birth of the original `admission.Controller` which 
was modeled more directly the DAGOR controller. Our situation differs from 
the Netflix and WeChat worlds in so far as we can't really reject traffic. 
Rejecting traffic is not helpful to our database primarily
because we don't have a mechanism to inform clients that they should cooperate 
with us by taking a hint on how long they should back off. This is worthy of 
its own discussion.

Around this time I also noticed the tight coupling between the monitoring 
inside of the fledgling `readcontroller` and its state. This led to a major
refactor with a focus on moving measurement outside of the stateful 
controllers. It was at this point that I also began to focus on using queueing
delay as an overload signal. What emerged was a read quota which seemed
rather adept at detecting overload conditions. Later I would discover that 
this had roughly nothing to do with the memory accounting and mostly to do 
with inefficiency in the read quota implementation.

There was also some experimentation using online t-digests to approximate 
memory usage rather than the adaptive algorithm of the previous prototype. 
This was likely more of a distraction than a source of value. 

## Queueing delay with blocking

Sending requests back to the caller immediately when we cannot service them 
has an extremely negative impact on tail latency in the face of short-lived 
overload. This is made much worse when nodes are far apart. As the system got
even close to rejecting a request tail latency would rise sharply and admission
controllers in remote nodes would react. In order to localize the response
behavior the blocking queue was added to the admission controller. At this
point we had a prototype that made for a pretty great demo. The system
could find its way to a nice dynamic equilibrium in a variety of interesting
scenarios. One compelling demo was to run TPC-C at a reasonable load, say 50%
and then launch a very high concurrency barrage of OLAP-style queries at a low
qos class. Without admission control TPC-C would achieve single-digit
efficiencies. With it, we could observe passing or nearly passing performance.

This approach however had a serious issue: KV workloads were directly affected
and top of line throughput for high-concurrency benchmarks were reduced by
over 30% in some cases.

## Optimized quota pool and the need for dynamic concurrency

The obvious bottleneck was the read quota implementation which necessarily
performed two selects in addition to several mutex interactions in order to
acquire quota even if plenty was available. This overload signal which seemed
to communicate information about the go runtime's run queue length was a
double-edged sword.
[#38759](https://github.com/cockroachdb/cockroach/pull/38759) added a fast-path
to the quota pool. Once the fast path was added the
quota pool was no longer a bottleneck and queuing no longer occurred unless
insufficient quota was available. Now the system needed to pick an appropriate
quantity of quota in order to work. At this point I began to experiment with
dynamic concurrency limits in the style of 
[Netflix/concurrency-limits](https://github.com/Netflix/concurrency-limits) 
which brings us to the current state of this document.

# References

* [DAGOR]
* [Netflix concurrency blog post]
* [CoDel]
* Random Early Detection ([RED])
* [FoundationDB load balancing talk]
* [Anomaly Detection on Golden Signals]


[DAGOR]: https://www.cs.columbia.edu/~ruigu/papers/socc18-final100.pdf
[Netflix concurrency blog post]: https://medium.com/@NetflixTechBlog/performance-under-load-3e6fa9a60581
[CoDel]: https://queue.acm.org/detail.cfm?id=2209336
[RED]: https://www.icir.org/floyd/papers/early.twocolumn.pdf
[FoundationDB load balancing talk]: https://www.slideshare.net/FoundationDB/load-balancing-theory-and-practice
[Anomaly Detection on Golden Signals]: https://www.usenix.org/conference/srecon19asia/presentation/chen-yu