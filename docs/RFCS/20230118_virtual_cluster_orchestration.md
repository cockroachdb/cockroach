- Feature Name: Virtual cluster orchestration in v23.1
- Status: completed
- Start Date: 2023-01-18
- Authors: knz ajw dt ssd
- RFC PR: [#96147](https://github.com/cockroachdb/cockroach/pull/96147)
- Cockroach Issue: [#96146](https://github.com/cockroachdb/cockroach/issues/96146)
  [#83650](https://github.com/cockroachdb/cockroach/issues/83650)
  [#93145](https://github.com/cockroachdb/cockroach/issues/93145)

# Summary

This RFC proposes to clarify the lifecycle of virtual clusters
(henceforth abbreviated "VC", a.k.a. "secondary tenants") in v23.1 and
introduce a mechanism by which the SQL service for a VC
can be proactively started on every cluster node (shared-process
execution as used in Unified Architecture/UA clusters).

The clarification takes the form of a state diagram (see below).

The new mechanism relies on the introduction of a new column in
`system.tenants`, ServiceMode, that describes the *deployment style*
for that VC's servers. This can be NONE (no service),
EXTERNAL (SQL pods, as in CC Serverless) or SHARED (shared-process
with the KV nodes). When in state SHARED, the KV nodes auto-start the
service.

We also propose to use this mechanism as a mutual exclusion interlock
to prevent SQL pods from starting when the VCs use
shared-process deployments.

The implementation for this change is spread over the following PRs:

- https://github.com/cockroachdb/cockroach/pull/95691
- https://github.com/cockroachdb/cockroach/pull/95658
- https://github.com/cockroachdb/cockroach/pull/96144

# Motivation

Until today, the only requirements we knew of were that:

1. we could not start the SQL service for INACTIVE VCs, whose
   keyspace is read-only and cannot support initialization of the SQL
   service yet.
2. we could not start the SQL service for all ACTIVE VCs
   inside a regular KV node, because there may be thousands of those in
   CC serverless.

The implementation as of this writing is that of a "server controller"
able to instantiate the service for a VC **upon first use** (triggered
by a client connection), and only if that VC is ACTIVE.

Unfortunately, this simplistic view failed to provide answers to many
operational questions. These questions include, but are not limited
to:

- what if there are 3 nodes, and a client only opens a SQL session on
  the first node. How can we *distribute queries* to all 3 nodes? At
  that point the SQL service is only running on 1 node and unable to
  run DistSQL on the other 2.
- what if a cluster is idle (no SQL connection) but there are
  scheduled jobs to run?
- which VCs should be woken up to serve a UI login request when a
  user hits the DB console the first time?

Generally, there is appetite for some form of mechanism that
**proactively starts the SQL service on all nodes.**

Additionally, we have two additional concerns:

- the way the SQL instances table is managed currently precludes
  serving a VC simultaneously using shared-process deployment
  and using separate SQL pods, lest RPC communication becomes
  incorrect. We would like some guardrails to prevent this mixed
  deployment style.
- we would like to prevent the SQL service from starting
  when the VC keyspace is not ready, e.g. during replication.

# Technical design

The proposal is to evolve the current VC record state diagram,
from this (Current state diagram as of 2022-01-17):

![baseline state diagram](20230118_virtual_cluster_orchestration/baseline.png)
([diagram source](https://www.plantuml.com/plantuml/uml/ZP91IyGm48Nl-HL3JoektlSW6pAZ199DITFru7eeD0UXzh0R3oB-ToS4QNGfUEjxV5zU2TcTqt1Axs-Ju8yKmSTXTFFq0Py2e5GI4OOpD5rEMqwD-AhefzDhYZMkWZvGPPHYaJ1ECSMnKj2xjX9oR0IKUhEvX-UR5zZ2rQEwvhOSj_G5EsWVUcb0cRuBv8-pjfPGoq2ycxdCIqJcbIUkBuq7xonuQerMCcYtNxIo-6CUvp-gu7D_Wz9mVRcWd6qD1QfN5XbntMnbtz6Tfwwtl5_fjpdjfIK8JfJzV_pAYfHlSkL2nIxE8p-JRm00))

(3 states: ADD, ACTIVE, DROP. ADD is used during streaming replication.)

To the following new state diagram:

![new state diagram](20230118_virtual_cluster_orchestration/new.png)
([diagram source](https://www.plantuml.com/plantuml/uml/fPD1JuCm5CVl_HGlEwbnySx1x8K-bGGeAMLgd0SYJLo2Q5WrBiRlRjD1HP5iYITeytw_z-UrB3QcRCthKytiYz4WTLc9fjx21mFORSquAWm0ELyrESbb55AGYfHcK6vWtPGFPlscsUT0SDskZHuR926__NQ0bxIMsUwhpgzG4dVrcyUorTK-W6uKoHHZX-Xtezkch1diki5yVjvv802CRIai8waAZ26CYzokhKgXLD09km0CUimQEbS0qmQHzO9XQv_dmER-OnXuz5y1F3Z1IKen8L327opNjJQw6XyhboBxusPKxVCB0_Tm3UzExcsxeylmsEVmsyun3X4b2V485TaGjixLznVEGg4aL3J6BgH8G58MHo6gIAIUTLqzk6f-aSox3nXGA98i9aN3HZy5VZ7foB38wKnINYGspK1qEZm0FoitoubXiOLkAliRVm40))

In prose:
- We split the VC "state" into two fields:
  - DATA (`DataState`) indicates the readiness of the logical
    keyspace: ADD, READY, DROP.
  - SERVICE (`ServiceMode`) indicates whether there's a service
    running doing processing for that VC: NONE (no server
    possible), SHARED (shared-process multitenancy) and EXTERNAL
    (separate-process multitenancy)
- New SQL syntax ALTER VIRTUAL CLUSTER START/STOP SERVICE SHARED/EXTERNAL to switch the SERVICE.
- Each KV node is responsible to wake up the SQL service for all
  VC records in the SERVICE:SHARED state. (This will be done
  initially via a refresh loop, and can be enhanced to become more
  precise via a rangefeed or a fan-out RPC.)
  - We would remove the code that auto-starts
    a service upon first connection. Instead, an attempt to connect to
    a service that is not yet started would fail.
- Each KV node is also responsible for shutting down SQL services that
  are currently running for a VC that is in the SERVICE:NONE
  state.
- When *Not* in SERVICE:NONE state (i.e. either SHARED or
  EXTERNAL), VCs cannot be renamed (at least not in
  v23.1 - this is discussed further in the appendix at the end).
- The `mt start-sql` command (start standalone SQL pod, used in CC
  Serverless) would refuse to start a SQL service for a VC whose
  record is not in state SERVICE:EXTERNAL, because at this stage we do
  not support running mixed-style deployments (with both
  separate-process and shared-process SQL services) - this solves
  [this issue](https://github.com/cockroachdb/cockroach/issues/93145).
- The SQL service would always refuse to start when the data state is
  not READY (cf [this
  issue](https://github.com/cockroachdb/cockroach/issues/83650)), and
  when the service mode does not match the requested deployment.

Once we have this mechanism in place:
- UI console login uses SERVICE:SHARED VCs for multi-login. No
  question remains "which VCs to log into".

We also take the opportunity to restructure the `system.tenants`
table, to store the data state and service mode as separate SQL
columns.

## Drawbacks

None known.

## Rationale and Alternatives

The main alternative considered was to not rely on a separate service mode column.
Instead:

- A new cluster setting `tenancy.shared_process.auto_start.enabled`,
  which, when set (it would be set for UA clusters) automatically
  starts the SQL service for all VCs in state ACTIVE.
- Like in the main proposal, we would not need to (and would remove)
  the code that auto-starts a service upon first connection. Instead,
  an attempt to connect to a service that is not yet started would
  fail.
- Server controller would also auto-shutdown VC records that go to
  state DROP or get deleted.
- DB console served from KV node / server controller would select
  VCs for auto-login as follows: if
  `tenancy.shared_process.auto_start.enabled`is set, all ACTIVE
  VCs otherwise, only `system`.

This alternate design does not allow us to serve some VCs using
separate processes, and some other VCs using shared-process
multitenancy, inside the same cluster. We are interested in this use
case for SRE access control in CC Serverless (e.g. using a
VC with limited privileges to manage the cluster, where SREs would
connect to)

We have also considered the following alternatives:

- a cluster setting that controls which VCs to wake up on every node.

  We disliked the cluster setting because it does not offer us clear
  controls about what happens on the "in" and "out" path of the state
  change.

- a constraint that max 1 SQL service for a VC can run at a time.

  This makes certain use cases / test cases difficult.

- the absence of any constraint on the max number of SQL service per node.

  We dislike this because it's too easy for folk to make mistakes and
  get confused about which VCs have running SQL services. We also
  dislike this because it will make it too easy for customers eager to
  use multi-tenancy to (ab)use the mechanisms.

- a single fixed VC record (with a fixed ID or a fixed name) that
  would be considered as "the" resident VC, and have servers only
  start SQL for that one VC.

  We dislike this because it will make flexible scripting of C2C replication more difficult.

## Summary table

| | Main approach: separate SERVICE and DATA states | Approach 2: no separate RESIDENT state, new cluster setting auto_activate |
|--|--|--|
| When does the SQL service start? | When record enters SERVICE:SHARED state. Or on node startup for VCs already in SERVICE:SHARED state. | When record enters ACTIVE state and auto_activate is true. Or on node startup for VCs already in ACTIVE state. |
| When does the SQL service stop? | When VC record leaves SERVICE:SHARED state. Or on node shutdown. | When record gets dropped or deleted. Or on node shutdown. |
| Steps during C2C replication failover. | ALTER VIRTUAL CLUSTER COMPLETE REPLICATION + ALTER VIRTUAL CLUSTER START SERVICE SHARED  | ALTER VIRTUAL CLUSTER COMPLETE REPLICATION |
| Which VCs to consider for UI login? | All VCs in SERVICE:SHARED state. | If auto_activate is true, all VCs in ACTIVE state. Otherwise, only system VC. |
| Ability to run some VCs using shared-process multitenancy in CC Serverless host clusters, alongside to Serverless fleet, for access control for SREs. | Yes | No |
| Control on number of SQL services separate from VC activation? | Yes | No |

# Explain it to folk outside of your team

The explanation here is largely unchanged from the previous stories we
have told about v23.1.

The main change is that a user would need to run `ALTER VIRTUAL CLUSTER ...
START SERVICE SHARED/EXTERNAL` before they can start the SQL service and
connect their SQL clients to it.

# Unresolved questions

N/A

# Appendix

## About VC renames

Why we may not support renaming VCs while they have SQL services running.

There are at least the following problems:
- SQL client traffic. We want clients to route their traffic by name.
  If we rename while service is active, clients suddenly see their
  conns going to a different cluster. That seems like undesirable UX.
- VC names in UI login cookies
  - Here the problem is that if a VC is renamed the cookie is invalid.
  - Also if another VCs get renamed to a name that was held by
    another VC previously, the browsers will sent cookies for that
    name to it.
  - Possible solution: hash the VC ID in the cookie? Or something
  - We don't know if we can do this improvement in v23.1, so the
    conservative approach is to prevent renames in that state.
- VC names in metrics
  - for metrics, we observe a service not a name, if the VC gets
    renamed while the service is still running the metrics being
    observed should still be that of the original service.
  - David T disagrees.
  - We don't know what the resolution should be, so the conservative
    approach is to prevent renames in that state until we know what we
    want.

## Possible future extension

In the future, we may want to support serving SQL for a VC keyspace
in a read-only state.

![later state diagram](20230118_virtual_cluster_orchestration/future.png)
([diagram source](https://www.plantuml.com/plantuml/uml/ZPJ1ZjCm48RlVehHdW1HSUy1hKa6DYAn8yTTGBizHCGIbOAB4eEeAjwTKHoDwgPRzPHqvl_c7q_6lHz3EuJVFtj1By61SMsdVN-0em28eRjSMhc6ZPAFiYZbXnBle1rXvzllOVV7dUiCobohyjkpRz0y5XckGMaLcM5_Wng_MZHAbZDnYq7p80tcCp0A0TmTh9wwVGYkswxUKmxMa3rWzhdMXR8aeqWgS9U2e_XtCfulmuxUZfVQkdneOWvNrUa9nX_juBehm79AxczmWszx0T4DLjHth4CdbYL9mQAIob85Aus5kSxiIzoi9Z2M86u1wdh9iHytSTsH6nV0n6skASk-3AQSeMU5O3L_kzprBhXk-ULoe-jfZDsm_oLWUaoLdzvigUwhu7ph0tpANhClPoUOJOkgmhoG5icKqfECDv5Mpo3bMFtHw0eCrcMHILjenSZamVfd7m71bLu-TQQhkaIBITc4un_dQ2qt3Rups6mgiZpXtvuECxqRiGiAkYJAwqXN9qdCgyqpZADEXYZkdU_e_-W_))
