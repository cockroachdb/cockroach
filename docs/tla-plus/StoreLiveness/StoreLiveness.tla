----------------------------- MODULE StoreLiveness ----------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences

CONSTANTS Nodes, MaxClock, MaxRestarts, HeartbeatIntervals
CONSTANTS MsgHeartbeat, MsgHeartbeatResp, AllowMsgReordering, AllowClockRegressionOnRestart
ASSUME Cardinality(Nodes) > 0
ASSUME Cardinality(HeartbeatIntervals) > 0
ASSUME \A i \in HeartbeatIntervals: i < MaxClock

(*****************************************************************************)
(* StoreLiveness is a specification for the Store Liveness fabric. Store     *)
(* Liveness sits below Raft and power the Raft leader-lease mechanism.       *)
(*                                                                           *)
(* The central safety property of Store Liveness is Durable Support. A node  *)
(* in the Store Liveness fabric can receive support from another node with   *)
(* an associated "end time" and be confident that this support will not be   *)
(* withdrawn until that supporting node's clock exceeds the end time. This   *)
(* property allows us to build lease disjointness, as we can be sure that a  *)
(* future lease will have a start time that is greater than the end time of  *)
(* the previous lease's corresponding store liveness support.                *)
(*****************************************************************************)

(*--algorithm StoreLiveness
variables
  max_epoch     = [i \in Nodes |-> 1];
  max_requested = [i \in Nodes |-> 0];
  max_withdrawn = [i \in Nodes |-> 0];
  support_from  = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 1, expiration |-> 0]]];
  support_for   = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 0, expiration |-> 0]]];
  clocks        = [i \in Nodes |-> 1];
  network       = [i \in Nodes |-> EmptyNetwork];

define
  \* Arbitrary maximum element in the set S.
  Max(S) == CHOOSE x \in S : \A y \in S : y <= x
    
  \* Define Nodes as a symmetry set. Cuts runtime by Cardinality(Nodes)!
  Symmetry == Permutations(Nodes)

  \* If we allow message reordering, represent the network as a set. Otherwise,
  \* represent it as a sequence.
  EmptyNetwork == IF AllowMsgReordering THEN {} ELSE <<>>

  EpochValid(map, i, j) == map[i][j].expiration /= 0
  \* Has i ever received support from j for the current epoch?
  SupportFromEpochValid(i, j) == EpochValid(support_from, i, j)
  \* Has i ever supported j for the current epoch?
  SupportForEpochValid(i, j) == EpochValid(support_for, i, j)

  EpochSupportExpired(map, i, j, supporter_time) == map[i][j].expiration <= supporter_time
  \* Is i's support from j (according to i's support_from map) expired (according to j's clock)?
  SupportFromExpired(i, j) == EpochSupportExpired(support_from, i, j, clocks[j])
  \* Is i's support for j (according to i's support_for map) expired (according to i's clock)?
  SupportForExpired(i, j) == EpochSupportExpired(support_for, i, j, clocks[i])

  \* Is support for i from j upheld?
  SupportUpheld(i, j) == support_from[i][j].epoch = support_for[j][i].epoch

  \* Can i withdraw support for j?
  CanInvalidateSupportFor(i, j) == SupportForEpochValid(i, j) /\ SupportForExpired(i, j)
  CanInvalidateSupportForSet(i) == {j \in Nodes \ {i}: CanInvalidateSupportFor(i, j)}

  \* If we ever had support for the current i=>j epoch, then either support
  \* is still upheld or the support we have received had expired according to
  \* j's clock.
  \*
  \* Durable support is the central safety property of the algorithm.
  DurableSupportInvariant ==
    \A i \in Nodes:
      \A j \in Nodes \ {i}:
        SupportFromEpochValid(i, j) =>
          (SupportUpheld(i, j) \/ SupportFromExpired(i, j))

  \* If support for i from j is provided in en epoch, the end time of support
  \* known to the supporter (j) must be greater than or equal to the end time
  \* of support known to the supportee (i).
  \*
  \* This is a structural invariant in the algorithm used to provide safety.
  SupportProvidedLeadsSupportAssumedInvariant ==
    \A i \in Nodes:
      \A j \in Nodes \ {i}:
        SupportUpheld(i, j) =>
          support_from[i][j].expiration <= support_for[j][i].expiration

  \* A node's current epoch leads its supported epoch by all other nodes.
  \*
  \* This is a structural invariant in the algorithm used to provide safety.
  CurrentEpochLeadsSupportedEpochsInvariant ==
    \A i \in Nodes:
      \A j \in Nodes \ {i}:
        max_epoch[i] >= support_from[i][j].epoch

  \* The minimum epoch assigned to store liveness support after support has
  \* been withdrawn from a prior epoch leads the supportee's support_from epoch
  \* by exactly 1.
  \*
  \* This is a structural invariant in the algorithm used to provide safety.
  WithdrawnSupportMinimumEpochInvariant ==
    \A i \in Nodes:
      \A j \in Nodes \ {i}:
        (support_for[i][j].epoch > support_from[j][i].epoch /\ support_for[i][j].expiration = 0) =>
          support_for[i][j].epoch = support_from[j][i].epoch + 1

  \* The Support Disjointness invariant states that, for any requester and supporter,
  \* no two support intervals overlap in time; i.e. a node does not receive support 
  \* for a new epoch while support for the previous epoch is still valid.
  \* We check this invariant as two assertions in the spec below because it is not
  \* possible to state the invariant here with the minimal algorithm state we keep.
end define;

macro forward(clock, time)
begin
  if clock < time then
    clock := time;
  end if;
end macro

macro send_msg(to, msg)
begin
  if AllowMsgReordering then
    network[to] := network[to] \union {msg};
  else
    network[to] := Append(network[to], msg);
  end if;
end macro

macro send_heartbeat(to)
begin
  with interval \in HeartbeatIntervals do
    forward(max_requested[self], clocks[self] + interval);
    send_msg(to, [
      type       |-> MsgHeartbeat,
      from       |-> self,
      epoch      |-> support_from[self][to].epoch,
      expiration |-> max_requested[self],
      now        |-> clocks[self]
    ]);

  end with;
end macro

macro send_heartbeat_resp(to)
begin
  send_msg(to, [
    type       |-> MsgHeartbeatResp,
    from       |-> self,
    epoch      |-> support_for[self][to].epoch,
    expiration |-> support_for[self][to].expiration,
    now        |-> clocks[self]
  ]);
end macro

macro recv_msg()
begin
  if AllowMsgReordering then
    with recv \in network[self] do
      network[self] := network[self] \ {recv};
      msg := recv;
    end with;
  else
    msg := Head(network[self]);
    network[self] := Tail(network[self]);
  end if;
  \* Clock propagation is necessary for the Support Disjointness Invariant.
  forward(clocks[self], msg.now);
end macro

macro restart()
begin
  if AllowClockRegressionOnRestart then
    clocks[self] := Max({max_withdrawn[self], max_requested[self], clocks[self] - 1});
  else
    clocks[self] := Max({max_withdrawn[self], max_requested[self], clocks[self]});
  end if;
  max_epoch[self]    := max_epoch[self] + 1;
  support_from[self] := [j \in Nodes \ {self} |-> [epoch |-> max_epoch[self], expiration |-> 0]];
end macro

process node \in Nodes
variables
  restarts = 0;
  msg      = [type |-> FALSE];
begin Loop:
  while TRUE do
    either
      await clocks[self] < MaxClock;
      TickClock:
        clocks[self] := clocks[self] + 1;
    or
      await clocks[self] < MaxClock;
      TickClockAndSendHeartbeats:
        clocks[self] := clocks[self] + 1;
        with i \in Nodes \ {self} do
          send_heartbeat(i);
        end with;
    or
      await restarts < MaxRestarts;
      Restart:
        restart();
        restarts := restarts + 1;
    or
      await CanInvalidateSupportForSet(self) /= {};
      WithdrawSupport:
        with expired \in CanInvalidateSupportForSet(self) do
          support_for[self][expired].epoch      := support_for[self][expired].epoch + 1 ||
          support_for[self][expired].expiration := 0;
          forward(max_withdrawn[self], clocks[self]);
        end with;
    or
      await network[self] /= EmptyNetwork;
      recv_msg();

      if msg.type = MsgHeartbeat then
        ReceiveHeartbeat:
          if support_for[self][msg.from].epoch = msg.epoch then
            \* Forward the expiration to prevent regressions due to out-of-order
            \* delivery of heartbeats.
            forward(support_for[self][msg.from].expiration, msg.expiration);
          elsif support_for[self][msg.from].epoch < msg.epoch then
            assert support_for[self][msg.from].expiration < msg.expiration;
            \* This assertion is part of the Support Disjointness invariant.
            \* We assert that the requestor of support with this new epoch has
            \* a clock that exceeds the expiration of the previous epoch.
            assert support_for[self][msg.from].expiration < clocks[self];
            support_for[self][msg.from].epoch      := msg.epoch ||
            support_for[self][msg.from].expiration := msg.expiration;
          end if;

          send_heartbeat_resp(msg.from);

      elsif msg.type = MsgHeartbeatResp then
        ReceiveHeartbeatResp:
          if max_epoch[self] < msg.epoch then
            max_epoch[self] := msg.epoch;
          end if;
          if support_from[self][msg.from].epoch = msg.epoch then
            \* Forward the expiration to prevent regressions due to out-of-order
            \* delivery of heartbeat responses.
            forward(support_from[self][msg.from].expiration, msg.expiration);
          elsif support_from[self][msg.from].epoch < msg.epoch then
            assert support_from[self][msg.from].epoch = msg.epoch - 1;
            assert msg.expiration = 0;
            \* This assertion is part of the Support Disjointness invariant.
            \* We assert that support for the previous epoch has expired before 
            \* increasing the epoch and forgetting the previous epoch's expiration.
            \* We check the expiration wrt clocks[self] because, by the propagation
            \* of clocks via messages, we know that clocks[self] <= clock[msg.from].
            assert support_from[self][msg.from].expiration <= clocks[self];
            support_from[self][msg.from].epoch      := msg.epoch ||
            support_from[self][msg.from].expiration := msg.expiration;
          end if;
      else
        assert FALSE;
      end if;
    end either;
  end while;
end process;
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "891e8ae9" /\ chksum(tla) = "38884a6c")
VARIABLES max_epoch, max_requested, max_withdrawn, support_from, support_for, 
          clocks, network, pc

(* define statement *)
Max(S) == CHOOSE x \in S : \A y \in S : y <= x


Symmetry == Permutations(Nodes)



EmptyNetwork == IF AllowMsgReordering THEN {} ELSE <<>>

EpochValid(map, i, j) == map[i][j].expiration /= 0

SupportFromEpochValid(i, j) == EpochValid(support_from, i, j)

SupportForEpochValid(i, j) == EpochValid(support_for, i, j)

EpochSupportExpired(map, i, j, supporter_time) == map[i][j].expiration <= supporter_time

SupportFromExpired(i, j) == EpochSupportExpired(support_from, i, j, clocks[j])

SupportForExpired(i, j) == EpochSupportExpired(support_for, i, j, clocks[i])


SupportUpheld(i, j) == support_from[i][j].epoch = support_for[j][i].epoch


CanInvalidateSupportFor(i, j) == SupportForEpochValid(i, j) /\ SupportForExpired(i, j)
CanInvalidateSupportForSet(i) == {j \in Nodes \ {i}: CanInvalidateSupportFor(i, j)}






DurableSupportInvariant ==
  \A i \in Nodes:
    \A j \in Nodes \ {i}:
      SupportFromEpochValid(i, j) =>
        (SupportUpheld(i, j) \/ SupportFromExpired(i, j))






SupportProvidedLeadsSupportAssumedInvariant ==
  \A i \in Nodes:
    \A j \in Nodes \ {i}:
      SupportUpheld(i, j) =>
        support_from[i][j].expiration <= support_for[j][i].expiration




CurrentEpochLeadsSupportedEpochsInvariant ==
  \A i \in Nodes:
    \A j \in Nodes \ {i}:
      max_epoch[i] >= support_from[i][j].epoch






WithdrawnSupportMinimumEpochInvariant ==
  \A i \in Nodes:
    \A j \in Nodes \ {i}:
      (support_for[i][j].epoch > support_from[j][i].epoch /\ support_for[i][j].expiration = 0) =>
        support_for[i][j].epoch = support_from[j][i].epoch + 1

VARIABLES restarts, msg

vars == << max_epoch, max_requested, max_withdrawn, support_from, support_for, 
           clocks, network, pc, restarts, msg >>

ProcSet == (Nodes)

Init == (* Global variables *)
        /\ max_epoch = [i \in Nodes |-> 1]
        /\ max_requested = [i \in Nodes |-> 0]
        /\ max_withdrawn = [i \in Nodes |-> 0]
        /\ support_from = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 1, expiration |-> 0]]]
        /\ support_for = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 0, expiration |-> 0]]]
        /\ clocks = [i \in Nodes |-> 1]
        /\ network = [i \in Nodes |-> EmptyNetwork]
        (* Process node *)
        /\ restarts = [self \in Nodes |-> 0]
        /\ msg = [self \in Nodes |-> [type |-> FALSE]]
        /\ pc = [self \in ProcSet |-> "Loop"]

Loop(self) == /\ pc[self] = "Loop"
              /\ \/ /\ clocks[self] < MaxClock
                    /\ pc' = [pc EXCEPT ![self] = "TickClock"]
                    /\ UNCHANGED <<clocks, network, msg>>
                 \/ /\ clocks[self] < MaxClock
                    /\ pc' = [pc EXCEPT ![self] = "TickClockAndSendHeartbeats"]
                    /\ UNCHANGED <<clocks, network, msg>>
                 \/ /\ restarts[self] < MaxRestarts
                    /\ pc' = [pc EXCEPT ![self] = "Restart"]
                    /\ UNCHANGED <<clocks, network, msg>>
                 \/ /\ CanInvalidateSupportForSet(self) /= {}
                    /\ pc' = [pc EXCEPT ![self] = "WithdrawSupport"]
                    /\ UNCHANGED <<clocks, network, msg>>
                 \/ /\ network[self] /= EmptyNetwork
                    /\ IF AllowMsgReordering
                          THEN /\ \E recv \in network[self]:
                                    /\ network' = [network EXCEPT ![self] = network[self] \ {recv}]
                                    /\ msg' = [msg EXCEPT ![self] = recv]
                          ELSE /\ msg' = [msg EXCEPT ![self] = Head(network[self])]
                               /\ network' = [network EXCEPT ![self] = Tail(network[self])]
                    /\ IF (clocks[self]) < (msg'[self].now)
                          THEN /\ clocks' = [clocks EXCEPT ![self] = msg'[self].now]
                          ELSE /\ TRUE
                               /\ UNCHANGED clocks
                    /\ IF msg'[self].type = MsgHeartbeat
                          THEN /\ pc' = [pc EXCEPT ![self] = "ReceiveHeartbeat"]
                          ELSE /\ IF msg'[self].type = MsgHeartbeatResp
                                     THEN /\ pc' = [pc EXCEPT ![self] = "ReceiveHeartbeatResp"]
                                     ELSE /\ Assert(FALSE, 
                                                    "Failure of assertion at line 253, column 9.")
                                          /\ pc' = [pc EXCEPT ![self] = "Loop"]
              /\ UNCHANGED << max_epoch, max_requested, max_withdrawn, 
                              support_from, support_for, restarts >>

TickClock(self) == /\ pc[self] = "TickClock"
                   /\ clocks' = [clocks EXCEPT ![self] = clocks[self] + 1]
                   /\ pc' = [pc EXCEPT ![self] = "Loop"]
                   /\ UNCHANGED << max_epoch, max_requested, max_withdrawn, 
                                   support_from, support_for, network, 
                                   restarts, msg >>

TickClockAndSendHeartbeats(self) == /\ pc[self] = "TickClockAndSendHeartbeats"
                                    /\ clocks' = [clocks EXCEPT ![self] = clocks[self] + 1]
                                    /\ \E i \in Nodes \ {self}:
                                         \E interval \in HeartbeatIntervals:
                                           /\ IF (max_requested[self]) < (clocks'[self] + interval)
                                                 THEN /\ max_requested' = [max_requested EXCEPT ![self] = clocks'[self] + interval]
                                                 ELSE /\ TRUE
                                                      /\ UNCHANGED max_requested
                                           /\ IF AllowMsgReordering
                                                 THEN /\ network' = [network EXCEPT ![i] = network[i] \union {(             [
                                                                                             type       |-> MsgHeartbeat,
                                                                                             from       |-> self,
                                                                                             epoch      |-> support_from[self][i].epoch,
                                                                                             expiration |-> max_requested'[self],
                                                                                             now        |-> clocks'[self]
                                                                                           ])}]
                                                 ELSE /\ network' = [network EXCEPT ![i] = Append(network[i], (             [
                                                                                             type       |-> MsgHeartbeat,
                                                                                             from       |-> self,
                                                                                             epoch      |-> support_from[self][i].epoch,
                                                                                             expiration |-> max_requested'[self],
                                                                                             now        |-> clocks'[self]
                                                                                           ]))]
                                    /\ pc' = [pc EXCEPT ![self] = "Loop"]
                                    /\ UNCHANGED << max_epoch, max_withdrawn, 
                                                    support_from, support_for, 
                                                    restarts, msg >>

Restart(self) == /\ pc[self] = "Restart"
                 /\ IF AllowClockRegressionOnRestart
                       THEN /\ clocks' = [clocks EXCEPT ![self] = Max({max_withdrawn[self], max_requested[self], clocks[self] - 1})]
                       ELSE /\ clocks' = [clocks EXCEPT ![self] = Max({max_withdrawn[self], max_requested[self], clocks[self]})]
                 /\ max_epoch' = [max_epoch EXCEPT ![self] = max_epoch[self] + 1]
                 /\ support_from' = [support_from EXCEPT ![self] = [j \in Nodes \ {self} |-> [epoch |-> max_epoch'[self], expiration |-> 0]]]
                 /\ restarts' = [restarts EXCEPT ![self] = restarts[self] + 1]
                 /\ pc' = [pc EXCEPT ![self] = "Loop"]
                 /\ UNCHANGED << max_requested, max_withdrawn, support_for, 
                                 network, msg >>

WithdrawSupport(self) == /\ pc[self] = "WithdrawSupport"
                         /\ \E expired \in CanInvalidateSupportForSet(self):
                              /\ support_for' = [support_for EXCEPT ![self][expired].epoch = support_for[self][expired].epoch + 1,
                                                                    ![self][expired].expiration = 0]
                              /\ IF (max_withdrawn[self]) < (clocks[self])
                                    THEN /\ max_withdrawn' = [max_withdrawn EXCEPT ![self] = clocks[self]]
                                    ELSE /\ TRUE
                                         /\ UNCHANGED max_withdrawn
                         /\ pc' = [pc EXCEPT ![self] = "Loop"]
                         /\ UNCHANGED << max_epoch, max_requested, 
                                         support_from, clocks, network, 
                                         restarts, msg >>

ReceiveHeartbeat(self) == /\ pc[self] = "ReceiveHeartbeat"
                          /\ IF support_for[self][msg[self].from].epoch = msg[self].epoch
                                THEN /\ IF (support_for[self][msg[self].from].expiration) < (msg[self].expiration)
                                           THEN /\ support_for' = [support_for EXCEPT ![self][msg[self].from].expiration = msg[self].expiration]
                                           ELSE /\ TRUE
                                                /\ UNCHANGED support_for
                                ELSE /\ IF support_for[self][msg[self].from].epoch < msg[self].epoch
                                           THEN /\ Assert(support_for[self][msg[self].from].expiration < msg[self].expiration, 
                                                          "Failure of assertion at line 220, column 13.")
                                                /\ Assert(support_for[self][msg[self].from].expiration < clocks[self], 
                                                          "Failure of assertion at line 224, column 13.")
                                                /\ support_for' = [support_for EXCEPT ![self][msg[self].from].epoch = msg[self].epoch,
                                                                                      ![self][msg[self].from].expiration = msg[self].expiration]
                                           ELSE /\ TRUE
                                                /\ UNCHANGED support_for
                          /\ IF AllowMsgReordering
                                THEN /\ network' = [network EXCEPT ![(msg[self].from)] = network[(msg[self].from)] \union {(             [
                                                                                           type       |-> MsgHeartbeatResp,
                                                                                           from       |-> self,
                                                                                           epoch      |-> support_for'[self][(msg[self].from)].epoch,
                                                                                           expiration |-> support_for'[self][(msg[self].from)].expiration,
                                                                                           now        |-> clocks[self]
                                                                                         ])}]
                                ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                           type       |-> MsgHeartbeatResp,
                                                                                           from       |-> self,
                                                                                           epoch      |-> support_for'[self][(msg[self].from)].epoch,
                                                                                           expiration |-> support_for'[self][(msg[self].from)].expiration,
                                                                                           now        |-> clocks[self]
                                                                                         ]))]
                          /\ pc' = [pc EXCEPT ![self] = "Loop"]
                          /\ UNCHANGED << max_epoch, max_requested, 
                                          max_withdrawn, support_from, clocks, 
                                          restarts, msg >>

ReceiveHeartbeatResp(self) == /\ pc[self] = "ReceiveHeartbeatResp"
                              /\ IF max_epoch[self] < msg[self].epoch
                                    THEN /\ max_epoch' = [max_epoch EXCEPT ![self] = msg[self].epoch]
                                    ELSE /\ TRUE
                                         /\ UNCHANGED max_epoch
                              /\ IF support_from[self][msg[self].from].epoch = msg[self].epoch
                                    THEN /\ IF (support_from[self][msg[self].from].expiration) < (msg[self].expiration)
                                               THEN /\ support_from' = [support_from EXCEPT ![self][msg[self].from].expiration = msg[self].expiration]
                                               ELSE /\ TRUE
                                                    /\ UNCHANGED support_from
                                    ELSE /\ IF support_from[self][msg[self].from].epoch < msg[self].epoch
                                               THEN /\ Assert(support_from[self][msg[self].from].epoch = msg[self].epoch - 1, 
                                                              "Failure of assertion at line 241, column 13.")
                                                    /\ Assert(msg[self].expiration = 0, 
                                                              "Failure of assertion at line 242, column 13.")
                                                    /\ Assert(support_from[self][msg[self].from].expiration <= clocks[self], 
                                                              "Failure of assertion at line 248, column 13.")
                                                    /\ support_from' = [support_from EXCEPT ![self][msg[self].from].epoch = msg[self].epoch,
                                                                                            ![self][msg[self].from].expiration = msg[self].expiration]
                                               ELSE /\ TRUE
                                                    /\ UNCHANGED support_from
                              /\ pc' = [pc EXCEPT ![self] = "Loop"]
                              /\ UNCHANGED << max_requested, max_withdrawn, 
                                              support_for, clocks, network, 
                                              restarts, msg >>

node(self) == Loop(self) \/ TickClock(self)
                 \/ TickClockAndSendHeartbeats(self) \/ Restart(self)
                 \/ WithdrawSupport(self) \/ ReceiveHeartbeat(self)
                 \/ ReceiveHeartbeatResp(self)

Next == (\E self \in Nodes: node(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 
====
