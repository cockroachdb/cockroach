----------------------------- MODULE StoreLiveness ----------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences

CONSTANTS Nodes, MaxClock, MaxRestarts, HeartbeatInterval
CONSTANTS MsgHeartbeat, MsgHeartbeatResp, AllowMsgReordering
ASSUME Cardinality(Nodes) > 0
ASSUME HeartbeatInterval < MaxClock

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
  current_epoch = [i \in Nodes |-> 1];
  for_self_by   = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 1, end_time |-> 0]]];
  by_self_for   = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 0, end_time |-> 0]]];
  clocks        = [i \in Nodes |-> 1];
  network       = [i \in Nodes |-> EmptyNetwork];

define
  \* Define Nodes as a symmetry set. Cuts runtime by Cardinality(Nodes)!
  Symmetry == Permutations(Nodes)

  \* If we allow message reordering, represent the network as a set. Otherwise,
  \* represent it as a sequence.
  EmptyNetwork == IF AllowMsgReordering THEN {} ELSE <<>>

  EpochValid(map, i, j) == map[i][j].end_time /= 0
  \* Has i ever received support from j for the current epoch?
  ForSelfByEpochValid(i, j) == EpochValid(for_self_by, i, j)
  \* Has i ever supported j for the current epoch?
  BySelfForEpochValid(i, j) == EpochValid(by_self_for, i, j)

  EpochSupportExpired(map, i, j, supporter_time) == map[i][j].end_time < supporter_time
  \* Is i's support from j (according to i's for_self_by map) expired (according to j's clock)?
  ForSelfByEpochSupportExpired(i, j) == EpochSupportExpired(for_self_by, i, j, clocks[j])
  \* Is i's support for j (according to i's by_self_for map) expired (according to i's clock)?
  BySelfForEpochSupportExpired(i, j) == EpochSupportExpired(by_self_for, i, j, clocks[i])

  \* Is support for i from j upheld?
  SupportUpheld(i, j) == for_self_by[i][j].epoch = by_self_for[j][i].epoch

  \* Can i withdraw support for j?
  CanInvlidateBySelfFor(i, j) == BySelfForEpochValid(i, j) /\ BySelfForEpochSupportExpired(i, j)
  CanInvlidateBySelfForSet(i) == {j \in Nodes \ {i}: CanInvlidateBySelfFor(i, j)}

  \* If we ever had support for the current i=>j epoch, then either support
  \* is still upheld or the support we have received had expired according to
  \* j's clock.
  DurableSupportInvariant ==
    \A i \in Nodes:
      \A j \in Nodes \ {i}:
        ForSelfByEpochValid(i, j) => 
          (SupportUpheld(i, j) \/ ForSelfByEpochSupportExpired(i, j))

  \* A node's current epoch leads its supported epoch by all other nodes.
  CurrentEpochLeadsSupportedEpochsInvariant ==
    \A i \in Nodes:
      \A j \in Nodes \ {i}:
        current_epoch[i] >= for_self_by[i][j].epoch
end define;

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
  send_msg(to, [
    type     |-> MsgHeartbeat,
    from     |-> self,
    epoch    |-> for_self_by[self][to].epoch,
    end_time |-> clocks[self] + HeartbeatInterval
  ]);
end macro

macro send_heartbeat_resp(to, ack)
begin
  send_msg(to, [
    type     |-> MsgHeartbeatResp,
    from     |-> self,
    epoch    |-> by_self_for[self][to].epoch,
    end_time |-> by_self_for[self][to].end_time,
    ack      |-> ack
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
end macro

macro restart()
begin
  current_epoch[self] := current_epoch[self];
  for_self_by[self]   := [j \in Nodes \ {self} |-> [epoch |-> current_epoch[self], end_time |-> 0]];
end macro

process node \in Nodes
variables
  restarts = 0;
  msg      = [type |-> FALSE];
  ack      = FALSE;
begin Loop:
  while TRUE do
    either
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
      await CanInvlidateBySelfForSet(self) /= {};
      WithdrawSupport:
        with expired \in CanInvlidateBySelfForSet(self) do
          by_self_for[self][expired].epoch    := by_self_for[self][expired].epoch + 1 ||
          by_self_for[self][expired].end_time := 0;
        end with;
    or
      await network[self] /= EmptyNetwork;
      recv_msg();

      if msg.type = MsgHeartbeat then
        ReceiveHeartbeat:
          if by_self_for[self][msg.from].epoch < msg.epoch then
            assert by_self_for[self][msg.from].end_time < msg.end_time;
            by_self_for[self][msg.from].epoch    := msg.epoch ||
            by_self_for[self][msg.from].end_time := msg.end_time;
            ack := TRUE;
          elsif by_self_for[self][msg.from].epoch = msg.epoch then
            if by_self_for[self][msg.from].end_time < msg.end_time then
              by_self_for[self][msg.from].end_time := msg.end_time;
            end if;
            ack := TRUE;
          else
            ack := FALSE;
          end if;

          send_heartbeat_resp(msg.from, ack);
        
      elsif msg.type = MsgHeartbeatResp then
        ReceiveHeartbeatResp:
          if msg.ack then
            if for_self_by[self][msg.from].epoch < msg.epoch then
              assert for_self_by[self][msg.from].end_time < msg.end_time;
              for_self_by[self][msg.from].epoch    := msg.epoch ||
              for_self_by[self][msg.from].end_time := msg.end_time;

            elsif for_self_by[self][msg.from].epoch = msg.epoch then
              if for_self_by[self][msg.from].end_time < msg.end_time then
                for_self_by[self][msg.from].end_time := msg.end_time;
              end if;
            end if;
          else
            if current_epoch[self] < msg.epoch then
              current_epoch[self] := msg.epoch;
            end if;
            if for_self_by[self][msg.from].epoch < msg.epoch then
              for_self_by[self][msg.from].epoch := current_epoch[self] ||
              for_self_by[self][msg.from].end_time := 0;
            end if
          end if;

      else
        assert FALSE;
      end if;
    end either;
  end while;    
end process;
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "96c1f7b1" /\ chksum(tla) = "75b17564")
VARIABLES current_epoch, for_self_by, by_self_for, clocks, network, pc

(* define statement *)
Symmetry == Permutations(Nodes)



EmptyNetwork == IF AllowMsgReordering THEN {} ELSE <<>>

EpochValid(map, i, j) == map[i][j].end_time /= 0

ForSelfByEpochValid(i, j) == EpochValid(for_self_by, i, j)

BySelfForEpochValid(i, j) == EpochValid(by_self_for, i, j)

EpochSupportExpired(map, i, j, supporter_time) == map[i][j].end_time < supporter_time

ForSelfByEpochSupportExpired(i, j) == EpochSupportExpired(for_self_by, i, j, clocks[j])

BySelfForEpochSupportExpired(i, j) == EpochSupportExpired(by_self_for, i, j, clocks[i])


SupportUpheld(i, j) == for_self_by[i][j].epoch = by_self_for[j][i].epoch


CanInvlidateBySelfFor(i, j) == BySelfForEpochValid(i, j) /\ BySelfForEpochSupportExpired(i, j)
CanInvlidateBySelfForSet(i) == {j \in Nodes \ {i}: CanInvlidateBySelfFor(i, j)}




DurableSupportInvariant ==
  \A i \in Nodes:
    \A j \in Nodes \ {i}:
      ForSelfByEpochValid(i, j) =>
        (SupportUpheld(i, j) \/ ForSelfByEpochSupportExpired(i, j))


CurrentEpochLeadsSupportedEpochsInvariant ==
  \A i \in Nodes:
    \A j \in Nodes \ {i}:
      current_epoch[i] >= for_self_by[i][j].epoch

VARIABLES restarts, msg, ack

vars == << current_epoch, for_self_by, by_self_for, clocks, network, pc, 
           restarts, msg, ack >>

ProcSet == (Nodes)

Init == (* Global variables *)
        /\ current_epoch = [i \in Nodes |-> 1]
        /\ for_self_by = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 1, end_time |-> 0]]]
        /\ by_self_for = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 0, end_time |-> 0]]]
        /\ clocks = [i \in Nodes |-> 1]
        /\ network = [i \in Nodes |-> EmptyNetwork]
        (* Process node *)
        /\ restarts = [self \in Nodes |-> 0]
        /\ msg = [self \in Nodes |-> [type |-> FALSE]]
        /\ ack = [self \in Nodes |-> FALSE]
        /\ pc = [self \in ProcSet |-> "Loop"]

Loop(self) == /\ pc[self] = "Loop"
              /\ \/ /\ clocks[self] < MaxClock
                    /\ pc' = [pc EXCEPT ![self] = "TickClockAndSendHeartbeats"]
                    /\ UNCHANGED <<network, msg>>
                 \/ /\ restarts[self] < MaxRestarts
                    /\ pc' = [pc EXCEPT ![self] = "Restart"]
                    /\ UNCHANGED <<network, msg>>
                 \/ /\ CanInvlidateBySelfForSet(self) /= {}
                    /\ pc' = [pc EXCEPT ![self] = "WithdrawSupport"]
                    /\ UNCHANGED <<network, msg>>
                 \/ /\ network[self] /= EmptyNetwork
                    /\ IF AllowMsgReordering
                          THEN /\ \E recv \in network[self]:
                                    /\ network' = [network EXCEPT ![self] = network[self] \ {recv}]
                                    /\ msg' = [msg EXCEPT ![self] = recv]
                          ELSE /\ msg' = [msg EXCEPT ![self] = Head(network[self])]
                               /\ network' = [network EXCEPT ![self] = Tail(network[self])]
                    /\ IF msg'[self].type = MsgHeartbeat
                          THEN /\ pc' = [pc EXCEPT ![self] = "ReceiveHeartbeat"]
                          ELSE /\ IF msg'[self].type = MsgHeartbeatResp
                                     THEN /\ pc' = [pc EXCEPT ![self] = "ReceiveHeartbeatResp"]
                                     ELSE /\ Assert(FALSE, 
                                                    "Failure of assertion at line 194, column 9.")
                                          /\ pc' = [pc EXCEPT ![self] = "Loop"]
              /\ UNCHANGED << current_epoch, for_self_by, by_self_for, clocks, 
                              restarts, ack >>

TickClockAndSendHeartbeats(self) == /\ pc[self] = "TickClockAndSendHeartbeats"
                                    /\ clocks' = [clocks EXCEPT ![self] = clocks[self] + 1]
                                    /\ \E i \in Nodes \ {self}:
                                         IF AllowMsgReordering
                                            THEN /\ network' = [network EXCEPT ![i] = network[i] \union {(             [
                                                                                        type     |-> MsgHeartbeat,
                                                                                        from     |-> self,
                                                                                        epoch    |-> for_self_by[self][i].epoch,
                                                                                        end_time |-> clocks'[self] + HeartbeatInterval
                                                                                      ])}]
                                            ELSE /\ network' = [network EXCEPT ![i] = Append(network[i], (             [
                                                                                        type     |-> MsgHeartbeat,
                                                                                        from     |-> self,
                                                                                        epoch    |-> for_self_by[self][i].epoch,
                                                                                        end_time |-> clocks'[self] + HeartbeatInterval
                                                                                      ]))]
                                    /\ pc' = [pc EXCEPT ![self] = "Loop"]
                                    /\ UNCHANGED << current_epoch, for_self_by, 
                                                    by_self_for, restarts, msg, 
                                                    ack >>

Restart(self) == /\ pc[self] = "Restart"
                 /\ current_epoch' = [current_epoch EXCEPT ![self] = current_epoch[self]]
                 /\ for_self_by' = [for_self_by EXCEPT ![self] = [j \in Nodes \ {self} |-> [epoch |-> current_epoch'[self], end_time |-> 0]]]
                 /\ restarts' = [restarts EXCEPT ![self] = restarts[self] + 1]
                 /\ pc' = [pc EXCEPT ![self] = "Loop"]
                 /\ UNCHANGED << by_self_for, clocks, network, msg, ack >>

WithdrawSupport(self) == /\ pc[self] = "WithdrawSupport"
                         /\ \E expired \in CanInvlidateBySelfForSet(self):
                              by_self_for' = [by_self_for EXCEPT ![self][expired].epoch = by_self_for[self][expired].epoch + 1,
                                                                 ![self][expired].end_time = 0]
                         /\ pc' = [pc EXCEPT ![self] = "Loop"]
                         /\ UNCHANGED << current_epoch, for_self_by, clocks, 
                                         network, restarts, msg, ack >>

ReceiveHeartbeat(self) == /\ pc[self] = "ReceiveHeartbeat"
                          /\ IF by_self_for[self][msg[self].from].epoch < msg[self].epoch
                                THEN /\ Assert(by_self_for[self][msg[self].from].end_time < msg[self].end_time, 
                                               "Failure of assertion at line 155, column 13.")
                                     /\ by_self_for' = [by_self_for EXCEPT ![self][msg[self].from].epoch = msg[self].epoch,
                                                                           ![self][msg[self].from].end_time = msg[self].end_time]
                                     /\ ack' = [ack EXCEPT ![self] = TRUE]
                                ELSE /\ IF by_self_for[self][msg[self].from].epoch = msg[self].epoch
                                           THEN /\ IF by_self_for[self][msg[self].from].end_time < msg[self].end_time
                                                      THEN /\ by_self_for' = [by_self_for EXCEPT ![self][msg[self].from].end_time = msg[self].end_time]
                                                      ELSE /\ TRUE
                                                           /\ UNCHANGED by_self_for
                                                /\ ack' = [ack EXCEPT ![self] = TRUE]
                                           ELSE /\ ack' = [ack EXCEPT ![self] = FALSE]
                                                /\ UNCHANGED by_self_for
                          /\ IF AllowMsgReordering
                                THEN /\ network' = [network EXCEPT ![(msg[self].from)] = network[(msg[self].from)] \union {(             [
                                                                                           type     |-> MsgHeartbeatResp,
                                                                                           from     |-> self,
                                                                                           epoch    |-> by_self_for'[self][(msg[self].from)].epoch,
                                                                                           end_time |-> by_self_for'[self][(msg[self].from)].end_time,
                                                                                           ack      |-> ack'[self]
                                                                                         ])}]
                                ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                           type     |-> MsgHeartbeatResp,
                                                                                           from     |-> self,
                                                                                           epoch    |-> by_self_for'[self][(msg[self].from)].epoch,
                                                                                           end_time |-> by_self_for'[self][(msg[self].from)].end_time,
                                                                                           ack      |-> ack'[self]
                                                                                         ]))]
                          /\ pc' = [pc EXCEPT ![self] = "Loop"]
                          /\ UNCHANGED << current_epoch, for_self_by, clocks, 
                                          restarts, msg >>

ReceiveHeartbeatResp(self) == /\ pc[self] = "ReceiveHeartbeatResp"
                              /\ IF msg[self].ack
                                    THEN /\ IF for_self_by[self][msg[self].from].epoch < msg[self].epoch
                                               THEN /\ Assert(for_self_by[self][msg[self].from].end_time < msg[self].end_time, 
                                                              "Failure of assertion at line 174, column 15.")
                                                    /\ for_self_by' = [for_self_by EXCEPT ![self][msg[self].from].epoch = msg[self].epoch,
                                                                                          ![self][msg[self].from].end_time = msg[self].end_time]
                                               ELSE /\ IF for_self_by[self][msg[self].from].epoch = msg[self].epoch
                                                          THEN /\ IF for_self_by[self][msg[self].from].end_time < msg[self].end_time
                                                                     THEN /\ for_self_by' = [for_self_by EXCEPT ![self][msg[self].from].end_time = msg[self].end_time]
                                                                     ELSE /\ TRUE
                                                                          /\ UNCHANGED for_self_by
                                                          ELSE /\ TRUE
                                                               /\ UNCHANGED for_self_by
                                         /\ UNCHANGED current_epoch
                                    ELSE /\ IF current_epoch[self] < msg[self].epoch
                                               THEN /\ current_epoch' = [current_epoch EXCEPT ![self] = msg[self].epoch]
                                               ELSE /\ TRUE
                                                    /\ UNCHANGED current_epoch
                                         /\ IF for_self_by[self][msg[self].from].epoch < msg[self].epoch
                                               THEN /\ for_self_by' = [for_self_by EXCEPT ![self][msg[self].from].epoch = current_epoch'[self],
                                                                                          ![self][msg[self].from].end_time = 0]
                                               ELSE /\ TRUE
                                                    /\ UNCHANGED for_self_by
                              /\ pc' = [pc EXCEPT ![self] = "Loop"]
                              /\ UNCHANGED << by_self_for, clocks, network, 
                                              restarts, msg, ack >>

node(self) == Loop(self) \/ TickClockAndSendHeartbeats(self)
                 \/ Restart(self) \/ WithdrawSupport(self)
                 \/ ReceiveHeartbeat(self) \/ ReceiveHeartbeatResp(self)

Next == (\E self \in Nodes: node(self))

Spec == Init /\ [][Next]_vars

\* END TRANSLATION 
====
