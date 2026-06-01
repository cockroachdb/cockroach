------------------------------ MODULE RaftLeader ------------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences, SequencesExt

CONSTANTS Nodes, Terms
ASSUME Cardinality(Nodes) > 0
ASSUME Terms > 0

CONSTANTS StateFollower, StateCandidate, StateLeader
CONSTANTS MsgVote, MsgVoteResp
CONSTANTS MsgFortifyLeader, MsgFortifyLeaderResp, MsgDeFortifyLeader
CONSTANTS MaxEpoch, MaxClock

(*****************************************************************************)
(* RaftLeader is a specification for the leader election protocol in Raft,   *)
(* as described in section 3.4 of the Raft thesis[^1].                       *)
(*                                                                           *)
(* This spec focuses on CockroachDB's extension of Raft for leader leases.   *)
(* In this extension, followers "fortify" the leader by providing store      *)
(* liveness support. While a quorum of followers is actively supporting the  *)
(* leader, no other node can win an election -- giving the leader an         *)
(* exclusive lease for serving reads without going through Raft. The store   *)
(* liveness fabric that underpins this mechanism is verified separately in   *)
(* StoreLiveness.tla.                                                        *)
(*                                                                           *)
(* What this spec models:                                                    *)
(*  - Store liveness support with expiration timestamps.                     *)
(*  - Raft leader election (MsgVote / MsgVoteResp) across multiple terms.    *)
(*  - Leader fortification (MsgFortifyLeader / MsgFortifyLeaderResp).        *)
(*  - Vote and campaign blocking while a node is fortifying a leader.        *)
(*  - Fortification expiration when store liveness support expires.          *)
(*  - Leader step-down constraint: a leader computes LeadSupportUntil,       *)
(*    freezes it, and delays stepping down until the lease expires.          *)
(*  - Broadcast de-fortification (MsgDeFortifyLeader) on step-down.          *)
(*                                                                           *)
(* Safety properties verified:                                               *)
(*  - LeaderExclusivity: at most one leader per term.                        *)
(*  - FortifiedLeaderSafety: if a leader has a quorum of followers whose     *)
(*    store liveness support is actively upheld, no other leader can exist.  *)
(*                                                                           *)
(* Not modeled. Some features are omitted because they are orthogonal to     *)
(* leader leases or verified elsewhere; others because they cause state      *)
(* space explosion and could be added in the future (if we figure out a way  *)
(* around the state space increase).                                         *)
(*                                                                           *)
(*  Orthogonal to leader leases:                                             *)
(*  - Raft log and log replication (MsgApp/MsgAppResp). MsgFortifyLeader     *)
(*    subsumes MsgApp's role of establishing leadership in this spec.        *)
(*  - Pre-Vote extension (MsgPreVote/MsgPreVoteResp). Fortification          *)
(*    treats MsgVote and MsgPreVote identically.                             *)
(*                                                                           *)
(*  Verified separately:                                                     *)
(*  - The store liveness protocol. Specifically, the support_from side of    *)
(*    store liveness and heartbeat exchange. Only the support_for side is    *)
(*    modeled here; the full protocol is verified in StoreLiveness.tla.      *)
(*                                                                           *)
(*  State space constraints:                                                 *)
(*  - Node restarts.                                                         *)
(*  - Configuration changes and joint consensus.                             *)
(*  - Message reordering (network is FIFO).                                  *)
(*                                                                           *)
(* [^1]: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf       *)
(*****************************************************************************)

(*--algorithm RaftLeader
variables
  \* Raft state.
  raft = [i \in Nodes |-> [
    state            |-> StateFollower,
    term             |-> 0,
    vote             |-> 0,
    lead             |-> 0,
    voteResps        |-> {},
    leadEpoch        |-> 0,
    fortifying       |-> {},
    steppingDown     |-> 0,
    leadSupportUntil |-> 0
  ]];
  \* In-flight messages, by destination.
  network = [i \in Nodes |-> EmptyNetwork];
  \* Abstract store liveness state. Models the ground truth of what support
  \* each node provides to others, with expiration timestamps. Support is
  \* active when expiration > clock. The full store liveness protocol is
  \* verified separately in StoreLiveness.tla.
  slSupportFor = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 0, expiration |-> 0]]];
  \* Global logical clock. Support expires when the clock advances past the
  \* expiration timestamp.
  clock = 1;

define
  \* Define Nodes as a symmetry set. Cuts runtime by 6x.
  Symmetry == Permutations(Nodes)

  EmptyNetwork == <<>>

  \*******************************************\
  \* Define leader fortification predicates *\
  \*******************************************\

  \* A node is actively supporting a fortified leader if it has a non-zero
  \* leadEpoch, knows its leader, and the store liveness ground truth confirms
  \* support is still active at the fortification epoch. Self-support (when the
  \* node IS the leader) is always considered active.
  SupportingFortifiedLeader(n) ==
    /\ raft[n].leadEpoch /= 0
    /\ raft[n].lead /= 0
    /\ IF raft[n].lead = n
       THEN TRUE
       ELSE slSupportFor[n][raft[n].lead].expiration > clock
            /\ slSupportFor[n][raft[n].lead].epoch = raft[n].leadEpoch

  SetMax(S) == CHOOSE x \in S : \A y \in S : y <= x

  \* Compute the leader's LeadSupportUntil: the maximum timestamp T such
  \* that a quorum of nodes has support expiration > T. This is the latest
  \* time until which the leader's lease is guaranteed.
  \*
  \* Simplified for 3 nodes with quorum=2: the leader counts as self-
  \* supported (infinite), so it needs 1 follower with active support.
  \* The result is the max expiration among fortified followers.
  \*
  \* For a general N-node cluster, this would be computed as follows
  \* (matching MajorityConfig.LeadSupportExpiration in the real code):
  \*
  \*   ComputeLeadSupportUntil(leader) ==
  \*     LET support == [n \in Nodes |->
  \*           IF n = leader THEN MaxClock + 1
  \*           ELSE IF n \in raft[leader].fortifying
  \*                THEN slSupportFor[n][leader].expiration
  \*                ELSE 0]
  \*     IN CHOOSE t \in 0..MaxClock :
  \*          /\ Cardinality({n \in Nodes : support[n] > t}) * 2
  \*               > Cardinality(Nodes)
  \*          /\ \A t2 \in (t+1)..MaxClock :
  \*               ~(Cardinality({n \in Nodes : support[n] > t2}) * 2
  \*                   > Cardinality(Nodes))
  ComputeLeadSupportUntil(leader) ==
    LET supporters == {n \in raft[leader].fortifying \ {leader} :
                         slSupportFor[n][leader].expiration > clock}
        exps == {slSupportFor[n][leader].expiration : n \in supporters}
    IN IF exps = {} THEN 0 ELSE SetMax(exps)

  RaftQuorum    == {i \in SUBSET(Nodes) : Cardinality(i) * 2 > Cardinality(Nodes)}
  CanCampaign   == [i \in Nodes |->
    raft[i].term < Terms /\ raft[i].state = StateFollower
    /\ ~SupportingFortifiedLeader(i)]
  CanReceiveMsg == [i \in Nodes |-> network[i] /= EmptyNetwork]

  \*****************************************************\
  \* Define type invariants as a form of type checking *\
  \*****************************************************\

  RaftStates   == {StateFollower, StateCandidate, StateLeader}
  RaftMsgTypes == {MsgVote, MsgVoteResp, MsgFortifyLeader, MsgFortifyLeaderResp,
                   MsgDeFortifyLeader}
  RaftTerms    == 0..Terms
  RaftVotes    == Nodes \union {0}
  RaftLead     == Nodes \union {0}

  RaftStateInvariant ==
    /\ \A i \in Nodes :
      /\ raft[i].state \in RaftStates
      /\ raft[i].term  \in RaftTerms
      /\ raft[i].vote  \in RaftVotes
      /\ raft[i].lead  \in RaftLead

  RaftMsgInvariant ==
    /\ \A i \in Nodes :
      \A msg \in ToSet(network[i]):
        /\ msg.type \in RaftMsgTypes
        /\ msg.from \in Nodes
        /\ msg.term \in RaftTerms

  SLTypeInvariant ==
    /\ clock \in 1..MaxClock
    /\ \A i \in Nodes:
      \A j \in Nodes \ {i}:
        /\ slSupportFor[i][j].epoch \in 0..MaxEpoch
        /\ slSupportFor[i][j].expiration \in 0..MaxClock

  TypeInvariants ==
    /\ RaftStateInvariant
    /\ RaftMsgInvariant
    /\ SLTypeInvariant

  \*************************************************\
  \* Define safety invariants to check correctness *\
  \*************************************************\

  \* There can only be one leader for a given term.
  LeaderExclusivity == \A i, j \in Nodes :
    raft[i].term = raft[j].term /\ raft[i].lead /= 0 /\ raft[j].lead /= 0 =>
    raft[i].lead = raft[j].lead

  \* A follower is actively fortifying a specific leader if its leadEpoch is set,
  \* it knows this leader, and store liveness confirms support is still live.
  IsActivelyFortifying(leader, n) ==
    IF n = leader
    THEN raft[leader].leadEpoch /= 0
    ELSE /\ raft[n].lead = leader
         /\ raft[n].leadEpoch /= 0
         /\ slSupportFor[n][leader].expiration > clock
         /\ slSupportFor[n][leader].epoch = raft[n].leadEpoch

  HasActiveFortificationQuorum(leader) ==
    {n \in Nodes : IsActivelyFortifying(leader, n)} \in RaftQuorum

  \* If a leader has a quorum of nodes whose support is actively upheld, no
  \* other node can simultaneously be a leader (at any term).
  FortifiedLeaderSafety == \A i \in Nodes :
    raft[i].state = StateLeader /\ HasActiveFortificationQuorum(i) =>
      ~\E j \in Nodes \ {i} : raft[j].state = StateLeader

end define;

\*************************************\
\* Macros for Raft state transitions *\
\*************************************\

macro become_follower(term, lead)
begin
  if raft[self].term /= term then
    raft[self].state            := StateFollower ||
    raft[self].term             := term          ||
    raft[self].vote             := 0             ||
    raft[self].lead             := lead          ||
    raft[self].leadEpoch        := 0             ||
    raft[self].fortifying       := {}            ||
    raft[self].steppingDown     := 0             ||
    raft[self].leadSupportUntil := 0;
  else
    raft[self].state := StateFollower ||
    raft[self].lead  := lead;
  end if;
end macro;

macro become_candidate()
begin
  raft[self].state            := StateCandidate      ||
  raft[self].term             := raft[self].term + 1 ||
  raft[self].vote             := self                ||
  raft[self].lead             := 0                   ||
  raft[self].voteResps        := {self}              ||
  raft[self].leadEpoch        := 0                   ||
  raft[self].steppingDown     := 0                   ||
  raft[self].leadSupportUntil := 0;
end macro;

macro become_leader()
begin
  raft[self].state            := StateLeader ||
  raft[self].lead             := self        ||
  raft[self].leadEpoch        := 1           ||
  raft[self].fortifying       := {self}      ||
  raft[self].steppingDown     := 0           ||
  raft[self].leadSupportUntil := 0;
end macro;

\************************************\
\* Macros for sending Raft messages *\
\************************************\

macro send_msg(to, msg)
begin
  network[to] := Append(network[to], msg);
end macro

macro send_msg_vote(to)
begin
  send_msg(to, [
    type   |-> MsgVote,
    from   |-> self,
    term   |-> raft[self].term
  ]);
end macro

macro send_msg_vote_resp(to, reject)
begin
  send_msg(to, [
    type   |-> MsgVoteResp,
    from   |-> self,
    term   |-> raft[self].term,
    reject |-> reject
  ]);
end macro

macro send_msg_fortify_leader(to)
begin
  send_msg(to, [
    type |-> MsgFortifyLeader,
    from |-> self,
    term |-> raft[self].term
  ]);
end macro

macro send_msg_fortify_leader_resp(to, epoch, reject)
begin
  send_msg(to, [
    type   |-> MsgFortifyLeaderResp,
    from   |-> self,
    term   |-> raft[self].term,
    epoch  |-> epoch,
    reject |-> reject
  ]);
end macro

macro send_msg_defortify_leader(to)
begin
  send_msg(to, [
    type |-> MsgDeFortifyLeader,
    from |-> self,
    term |-> raft[self].term
  ]);
end macro

macro prep_broadcast()
begin
  toSend := SetToSeq(Nodes \ {self});
end macro;

macro recv_msg()
begin
  msg := Head(network[self]);
  network[self] := Tail(network[self]);
end macro

\****************************\
\* Replicas in a Raft group *\
\****************************\

fair process node \in Nodes
variables
  canVote = FALSE;
  toSend  = <<>>;
  msg     = [type |-> 0];
begin TickLoop:
  while TRUE do
    either
      await CanCampaign[self];
      become_candidate();
      prep_broadcast();
      BroadcastVotes:
        while toSend /= <<>> do
          send_msg_vote(Head(toSend));
          toSend := Tail(toSend);
        end while;
    or
      \* Store Liveness: grant support to another node. Sets the expiration
      \* to clock + 1 and advances the epoch. Support is active while
      \* clock < expiration. Requires expired or never-granted support and
      \* room for a valid expiration.
      with target \in {j \in Nodes \ {self} :
            slSupportFor[self][j].expiration <= clock
            /\ slSupportFor[self][j].epoch < MaxEpoch
            /\ clock < MaxClock} do
        slSupportFor[self][target].epoch      := slSupportFor[self][target].epoch + 1 ||
        slSupportFor[self][target].expiration := clock + 1;
      end with;
    or
      \* Store Liveness: extend support. Models a heartbeat that forwards
      \* the expiration time while support is still active (same epoch).
      with target \in {j \in Nodes \ {self} :
            slSupportFor[self][j].expiration > clock
            /\ clock < MaxClock} do
        slSupportFor[self][target].expiration := clock + 1;
      end with;
    or
      \* Time passes: advance the global clock. Support expires naturally
      \* when the clock passes the expiration timestamp.
      await clock < MaxClock;
      clock := clock + 1;
    or
      \* Leader sends MsgFortifyLeader to a follower it hasn't sent to yet.
      await raft[self].state = StateLeader;
      with target \in Nodes \ raft[self].fortifying do
        send_msg_fortify_leader(target);
        raft[self].fortifying := raft[self].fortifying \union {target};
      end with;
    or
      \* Fortification expired: if support for the leader has expired
      \* (clock passed expiration) or the epoch no longer matches, clear
      \* leadEpoch so the node can vote again.
      await raft[self].leadEpoch /= 0 /\ raft[self].lead /= 0
            /\ raft[self].lead /= self
            /\ ~(slSupportFor[self][raft[self].lead].expiration > clock
                 /\ slSupportFor[self][raft[self].lead].epoch = raft[self].leadEpoch);
      raft[self].leadEpoch := 0;
    or
      \* Leader completes step-down after the frozen lease has expired.
      \* Broadcasts MsgDeFortifyLeader to all followers and then becomes
      \* a follower, matching the real code's becomeFollower+bcastDeFortify.
      await raft[self].steppingDown > 0
            /\ clock >= raft[self].leadSupportUntil;
      become_follower(raft[self].steppingDown, 0);
      prep_broadcast();
      BroadcastDeFortify:
        while toSend /= <<>> do
          send_msg_defortify_leader(Head(toSend));
          toSend := Tail(toSend);
        end while;
    or
      await CanReceiveMsg[self];
      recv_msg();

      \* If fortifying a leader, ignore vote requests regardless of term.
      if msg.type = MsgVote /\ SupportingFortifiedLeader(self) then
        goto TickLoop;
      \* Leader step-down constraint: when a higher-term message arrives,
      \* check if the lease has already expired. If so, step down
      \* immediately. Otherwise, freeze LeadSupportUntil and begin
      \* stepping down; the leader will step down once the lease expires.
      elsif msg.term > raft[self].term /\ raft[self].state = StateLeader
            /\ raft[self].steppingDown = 0
            /\ clock >= ComputeLeadSupportUntil(self) then
        become_follower(msg.term, 0);
      elsif msg.term > raft[self].term /\ raft[self].state = StateLeader
            /\ raft[self].steppingDown = 0 then
        raft[self].steppingDown     := msg.term ||
        raft[self].leadSupportUntil := ComputeLeadSupportUntil(self);
        goto TickLoop;
      elsif msg.term > raft[self].term /\ raft[self].state = StateLeader
            /\ raft[self].steppingDown > 0 then
        goto TickLoop;
      end if;

      HandleTerm:
      if msg.term > raft[self].term then
          if msg.type = MsgVote /\ raft[self].lead /= 0 then
          become_follower(msg.term, 0);
        else
          if msg.type = MsgFortifyLeader then
            become_follower(msg.term, msg.from);
          else
            become_follower(msg.term, 0);
          end if;
        end if;
      elsif msg.term < raft[self].term then
        goto TickLoop;
      end if;

      RecvMessage:
        if msg.type = MsgVote then
          \* We can vote if this is a repeat of a vote we've already cast,
          \* or we haven't voted and we don't think there's a leader yet.
          canVote := raft[self].vote = msg.from
            \/ (raft[self].vote = 0 /\ raft[self].lead = 0);

          if canVote then
            send_msg_vote_resp(msg.from, FALSE);
            raft[self].vote := msg.from;
          else
            send_msg_vote_resp(msg.from, TRUE);
          end if;

        elsif msg.type = MsgVoteResp then
          if ~msg.reject /\ raft[self].state = StateCandidate then
            raft[self].voteResps := raft[self].voteResps \union {msg.from};
            if raft[self].voteResps \in RaftQuorum then
              WonElection:
                become_leader();
                prep_broadcast();
                BroadcastFortify:
                  while toSend /= <<>> do
                    send_msg_fortify_leader(Head(toSend));
                    raft[self].fortifying := raft[self].fortifying \union {Head(toSend)};
                    toSend := Tail(toSend);
                  end while;
            end if;
          end if;

        elsif msg.type = MsgFortifyLeader then
          \* Follower handles fortification request from its leader.
          if raft[self].state = StateFollower /\ raft[self].lead = msg.from then
            if slSupportFor[self][msg.from].expiration > clock then
              raft[self].leadEpoch := slSupportFor[self][msg.from].epoch;
              send_msg_fortify_leader_resp(msg.from, slSupportFor[self][msg.from].epoch, FALSE);
            else
              send_msg_fortify_leader_resp(msg.from, 0, TRUE);
            end if;
          end if;

        elsif msg.type = MsgFortifyLeaderResp then
          \* Consumed but not tracked in this simplified model.
          skip;

        elsif msg.type = MsgDeFortifyLeader then
          \* Follower clears fortification if the message is from its leader.
          if raft[self].lead = msg.from then
            raft[self].leadEpoch := 0;
          end if;

        else
          assert FALSE;
        end if;
    end either;
  end while;
end process;
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "72a166a6" /\ chksum(tla) = "f988a190")
VARIABLES pc, raft, network, slSupportFor, clock

(* define statement *)
Symmetry == Permutations(Nodes)

EmptyNetwork == <<>>









SupportingFortifiedLeader(n) ==
  /\ raft[n].leadEpoch /= 0
  /\ raft[n].lead /= 0
  /\ IF raft[n].lead = n
     THEN TRUE
     ELSE slSupportFor[n][raft[n].lead].expiration > clock
          /\ slSupportFor[n][raft[n].lead].epoch = raft[n].leadEpoch

SetMax(S) == CHOOSE x \in S : \A y \in S : y <= x
























ComputeLeadSupportUntil(leader) ==
  LET supporters == {n \in raft[leader].fortifying \ {leader} :
                       slSupportFor[n][leader].expiration > clock}
      exps == {slSupportFor[n][leader].expiration : n \in supporters}
  IN IF exps = {} THEN 0 ELSE SetMax(exps)

RaftQuorum    == {i \in SUBSET(Nodes) : Cardinality(i) * 2 > Cardinality(Nodes)}
CanCampaign   == [i \in Nodes |->
  raft[i].term < Terms /\ raft[i].state = StateFollower
  /\ ~SupportingFortifiedLeader(i)]
CanReceiveMsg == [i \in Nodes |-> network[i] /= EmptyNetwork]





RaftStates   == {StateFollower, StateCandidate, StateLeader}
RaftMsgTypes == {MsgVote, MsgVoteResp, MsgFortifyLeader, MsgFortifyLeaderResp,
                 MsgDeFortifyLeader}
RaftTerms    == 0..Terms
RaftVotes    == Nodes \union {0}
RaftLead     == Nodes \union {0}

RaftStateInvariant ==
  /\ \A i \in Nodes :
    /\ raft[i].state \in RaftStates
    /\ raft[i].term  \in RaftTerms
    /\ raft[i].vote  \in RaftVotes
    /\ raft[i].lead  \in RaftLead

RaftMsgInvariant ==
  /\ \A i \in Nodes :
    \A msg \in ToSet(network[i]):
      /\ msg.type \in RaftMsgTypes
      /\ msg.from \in Nodes
      /\ msg.term \in RaftTerms

SLTypeInvariant ==
  /\ clock \in 1..MaxClock
  /\ \A i \in Nodes:
    \A j \in Nodes \ {i}:
      /\ slSupportFor[i][j].epoch \in 0..MaxEpoch
      /\ slSupportFor[i][j].expiration \in 0..MaxClock

TypeInvariants ==
  /\ RaftStateInvariant
  /\ RaftMsgInvariant
  /\ SLTypeInvariant






LeaderExclusivity == \A i, j \in Nodes :
  raft[i].term = raft[j].term /\ raft[i].lead /= 0 /\ raft[j].lead /= 0 =>
  raft[i].lead = raft[j].lead



IsActivelyFortifying(leader, n) ==
  IF n = leader
  THEN raft[leader].leadEpoch /= 0
  ELSE /\ raft[n].lead = leader
       /\ raft[n].leadEpoch /= 0
       /\ slSupportFor[n][leader].expiration > clock
       /\ slSupportFor[n][leader].epoch = raft[n].leadEpoch

HasActiveFortificationQuorum(leader) ==
  {n \in Nodes : IsActivelyFortifying(leader, n)} \in RaftQuorum



FortifiedLeaderSafety == \A i \in Nodes :
  raft[i].state = StateLeader /\ HasActiveFortificationQuorum(i) =>
    ~\E j \in Nodes \ {i} : raft[j].state = StateLeader

VARIABLES canVote, toSend, msg

vars == << pc, raft, network, slSupportFor, clock, canVote, toSend, msg >>

ProcSet == (Nodes)

Init == (* Global variables *)
        /\ raft =        [i \in Nodes |-> [
                    state            |-> StateFollower,
                    term             |-> 0,
                    vote             |-> 0,
                    lead             |-> 0,
                    voteResps        |-> {},
                    leadEpoch        |-> 0,
                    fortifying       |-> {},
                    steppingDown     |-> 0,
                    leadSupportUntil |-> 0
                  ]]
        /\ network = [i \in Nodes |-> EmptyNetwork]
        /\ slSupportFor = [i \in Nodes |-> [j \in Nodes \ {i} |-> [epoch |-> 0, expiration |-> 0]]]
        /\ clock = 1
        (* Process node *)
        /\ canVote = [self \in Nodes |-> FALSE]
        /\ toSend = [self \in Nodes |-> <<>>]
        /\ msg = [self \in Nodes |-> [type |-> 0]]
        /\ pc = [self \in ProcSet |-> "TickLoop"]

TickLoop(self) == /\ pc[self] = "TickLoop"
                  /\ \/ /\ CanCampaign[self]
                        /\ raft' = [raft EXCEPT ![self].state = StateCandidate,
                                                ![self].term = raft[self].term + 1,
                                                ![self].vote = self,
                                                ![self].lead = 0,
                                                ![self].voteResps = {self},
                                                ![self].leadEpoch = 0,
                                                ![self].steppingDown = 0,
                                                ![self].leadSupportUntil = 0]
                        /\ toSend' = [toSend EXCEPT ![self] = SetToSeq(Nodes \ {self})]
                        /\ pc' = [pc EXCEPT ![self] = "BroadcastVotes"]
                        /\ UNCHANGED <<network, slSupportFor, clock, msg>>
                     \/ /\ \E target \in           {j \in Nodes \ {self} :
                                         slSupportFor[self][j].expiration <= clock
                                         /\ slSupportFor[self][j].epoch < MaxEpoch
                                         /\ clock < MaxClock}:
                             slSupportFor' = [slSupportFor EXCEPT ![self][target].epoch = slSupportFor[self][target].epoch + 1,
                                                                  ![self][target].expiration = clock + 1]
                        /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                        /\ UNCHANGED <<raft, network, clock, toSend, msg>>
                     \/ /\ \E target \in           {j \in Nodes \ {self} :
                                         slSupportFor[self][j].expiration > clock
                                         /\ clock < MaxClock}:
                             slSupportFor' = [slSupportFor EXCEPT ![self][target].expiration = clock + 1]
                        /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                        /\ UNCHANGED <<raft, network, clock, toSend, msg>>
                     \/ /\ clock < MaxClock
                        /\ clock' = clock + 1
                        /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                        /\ UNCHANGED <<raft, network, slSupportFor, toSend, msg>>
                     \/ /\ raft[self].state = StateLeader
                        /\ \E target \in Nodes \ raft[self].fortifying:
                             /\ network' = [network EXCEPT ![target] = Append(network[target], (             [
                                                                         type |-> MsgFortifyLeader,
                                                                         from |-> self,
                                                                         term |-> raft[self].term
                                                                       ]))]
                             /\ raft' = [raft EXCEPT ![self].fortifying = raft[self].fortifying \union {target}]
                        /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                        /\ UNCHANGED <<slSupportFor, clock, toSend, msg>>
                     \/ /\ raft[self].leadEpoch /= 0 /\ raft[self].lead /= 0
                           /\ raft[self].lead /= self
                           /\ ~(slSupportFor[self][raft[self].lead].expiration > clock
                                /\ slSupportFor[self][raft[self].lead].epoch = raft[self].leadEpoch)
                        /\ raft' = [raft EXCEPT ![self].leadEpoch = 0]
                        /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                        /\ UNCHANGED <<network, slSupportFor, clock, toSend, msg>>
                     \/ /\ raft[self].steppingDown > 0
                           /\ clock >= raft[self].leadSupportUntil
                        /\ IF raft[self].term /= (raft[self].steppingDown)
                              THEN /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                           ![self].term = raft[self].steppingDown,
                                                           ![self].vote = 0,
                                                           ![self].lead = 0,
                                                           ![self].leadEpoch = 0,
                                                           ![self].fortifying = {},
                                                           ![self].steppingDown = 0,
                                                           ![self].leadSupportUntil = 0]
                              ELSE /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                           ![self].lead = 0]
                        /\ toSend' = [toSend EXCEPT ![self] = SetToSeq(Nodes \ {self})]
                        /\ pc' = [pc EXCEPT ![self] = "BroadcastDeFortify"]
                        /\ UNCHANGED <<network, slSupportFor, clock, msg>>
                     \/ /\ CanReceiveMsg[self]
                        /\ msg' = [msg EXCEPT ![self] = Head(network[self])]
                        /\ network' = [network EXCEPT ![self] = Tail(network[self])]
                        /\ IF msg'[self].type = MsgVote /\ SupportingFortifiedLeader(self)
                              THEN /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                   /\ raft' = raft
                              ELSE /\ IF msg'[self].term > raft[self].term /\ raft[self].state = StateLeader
                                         /\ raft[self].steppingDown = 0
                                         /\ clock >= ComputeLeadSupportUntil(self)
                                         THEN /\ IF raft[self].term /= (msg'[self].term)
                                                    THEN /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                 ![self].term = msg'[self].term,
                                                                                 ![self].vote = 0,
                                                                                 ![self].lead = 0,
                                                                                 ![self].leadEpoch = 0,
                                                                                 ![self].fortifying = {},
                                                                                 ![self].steppingDown = 0,
                                                                                 ![self].leadSupportUntil = 0]
                                                    ELSE /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                 ![self].lead = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "HandleTerm"]
                                         ELSE /\ IF msg'[self].term > raft[self].term /\ raft[self].state = StateLeader
                                                    /\ raft[self].steppingDown = 0
                                                    THEN /\ raft' = [raft EXCEPT ![self].steppingDown = msg'[self].term,
                                                                                 ![self].leadSupportUntil = ComputeLeadSupportUntil(self)]
                                                         /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                    ELSE /\ IF msg'[self].term > raft[self].term /\ raft[self].state = StateLeader
                                                               /\ raft[self].steppingDown > 0
                                                               THEN /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                               ELSE /\ pc' = [pc EXCEPT ![self] = "HandleTerm"]
                                                         /\ raft' = raft
                        /\ UNCHANGED <<slSupportFor, clock, toSend>>
                  /\ UNCHANGED canVote

BroadcastVotes(self) == /\ pc[self] = "BroadcastVotes"
                        /\ IF toSend[self] /= <<>>
                              THEN /\ network' = [network EXCEPT ![(Head(toSend[self]))] = Append(network[(Head(toSend[self]))], (             [
                                                                                             type   |-> MsgVote,
                                                                                             from   |-> self,
                                                                                             term   |-> raft[self].term
                                                                                           ]))]
                                   /\ toSend' = [toSend EXCEPT ![self] = Tail(toSend[self])]
                                   /\ pc' = [pc EXCEPT ![self] = "BroadcastVotes"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                   /\ UNCHANGED << network, toSend >>
                        /\ UNCHANGED << raft, slSupportFor, clock, canVote, 
                                        msg >>

BroadcastDeFortify(self) == /\ pc[self] = "BroadcastDeFortify"
                            /\ IF toSend[self] /= <<>>
                                  THEN /\ network' = [network EXCEPT ![(Head(toSend[self]))] = Append(network[(Head(toSend[self]))], (             [
                                                                                                 type |-> MsgDeFortifyLeader,
                                                                                                 from |-> self,
                                                                                                 term |-> raft[self].term
                                                                                               ]))]
                                       /\ toSend' = [toSend EXCEPT ![self] = Tail(toSend[self])]
                                       /\ pc' = [pc EXCEPT ![self] = "BroadcastDeFortify"]
                                  ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                       /\ UNCHANGED << network, toSend >>
                            /\ UNCHANGED << raft, slSupportFor, clock, canVote, 
                                            msg >>

HandleTerm(self) == /\ pc[self] = "HandleTerm"
                    /\ IF msg[self].term > raft[self].term
                          THEN /\ IF msg[self].type = MsgVote /\ raft[self].lead /= 0
                                     THEN /\ IF raft[self].term /= (msg[self].term)
                                                THEN /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                             ![self].term = msg[self].term,
                                                                             ![self].vote = 0,
                                                                             ![self].lead = 0,
                                                                             ![self].leadEpoch = 0,
                                                                             ![self].fortifying = {},
                                                                             ![self].steppingDown = 0,
                                                                             ![self].leadSupportUntil = 0]
                                                ELSE /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                             ![self].lead = 0]
                                     ELSE /\ IF msg[self].type = MsgFortifyLeader
                                                THEN /\ IF raft[self].term /= (msg[self].term)
                                                           THEN /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                        ![self].term = msg[self].term,
                                                                                        ![self].vote = 0,
                                                                                        ![self].lead = msg[self].from,
                                                                                        ![self].leadEpoch = 0,
                                                                                        ![self].fortifying = {},
                                                                                        ![self].steppingDown = 0,
                                                                                        ![self].leadSupportUntil = 0]
                                                           ELSE /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                        ![self].lead = msg[self].from]
                                                ELSE /\ IF raft[self].term /= (msg[self].term)
                                                           THEN /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                        ![self].term = msg[self].term,
                                                                                        ![self].vote = 0,
                                                                                        ![self].lead = 0,
                                                                                        ![self].leadEpoch = 0,
                                                                                        ![self].fortifying = {},
                                                                                        ![self].steppingDown = 0,
                                                                                        ![self].leadSupportUntil = 0]
                                                           ELSE /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                        ![self].lead = 0]
                               /\ pc' = [pc EXCEPT ![self] = "RecvMessage"]
                          ELSE /\ IF msg[self].term < raft[self].term
                                     THEN /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                     ELSE /\ pc' = [pc EXCEPT ![self] = "RecvMessage"]
                               /\ raft' = raft
                    /\ UNCHANGED << network, slSupportFor, clock, canVote, 
                                    toSend, msg >>

RecvMessage(self) == /\ pc[self] = "RecvMessage"
                     /\ IF msg[self].type = MsgVote
                           THEN /\ canVote' = [canVote EXCEPT ![self] =          raft[self].vote = msg[self].from
                                                                        \/ (raft[self].vote = 0 /\ raft[self].lead = 0)]
                                /\ IF canVote'[self]
                                      THEN /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                 type   |-> MsgVoteResp,
                                                                                                 from   |-> self,
                                                                                                 term   |-> raft[self].term,
                                                                                                 reject |-> FALSE
                                                                                               ]))]
                                           /\ raft' = [raft EXCEPT ![self].vote = msg[self].from]
                                      ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                 type   |-> MsgVoteResp,
                                                                                                 from   |-> self,
                                                                                                 term   |-> raft[self].term,
                                                                                                 reject |-> TRUE
                                                                                               ]))]
                                           /\ raft' = raft
                                /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                           ELSE /\ IF msg[self].type = MsgVoteResp
                                      THEN /\ IF ~msg[self].reject /\ raft[self].state = StateCandidate
                                                 THEN /\ raft' = [raft EXCEPT ![self].voteResps = raft[self].voteResps \union {msg[self].from}]
                                                      /\ IF raft'[self].voteResps \in RaftQuorum
                                                            THEN /\ pc' = [pc EXCEPT ![self] = "WonElection"]
                                                            ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                 ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                      /\ raft' = raft
                                           /\ UNCHANGED network
                                      ELSE /\ IF msg[self].type = MsgFortifyLeader
                                                 THEN /\ IF raft[self].state = StateFollower /\ raft[self].lead = msg[self].from
                                                            THEN /\ IF slSupportFor[self][msg[self].from].expiration > clock
                                                                       THEN /\ raft' = [raft EXCEPT ![self].leadEpoch = slSupportFor[self][msg[self].from].epoch]
                                                                            /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                                                  type   |-> MsgFortifyLeaderResp,
                                                                                                                                  from   |-> self,
                                                                                                                                  term   |-> raft'[self].term,
                                                                                                                                  epoch  |-> (slSupportFor[self][msg[self].from].epoch),
                                                                                                                                  reject |-> FALSE
                                                                                                                                ]))]
                                                                       ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                                                  type   |-> MsgFortifyLeaderResp,
                                                                                                                                  from   |-> self,
                                                                                                                                  term   |-> raft[self].term,
                                                                                                                                  epoch  |-> 0,
                                                                                                                                  reject |-> TRUE
                                                                                                                                ]))]
                                                                            /\ raft' = raft
                                                            ELSE /\ TRUE
                                                                 /\ UNCHANGED << raft, 
                                                                                 network >>
                                                 ELSE /\ IF msg[self].type = MsgFortifyLeaderResp
                                                            THEN /\ TRUE
                                                                 /\ raft' = raft
                                                            ELSE /\ IF msg[self].type = MsgDeFortifyLeader
                                                                       THEN /\ IF raft[self].lead = msg[self].from
                                                                                  THEN /\ raft' = [raft EXCEPT ![self].leadEpoch = 0]
                                                                                  ELSE /\ TRUE
                                                                                       /\ raft' = raft
                                                                       ELSE /\ Assert(FALSE, 
                                                                                      "Failure of assertion at line 492, column 11.")
                                                                            /\ raft' = raft
                                                      /\ UNCHANGED network
                                           /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                /\ UNCHANGED canVote
                     /\ UNCHANGED << slSupportFor, clock, toSend, msg >>

WonElection(self) == /\ pc[self] = "WonElection"
                     /\ raft' = [raft EXCEPT ![self].state = StateLeader,
                                             ![self].lead = self,
                                             ![self].leadEpoch = 1,
                                             ![self].fortifying = {self},
                                             ![self].steppingDown = 0,
                                             ![self].leadSupportUntil = 0]
                     /\ toSend' = [toSend EXCEPT ![self] = SetToSeq(Nodes \ {self})]
                     /\ pc' = [pc EXCEPT ![self] = "BroadcastFortify"]
                     /\ UNCHANGED << network, slSupportFor, clock, canVote, 
                                     msg >>

BroadcastFortify(self) == /\ pc[self] = "BroadcastFortify"
                          /\ IF toSend[self] /= <<>>
                                THEN /\ network' = [network EXCEPT ![(Head(toSend[self]))] = Append(network[(Head(toSend[self]))], (             [
                                                                                               type |-> MsgFortifyLeader,
                                                                                               from |-> self,
                                                                                               term |-> raft[self].term
                                                                                             ]))]
                                     /\ raft' = [raft EXCEPT ![self].fortifying = raft[self].fortifying \union {Head(toSend[self])}]
                                     /\ toSend' = [toSend EXCEPT ![self] = Tail(toSend[self])]
                                     /\ pc' = [pc EXCEPT ![self] = "BroadcastFortify"]
                                ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                     /\ UNCHANGED << raft, network, toSend >>
                          /\ UNCHANGED << slSupportFor, clock, canVote, msg >>

node(self) == TickLoop(self) \/ BroadcastVotes(self)
                 \/ BroadcastDeFortify(self) \/ HandleTerm(self)
                 \/ RecvMessage(self) \/ WonElection(self)
                 \/ BroadcastFortify(self)

Next == (\E self \in Nodes: node(self))

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Nodes : WF_vars(node(self))

\* END TRANSLATION 
====
