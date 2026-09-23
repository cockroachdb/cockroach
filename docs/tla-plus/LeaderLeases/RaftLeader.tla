------------------------------ MODULE RaftLeader ------------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences, SequencesExt

CONSTANTS Nodes, Terms
ASSUME Cardinality(Nodes) > 0
ASSUME Terms > 0

CONSTANTS StateFollower, StatePreCandidate, StateCandidate, StateLeader
CONSTANTS MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp, MsgApp, MsgAppResp
CONSTANTS WithRestarts, AllowMsgReordering, UsePreVote, SendMsgApp

(*****************************************************************************)
(* RaftLeader is a specification for the Raft leader election protocol, as   *)
(* described in section 3.4 of the Raft thesis[^1]. The spec models multiple *)
(* terms, node restarts, and the Pre-Vote election phase extension.          *)
(*                                                                           *)
(* The central safety property of leader election is exclusivity. In Raft,   *)
(* this is defined as the Election Safety Property: "at most one leader can  *)
(* be elected in a given term". This property is defined as an invariant of  *)
(* this spec.                                                                *)
(*                                                                           *)
(* [^1]: https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf       *)
(*****************************************************************************)

(*--algorithm RaftLeader
variables
  \* Raft state.
  raft = [i \in Nodes |-> [
    state     |-> StateFollower,
    term      |-> 0,
    vote      |-> 0,
    lead      |-> 0,
    voteResps |-> {},
    appResps  |-> {},
    committed |-> FALSE
  ]];
  \* In-flight messages, by destination.
  network = [i \in Nodes |-> EmptyNetwork];

define
  \* Define Nodes as a symmetry set. Cuts runtime by 6x.
  Symmetry == Permutations(Nodes)

  \* If we allow message reordering, represent the network as a set. Otherwise,
  \* represent it as a sequence.
  EmptyNetwork == IF AllowMsgReordering THEN {} ELSE <<>>

  RaftQuorum    == {i \in SUBSET(Nodes) : Cardinality(i) * 2 > Cardinality(Nodes)}
  CanCampaign   == [i \in Nodes |-> raft[i].term < Terms /\ raft[i].state = StateFollower]
  CanReceiveMsg == [i \in Nodes |-> network[i] /= EmptyNetwork]

  \*****************************************************\
  \* Define type invariants as a form of type checking *\
  \*****************************************************\

  RaftStates   == {StateFollower, StatePreCandidate, StateCandidate, StateLeader}
  RaftMsgTypes == {MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp, MsgApp, MsgAppResp}
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
      \A msg \in IF AllowMsgReordering THEN network[i] ELSE ToSet(network[i]):
        /\ msg.type \in RaftMsgTypes
        /\ msg.from \in Nodes
        /\ msg.term \in RaftTerms

  TypeInvariants ==
    /\ RaftStateInvariant
    /\ RaftMsgInvariant

  \*************************************************\
  \* Define safety invariants to check correctness *\
  \*************************************************\

  \* There can only be one leader for a given term.
  LeaderExclusivity == \A i, j \in Nodes :
    raft[i].term = raft[j].term /\ raft[i].lead /= 0 /\ raft[j].lead /= 0 =>
      raft[i].lead = raft[j].lead
end define;

\*************************************\
\* Macros for Raft state transitions *\
\*************************************\

macro become_follower(term, lead)
begin
  if raft[self].term /= term then
    raft[self].state := StateFollower ||
    raft[self].term  := term          ||
    raft[self].vote  := 0             ||
    raft[self].lead  := lead;
  else
    raft[self].state := StateFollower ||
    raft[self].lead  := lead;
  end if;
end macro;

macro become_pre_candidate()
begin
  raft[self].state     := StatePreCandidate ||
  raft[self].lead      := 0                 ||
  raft[self].voteResps := {self};
end macro;

macro become_candidate()
begin
  raft[self].state     := StateCandidate      ||
  raft[self].term      := raft[self].term + 1 ||
  raft[self].vote      := self                ||
  raft[self].lead      := 0                   ||
  raft[self].voteResps := {self};
end macro;

macro become_leader()
begin
  raft[self].state     := StateLeader ||
  raft[self].lead      := self        ||
  raft[self].appResps  := {self}      ||
  raft[self].committed := FALSE;
end macro;

macro restart()
begin
  raft[self] := [
    state     |-> StateFollower,
    term      |-> raft[self].term, \* persistent
    vote      |-> raft[self].vote, \* persistent
    lead      |-> 0,
    voteResps |-> {},
    appResps  |-> {},
    committed |-> FALSE
  ];
end macro;

\************************************\
\* Macros for sending Raft messages *\
\************************************\

macro send_msg(to, msg)
begin
  if AllowMsgReordering then
    network[to] := network[to] \union {msg};
  else
    network[to] := Append(network[to], msg);
  end if;
end macro

macro send_msg_pre_vote(to)
begin
  send_msg(to, [
    type |-> MsgPreVote,
    from |-> self,
    term |-> raft[self].term + 1
  ]);
end macro

macro send_msg_pre_vote_resp(to, reject)
begin
  send_msg(to, [
    type   |-> MsgPreVoteResp,
    from   |-> self,
    term   |-> raft[self].term,
    reject |-> reject
  ]);
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

macro send_msg_app(to)
begin
  send_msg(to, [
    type   |-> MsgApp,
    from   |-> self,
    term   |-> raft[self].term
  ]);
end macro

macro send_msg_app_resp(to, reject)
begin
  send_msg(to, [
    type   |-> MsgAppResp,
    from   |-> self,
    term   |-> raft[self].term,
    reject |-> reject
  ]);
end macro

macro prep_broadcast()
begin
  toSend := SetToSeq(Nodes \ {self});
end macro;

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

\****************************\
\* Replicas in a Raft group *\
\****************************\

fair process node \in Nodes
variables
  canVote = FALSE;
  toSend  = <<>>;
  msg     = [type |-> 0];
begin TickLoop:
  while CanCampaign[self] \/ CanReceiveMsg[self] do
    either
      await CanCampaign[self];
      if UsePreVote then
        become_pre_candidate();
        prep_broadcast();
        BroadcastPreVotes:
          while toSend /= <<>> do
            send_msg_pre_vote(Head(toSend));
            toSend := Tail(toSend);
          end while;
      else
        become_candidate();
        prep_broadcast();
        BroadcastVotes:
          while toSend /= <<>> do
            send_msg_vote(Head(toSend));
            toSend := Tail(toSend);
          end while;
      end if;
    or
      await WithRestarts;
      restart();
    or
      await CanReceiveMsg[self];
      recv_msg();

      if msg.term > raft[self].term then
        if msg.type \in {MsgVote, MsgPreVote} /\ raft[self].lead /= 0 then
          if msg.type = MsgVote then
            become_follower(msg.term, 0);
          end if;

          \* either \* NON-DETERMINISM! */
          \*   \* Reject vote or pre-vote if recently heard from leader.
          \*   goto TickLoop;
          \* or
          \*   if msg.type = MsgVote then
          \*     become_follower(msg.term, 0);
          \*   end if;
          \* end either;
        elsif msg.type = MsgPreVote then
          skip;
        elsif msg.type = MsgPreVoteResp /\ ~msg.reject then
          skip;
        else
          if msg.type = MsgApp then
            become_follower(msg.term, msg.from);
          else
            become_follower(msg.term, 0);
          end if;
        end if;
      elsif msg.term < raft[self].term then
        if msg.type = MsgApp then
          skip; \* Handled below.
        else
          goto TickLoop;
        end if;
      else
        \* TODO(nvanbenschoten): is not having this check a bug?
        \* if msg.type = MsgPreVoteResp then
        \*   assert FALSE;
        \* end if;
      end if;

      RecvMessage:
        if msg.type \in {MsgVote, MsgPreVote} then
          \* We can vote if this is a repeat of a vote we've already cast...
          canVote := raft[self].vote = msg.from
            \* ...we haven't voted and we don't think there's a leader yet in this term...
            \/ (raft[self].vote = 0 /\ raft[self].lead = 0)
            \* ...or this is a PreVote for a future term...
            \/ (msg.type = MsgPreVote /\ msg.term > raft[self].term);

          if canVote then
            \* We can vote for the candidate.
            if msg.type = MsgPreVote then
              send_msg_pre_vote_resp(msg.from, FALSE);
            else
              send_msg_vote_resp(msg.from, FALSE);
              raft[self].vote := msg.from;
            end if;

            \* either \* NON-DETERMINISM! */
            \*   \* The candidate's log is up-to-date.
            \*   if msg.type = MsgPreVote then
            \*     send_msg_pre_vote_resp(msg.from, FALSE);
            \*   else
            \*     send_msg_vote_resp(msg.from, FALSE);
            \*     raft[self].vote := msg.from;
            \*   end if;
            \* or
            \*   \* The candidate's log is not up-to-date.
            \*   if msg.type = MsgPreVote then
            \*     send_msg_pre_vote_resp(msg.from, TRUE);
            \*   else
            \*     send_msg_vote_resp(msg.from, TRUE);
            \*   end if;
            \* end either;

          else
            \* We cannot vote for the candidate.
            if msg.type = MsgPreVote then
              send_msg_pre_vote_resp(msg.from, TRUE);
            else
              send_msg_vote_resp(msg.from, TRUE);
            end if;
          end if;

        elsif msg.type \in {MsgVoteResp, MsgPreVoteResp} then
          if ~msg.reject /\ ((raft[self].state = StateCandidate /\ msg.type = MsgVoteResp)
                          \/ (raft[self].state = StatePreCandidate /\ msg.type = MsgPreVoteResp)) then
            raft[self].voteResps := raft[self].voteResps \union {msg.from};
            if raft[self].voteResps \in RaftQuorum then
              if raft[self].state = StatePreCandidate then
                WonPreElection:
                  become_candidate();
                  prep_broadcast();
                  BroadcastVotesAfterPreVote:
                    while toSend /= <<>> do
                      send_msg_vote(Head(toSend));
                      toSend := Tail(toSend);
                    end while;
              else
                WonElection:
                  become_leader();
                  prep_broadcast();
                  BroadcastAppend:
                    while toSend /= <<>> do
                      send_msg_app(Head(toSend));
                      toSend := Tail(toSend);
                    end while;
              end if;
            end if;
          end if;

        elsif msg.type = MsgApp then
          if msg.term < raft[self].term then
            \* TODO: will this violate leases?
            send_msg_app_resp(msg.from, TRUE);
          else
            send_msg_app_resp(msg.from, FALSE);
          end if;

        elsif msg.type = MsgAppResp then
          if ~msg.reject /\ raft[self].state = StateLeader then
            raft[self].appResps := raft[self].appResps \union {msg.from} ||
            raft[self].committed := (raft[self].appResps \union {msg.from}) \in RaftQuorum;
            if raft[self].committed then
              Appended:
                skip;
            end if;
          end if;

        else
          assert FALSE;
        end if;
    end either;
  end while;
end process;
end algorithm; *)
\* BEGIN TRANSLATION (chksum(pcal) = "a4b0aa8e" /\ chksum(tla) = "53a6c574")
VARIABLES raft, network, pc

(* define statement *)
Symmetry == Permutations(Nodes)



EmptyNetwork == IF AllowMsgReordering THEN {} ELSE <<>>

RaftQuorum    == {i \in SUBSET(Nodes) : Cardinality(i) * 2 > Cardinality(Nodes)}
CanCampaign   == [i \in Nodes |-> raft[i].term < Terms /\ raft[i].state = StateFollower]
CanReceiveMsg == [i \in Nodes |-> network[i] /= EmptyNetwork]





RaftStates   == {StateFollower, StatePreCandidate, StateCandidate, StateLeader}
RaftMsgTypes == {MsgVote, MsgVoteResp, MsgPreVote, MsgPreVoteResp, MsgApp, MsgAppResp}
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
    \A msg \in IF AllowMsgReordering THEN network[i] ELSE ToSet(network[i]):
      /\ msg.type \in RaftMsgTypes
      /\ msg.from \in Nodes
      /\ msg.term \in RaftTerms

TypeInvariants ==
  /\ RaftStateInvariant
  /\ RaftMsgInvariant






LeaderExclusivity == \A i, j \in Nodes :
  raft[i].term = raft[j].term /\ raft[i].lead /= 0 /\ raft[j].lead /= 0 =>
    raft[i].lead = raft[j].lead

VARIABLES canVote, toSend, msg

vars == << raft, network, pc, canVote, toSend, msg >>

ProcSet == (Nodes)

Init == (* Global variables *)
        /\ raft =        [i \in Nodes |-> [
                    state     |-> StateFollower,
                    term      |-> 0,
                    vote      |-> 0,
                    lead      |-> 0,
                    voteResps |-> {},
                    appResps  |-> {},
                    committed |-> FALSE
                  ]]
        /\ network = [i \in Nodes |-> EmptyNetwork]
        (* Process node *)
        /\ canVote = [self \in Nodes |-> FALSE]
        /\ toSend = [self \in Nodes |-> <<>>]
        /\ msg = [self \in Nodes |-> [type |-> 0]]
        /\ pc = [self \in ProcSet |-> "TickLoop"]

TickLoop(self) == /\ pc[self] = "TickLoop"
                  /\ IF CanCampaign[self] \/ CanReceiveMsg[self]
                        THEN /\ \/ /\ CanCampaign[self]
                                   /\ IF UsePreVote
                                         THEN /\ raft' = [raft EXCEPT ![self].state = StatePreCandidate,
                                                                      ![self].lead = 0,
                                                                      ![self].voteResps = {self}]
                                              /\ toSend' = [toSend EXCEPT ![self] = SetToSeq(Nodes \ {self})]
                                              /\ pc' = [pc EXCEPT ![self] = "BroadcastPreVotes"]
                                         ELSE /\ raft' = [raft EXCEPT ![self].state = StateCandidate,
                                                                      ![self].term = raft[self].term + 1,
                                                                      ![self].vote = self,
                                                                      ![self].lead = 0,
                                                                      ![self].voteResps = {self}]
                                              /\ toSend' = [toSend EXCEPT ![self] = SetToSeq(Nodes \ {self})]
                                              /\ pc' = [pc EXCEPT ![self] = "BroadcastVotes"]
                                   /\ UNCHANGED <<network, msg>>
                                \/ /\ WithRestarts
                                   /\ raft' = [raft EXCEPT ![self] =               [
                                                                       state     |-> StateFollower,
                                                                       term      |-> raft[self].term,
                                                                       vote      |-> raft[self].vote,
                                                                       lead      |-> 0,
                                                                       voteResps |-> {},
                                                                       appResps  |-> {},
                                                                       committed |-> FALSE
                                                                     ]]
                                   /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                   /\ UNCHANGED <<network, toSend, msg>>
                                \/ /\ CanReceiveMsg[self]
                                   /\ IF AllowMsgReordering
                                         THEN /\ \E recv \in network[self]:
                                                   /\ network' = [network EXCEPT ![self] = network[self] \ {recv}]
                                                   /\ msg' = [msg EXCEPT ![self] = recv]
                                         ELSE /\ msg' = [msg EXCEPT ![self] = Head(network[self])]
                                              /\ network' = [network EXCEPT ![self] = Tail(network[self])]
                                   /\ IF msg'[self].term > raft[self].term
                                         THEN /\ IF msg'[self].type \in {MsgVote, MsgPreVote} /\ raft[self].lead /= 0
                                                    THEN /\ IF msg'[self].type = MsgVote
                                                               THEN /\ IF raft[self].term /= (msg'[self].term)
                                                                          THEN /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                                       ![self].term = msg'[self].term,
                                                                                                       ![self].vote = 0,
                                                                                                       ![self].lead = 0]
                                                                          ELSE /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                                       ![self].lead = 0]
                                                               ELSE /\ TRUE
                                                                    /\ raft' = raft
                                                    ELSE /\ IF msg'[self].type = MsgPreVote
                                                               THEN /\ TRUE
                                                                    /\ raft' = raft
                                                               ELSE /\ IF msg'[self].type = MsgPreVoteResp /\ ~msg'[self].reject
                                                                          THEN /\ TRUE
                                                                               /\ raft' = raft
                                                                          ELSE /\ IF msg'[self].type = MsgApp
                                                                                     THEN /\ IF raft[self].term /= (msg'[self].term)
                                                                                                THEN /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                                                             ![self].term = msg'[self].term,
                                                                                                                             ![self].vote = 0,
                                                                                                                             ![self].lead = msg'[self].from]
                                                                                                ELSE /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                                                             ![self].lead = msg'[self].from]
                                                                                     ELSE /\ IF raft[self].term /= (msg'[self].term)
                                                                                                THEN /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                                                             ![self].term = msg'[self].term,
                                                                                                                             ![self].vote = 0,
                                                                                                                             ![self].lead = 0]
                                                                                                ELSE /\ raft' = [raft EXCEPT ![self].state = StateFollower,
                                                                                                                             ![self].lead = 0]
                                              /\ pc' = [pc EXCEPT ![self] = "RecvMessage"]
                                         ELSE /\ IF msg'[self].term < raft[self].term
                                                    THEN /\ IF msg'[self].type = MsgApp
                                                               THEN /\ TRUE
                                                                    /\ pc' = [pc EXCEPT ![self] = "RecvMessage"]
                                                               ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                    ELSE /\ pc' = [pc EXCEPT ![self] = "RecvMessage"]
                                              /\ raft' = raft
                                   /\ UNCHANGED toSend
                        ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                             /\ UNCHANGED << raft, network, toSend, msg >>
                  /\ UNCHANGED canVote

BroadcastPreVotes(self) == /\ pc[self] = "BroadcastPreVotes"
                           /\ IF toSend[self] /= <<>>
                                 THEN /\ IF AllowMsgReordering
                                            THEN /\ network' = [network EXCEPT ![(Head(toSend[self]))] = network[(Head(toSend[self]))] \union {(             [
                                                                                                           type |-> MsgPreVote,
                                                                                                           from |-> self,
                                                                                                           term |-> raft[self].term + 1
                                                                                                         ])}]
                                            ELSE /\ network' = [network EXCEPT ![(Head(toSend[self]))] = Append(network[(Head(toSend[self]))], (             [
                                                                                                           type |-> MsgPreVote,
                                                                                                           from |-> self,
                                                                                                           term |-> raft[self].term + 1
                                                                                                         ]))]
                                      /\ toSend' = [toSend EXCEPT ![self] = Tail(toSend[self])]
                                      /\ pc' = [pc EXCEPT ![self] = "BroadcastPreVotes"]
                                 ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                      /\ UNCHANGED << network, toSend >>
                           /\ UNCHANGED << raft, canVote, msg >>

BroadcastVotes(self) == /\ pc[self] = "BroadcastVotes"
                        /\ IF toSend[self] /= <<>>
                              THEN /\ IF AllowMsgReordering
                                         THEN /\ network' = [network EXCEPT ![(Head(toSend[self]))] = network[(Head(toSend[self]))] \union {(             [
                                                                                                        type   |-> MsgVote,
                                                                                                        from   |-> self,
                                                                                                        term   |-> raft[self].term
                                                                                                      ])}]
                                         ELSE /\ network' = [network EXCEPT ![(Head(toSend[self]))] = Append(network[(Head(toSend[self]))], (             [
                                                                                                        type   |-> MsgVote,
                                                                                                        from   |-> self,
                                                                                                        term   |-> raft[self].term
                                                                                                      ]))]
                                   /\ toSend' = [toSend EXCEPT ![self] = Tail(toSend[self])]
                                   /\ pc' = [pc EXCEPT ![self] = "BroadcastVotes"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                   /\ UNCHANGED << network, toSend >>
                        /\ UNCHANGED << raft, canVote, msg >>

RecvMessage(self) == /\ pc[self] = "RecvMessage"
                     /\ IF msg[self].type \in {MsgVote, MsgPreVote}
                           THEN /\ canVote' = [canVote EXCEPT ![self] =          raft[self].vote = msg[self].from
                                                                        
                                                                        \/ (raft[self].vote = 0 /\ raft[self].lead = 0)
                                                                        
                                                                        \/ (msg[self].type = MsgPreVote /\ msg[self].term > raft[self].term)]
                                /\ IF canVote'[self]
                                      THEN /\ IF msg[self].type = MsgPreVote
                                                 THEN /\ IF AllowMsgReordering
                                                            THEN /\ network' = [network EXCEPT ![(msg[self].from)] = network[(msg[self].from)] \union {(             [
                                                                                                                       type   |-> MsgPreVoteResp,
                                                                                                                       from   |-> self,
                                                                                                                       term   |-> raft[self].term,
                                                                                                                       reject |-> FALSE
                                                                                                                     ])}]
                                                            ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                                       type   |-> MsgPreVoteResp,
                                                                                                                       from   |-> self,
                                                                                                                       term   |-> raft[self].term,
                                                                                                                       reject |-> FALSE
                                                                                                                     ]))]
                                                      /\ raft' = raft
                                                 ELSE /\ IF AllowMsgReordering
                                                            THEN /\ network' = [network EXCEPT ![(msg[self].from)] = network[(msg[self].from)] \union {(             [
                                                                                                                       type   |-> MsgVoteResp,
                                                                                                                       from   |-> self,
                                                                                                                       term   |-> raft[self].term,
                                                                                                                       reject |-> FALSE
                                                                                                                     ])}]
                                                            ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                                       type   |-> MsgVoteResp,
                                                                                                                       from   |-> self,
                                                                                                                       term   |-> raft[self].term,
                                                                                                                       reject |-> FALSE
                                                                                                                     ]))]
                                                      /\ raft' = [raft EXCEPT ![self].vote = msg[self].from]
                                      ELSE /\ IF msg[self].type = MsgPreVote
                                                 THEN /\ IF AllowMsgReordering
                                                            THEN /\ network' = [network EXCEPT ![(msg[self].from)] = network[(msg[self].from)] \union {(             [
                                                                                                                       type   |-> MsgPreVoteResp,
                                                                                                                       from   |-> self,
                                                                                                                       term   |-> raft[self].term,
                                                                                                                       reject |-> TRUE
                                                                                                                     ])}]
                                                            ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                                       type   |-> MsgPreVoteResp,
                                                                                                                       from   |-> self,
                                                                                                                       term   |-> raft[self].term,
                                                                                                                       reject |-> TRUE
                                                                                                                     ]))]
                                                 ELSE /\ IF AllowMsgReordering
                                                            THEN /\ network' = [network EXCEPT ![(msg[self].from)] = network[(msg[self].from)] \union {(             [
                                                                                                                       type   |-> MsgVoteResp,
                                                                                                                       from   |-> self,
                                                                                                                       term   |-> raft[self].term,
                                                                                                                       reject |-> TRUE
                                                                                                                     ])}]
                                                            ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                                       type   |-> MsgVoteResp,
                                                                                                                       from   |-> self,
                                                                                                                       term   |-> raft[self].term,
                                                                                                                       reject |-> TRUE
                                                                                                                     ]))]
                                           /\ raft' = raft
                                /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                           ELSE /\ IF msg[self].type \in {MsgVoteResp, MsgPreVoteResp}
                                      THEN /\ IF ~msg[self].reject /\ ((raft[self].state = StateCandidate /\ msg[self].type = MsgVoteResp)
                                                                    \/ (raft[self].state = StatePreCandidate /\ msg[self].type = MsgPreVoteResp))
                                                 THEN /\ raft' = [raft EXCEPT ![self].voteResps = raft[self].voteResps \union {msg[self].from}]
                                                      /\ IF raft'[self].voteResps \in RaftQuorum
                                                            THEN /\ IF raft'[self].state = StatePreCandidate
                                                                       THEN /\ pc' = [pc EXCEPT ![self] = "WonPreElection"]
                                                                       ELSE /\ pc' = [pc EXCEPT ![self] = "WonElection"]
                                                            ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                 ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                      /\ raft' = raft
                                           /\ UNCHANGED network
                                      ELSE /\ IF msg[self].type = MsgApp
                                                 THEN /\ IF msg[self].term < raft[self].term
                                                            THEN /\ IF AllowMsgReordering
                                                                       THEN /\ network' = [network EXCEPT ![(msg[self].from)] = network[(msg[self].from)] \union {(             [
                                                                                                                                  type   |-> MsgAppResp,
                                                                                                                                  from   |-> self,
                                                                                                                                  term   |-> raft[self].term,
                                                                                                                                  reject |-> TRUE
                                                                                                                                ])}]
                                                                       ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                                                  type   |-> MsgAppResp,
                                                                                                                                  from   |-> self,
                                                                                                                                  term   |-> raft[self].term,
                                                                                                                                  reject |-> TRUE
                                                                                                                                ]))]
                                                            ELSE /\ IF AllowMsgReordering
                                                                       THEN /\ network' = [network EXCEPT ![(msg[self].from)] = network[(msg[self].from)] \union {(             [
                                                                                                                                  type   |-> MsgAppResp,
                                                                                                                                  from   |-> self,
                                                                                                                                  term   |-> raft[self].term,
                                                                                                                                  reject |-> FALSE
                                                                                                                                ])}]
                                                                       ELSE /\ network' = [network EXCEPT ![(msg[self].from)] = Append(network[(msg[self].from)], (             [
                                                                                                                                  type   |-> MsgAppResp,
                                                                                                                                  from   |-> self,
                                                                                                                                  term   |-> raft[self].term,
                                                                                                                                  reject |-> FALSE
                                                                                                                                ]))]
                                                      /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                      /\ raft' = raft
                                                 ELSE /\ IF msg[self].type = MsgAppResp
                                                            THEN /\ IF ~msg[self].reject /\ raft[self].state = StateLeader
                                                                       THEN /\ raft' = [raft EXCEPT ![self].appResps = raft[self].appResps \union {msg[self].from},
                                                                                                    ![self].committed = (raft[self].appResps \union {msg[self].from}) \in RaftQuorum]
                                                                            /\ IF raft'[self].committed
                                                                                  THEN /\ pc' = [pc EXCEPT ![self] = "Appended"]
                                                                                  ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                                       ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                                            /\ raft' = raft
                                                            ELSE /\ Assert(FALSE, 
                                                                           "Failure of assertion at line 397, column 11.")
                                                                 /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                                                 /\ raft' = raft
                                                      /\ UNCHANGED network
                                /\ UNCHANGED canVote
                     /\ UNCHANGED << toSend, msg >>

WonPreElection(self) == /\ pc[self] = "WonPreElection"
                        /\ raft' = [raft EXCEPT ![self].state = StateCandidate,
                                                ![self].term = raft[self].term + 1,
                                                ![self].vote = self,
                                                ![self].lead = 0,
                                                ![self].voteResps = {self}]
                        /\ toSend' = [toSend EXCEPT ![self] = SetToSeq(Nodes \ {self})]
                        /\ pc' = [pc EXCEPT ![self] = "BroadcastVotesAfterPreVote"]
                        /\ UNCHANGED << network, canVote, msg >>

BroadcastVotesAfterPreVote(self) == /\ pc[self] = "BroadcastVotesAfterPreVote"
                                    /\ IF toSend[self] /= <<>>
                                          THEN /\ IF AllowMsgReordering
                                                     THEN /\ network' = [network EXCEPT ![(Head(toSend[self]))] = network[(Head(toSend[self]))] \union {(             [
                                                                                                                    type   |-> MsgVote,
                                                                                                                    from   |-> self,
                                                                                                                    term   |-> raft[self].term
                                                                                                                  ])}]
                                                     ELSE /\ network' = [network EXCEPT ![(Head(toSend[self]))] = Append(network[(Head(toSend[self]))], (             [
                                                                                                                    type   |-> MsgVote,
                                                                                                                    from   |-> self,
                                                                                                                    term   |-> raft[self].term
                                                                                                                  ]))]
                                               /\ toSend' = [toSend EXCEPT ![self] = Tail(toSend[self])]
                                               /\ pc' = [pc EXCEPT ![self] = "BroadcastVotesAfterPreVote"]
                                          ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                               /\ UNCHANGED << network, toSend >>
                                    /\ UNCHANGED << raft, canVote, msg >>

WonElection(self) == /\ pc[self] = "WonElection"
                     /\ raft' = [raft EXCEPT ![self].state = StateLeader,
                                             ![self].lead = self,
                                             ![self].appResps = {self},
                                             ![self].committed = FALSE]
                     /\ toSend' = [toSend EXCEPT ![self] = SetToSeq(Nodes \ {self})]
                     /\ pc' = [pc EXCEPT ![self] = "BroadcastAppend"]
                     /\ UNCHANGED << network, canVote, msg >>

BroadcastAppend(self) == /\ pc[self] = "BroadcastAppend"
                         /\ IF toSend[self] /= <<>>
                               THEN /\ IF AllowMsgReordering
                                          THEN /\ network' = [network EXCEPT ![(Head(toSend[self]))] = network[(Head(toSend[self]))] \union {(             [
                                                                                                         type   |-> MsgApp,
                                                                                                         from   |-> self,
                                                                                                         term   |-> raft[self].term
                                                                                                       ])}]
                                          ELSE /\ network' = [network EXCEPT ![(Head(toSend[self]))] = Append(network[(Head(toSend[self]))], (             [
                                                                                                         type   |-> MsgApp,
                                                                                                         from   |-> self,
                                                                                                         term   |-> raft[self].term
                                                                                                       ]))]
                                    /\ toSend' = [toSend EXCEPT ![self] = Tail(toSend[self])]
                                    /\ pc' = [pc EXCEPT ![self] = "BroadcastAppend"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                                    /\ UNCHANGED << network, toSend >>
                         /\ UNCHANGED << raft, canVote, msg >>

Appended(self) == /\ pc[self] = "Appended"
                  /\ TRUE
                  /\ pc' = [pc EXCEPT ![self] = "TickLoop"]
                  /\ UNCHANGED << raft, network, canVote, toSend, msg >>

node(self) == TickLoop(self) \/ BroadcastPreVotes(self)
                 \/ BroadcastVotes(self) \/ RecvMessage(self)
                 \/ WonPreElection(self)
                 \/ BroadcastVotesAfterPreVote(self) \/ WonElection(self)
                 \/ BroadcastAppend(self) \/ Appended(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in Nodes: node(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in Nodes : WF_vars(node(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION 
====
