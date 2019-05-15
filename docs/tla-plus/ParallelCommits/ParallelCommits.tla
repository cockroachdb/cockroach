-------------------------- MODULE ParallelCommits --------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences
CONSTANTS KEYS, PREVENTERS
ASSUME Cardinality(KEYS) > 0

STATUSES == {"pending", "staging", "committed", "aborted"}

(*--algorithm parallelcommits
variables
  record = "pending";
  intent_writes = {};
  tscache = [k \in KEYS |-> FALSE];
  
define
  TypeInvariant ==
    /\ record \in STATUSES
    
  TemporalProperties ==
    <>[](record \in {"aborted", "committed"})
end define;

process committer = "committer"
variables
  to_write = KEYS;
  have_staged_record = FALSE;
begin
  StagingWrites:
    while to_write /= {} \/ ~have_staged_record do
      either
        await to_write /= {};
        with key \in to_write do
          if tscache[key] then
            \* Write prevented.
            record := "aborted";
            goto EndCommitter;
          else
            \* Write successful.
            intent_writes := intent_writes \union {key};
            to_write := to_write \ {key};
          end if;
        end with;
      or
        await ~have_staged_record;
        have_staged_record := TRUE;
        if record = "pending" then
          record := "staging";
        elsif record = "aborted" then
          \* Aborted before STAGING transaction record.
          goto EndCommitter;
        else
          assert FALSE;
        end if;
      end either
    end while;
  
  ExplicitCommit:
    if record = "staging" then
      record := "committed";
    elsif record = "committed" then
      \* Already committed by a recovery process.
      skip;
    else
      assert FALSE;
    end if;
    
  EndCommitter:
    skip;
  
end process;

fair process preventer \in PREVENTERS
variable
  checked_writes = {};
begin
  PushRecord:
    if record = "pending" then
      record := "aborted";
      goto EndRecover;
    elsif record = "staging" then
      skip;
    elsif record = "committed" then
      goto EndRecover;
    elsif record = "aborted" then
      goto EndRecover;
    end if;
    
  PreventWrites:
    while checked_writes /= KEYS do
      with key \in KEYS \ checked_writes do
        if key \in intent_writes then
          checked_writes := checked_writes \union {key}
        else 
          tscache[key] := TRUE;
          goto RecoverRecord;
        end if;
      end with;
    end while;
  
  RecoverRecord:
    with prevented = checked_writes /= KEYS do
      if record = "pending" then
        assert FALSE;
      elsif record = "staging" then
        if prevented then
           record := "aborted";
        else
           record := "committed";
        end if;
      elsif record = "committed" then
        assert ~prevented;
      elsif record = "aborted" then
        assert prevented;
      end if;
    end with;
    
  EndRecover:
    skip;
    
end process;  
end algorithm;*)
\* BEGIN TRANSLATION
VARIABLES record, intent_writes, tscache, pc

(* define statement *)
TypeInvariant ==
  /\ record \in STATUSES

TemporalProperties ==
  <>[](record \in {"aborted", "committed"})

VARIABLES to_write, have_staged_record, checked_writes

vars == << record, intent_writes, tscache, pc, to_write, have_staged_record, 
           checked_writes >>

ProcSet == {"committer"} \cup (PREVENTERS)

Init == (* Global variables *)
        /\ record = "pending"
        /\ intent_writes = {}
        /\ tscache = [k \in KEYS |-> FALSE]
        (* Process committer *)
        /\ to_write = KEYS
        /\ have_staged_record = FALSE
        (* Process preventer *)
        /\ checked_writes = [self \in PREVENTERS |-> {}]
        /\ pc = [self \in ProcSet |-> CASE self = "committer" -> "StagingWrites"
                                        [] self \in PREVENTERS -> "PushRecord"]

StagingWrites == /\ pc["committer"] = "StagingWrites"
                 /\ IF to_write /= {} \/ ~have_staged_record
                       THEN /\ \/ /\ to_write /= {}
                                  /\ \E key \in to_write:
                                       IF tscache[key]
                                          THEN /\ record' = "aborted"
                                               /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                                               /\ UNCHANGED << intent_writes, 
                                                               to_write >>
                                          ELSE /\ intent_writes' = (intent_writes \union {key})
                                               /\ to_write' = to_write \ {key}
                                               /\ pc' = [pc EXCEPT !["committer"] = "StagingWrites"]
                                               /\ UNCHANGED record
                                  /\ UNCHANGED have_staged_record
                               \/ /\ ~have_staged_record
                                  /\ have_staged_record' = TRUE
                                  /\ IF record = "pending"
                                        THEN /\ record' = "staging"
                                             /\ pc' = [pc EXCEPT !["committer"] = "StagingWrites"]
                                        ELSE /\ IF record = "aborted"
                                                   THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                                                   ELSE /\ Assert(FALSE, 
                                                                  "Failure of assertion at line 51, column 11.")
                                                        /\ pc' = [pc EXCEPT !["committer"] = "StagingWrites"]
                                             /\ UNCHANGED record
                                  /\ UNCHANGED <<intent_writes, to_write>>
                       ELSE /\ pc' = [pc EXCEPT !["committer"] = "ExplicitCommit"]
                            /\ UNCHANGED << record, intent_writes, to_write, 
                                            have_staged_record >>
                 /\ UNCHANGED << tscache, checked_writes >>

ExplicitCommit == /\ pc["committer"] = "ExplicitCommit"
                  /\ IF record = "staging"
                        THEN /\ record' = "committed"
                        ELSE /\ IF record = "committed"
                                   THEN /\ TRUE
                                   ELSE /\ Assert(FALSE, 
                                                  "Failure of assertion at line 63, column 7.")
                             /\ UNCHANGED record
                  /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                  /\ UNCHANGED << intent_writes, tscache, to_write, 
                                  have_staged_record, checked_writes >>

EndCommitter == /\ pc["committer"] = "EndCommitter"
                /\ TRUE
                /\ pc' = [pc EXCEPT !["committer"] = "Done"]
                /\ UNCHANGED << record, intent_writes, tscache, to_write, 
                                have_staged_record, checked_writes >>

committer == StagingWrites \/ ExplicitCommit \/ EndCommitter

PushRecord(self) == /\ pc[self] = "PushRecord"
                    /\ IF record = "pending"
                          THEN /\ record' = "aborted"
                               /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                          ELSE /\ IF record = "staging"
                                     THEN /\ TRUE
                                          /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                     ELSE /\ IF record = "committed"
                                                THEN /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                ELSE /\ IF record = "aborted"
                                                           THEN /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                           ELSE /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                               /\ UNCHANGED record
                    /\ UNCHANGED << intent_writes, tscache, to_write, 
                                    have_staged_record, checked_writes >>

PreventWrites(self) == /\ pc[self] = "PreventWrites"
                       /\ IF checked_writes[self] /= KEYS
                             THEN /\ \E key \in KEYS \ checked_writes[self]:
                                       IF key \in intent_writes
                                          THEN /\ checked_writes' = [checked_writes EXCEPT ![self] = checked_writes[self] \union {key}]
                                               /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                               /\ UNCHANGED tscache
                                          ELSE /\ tscache' = [tscache EXCEPT ![key] = TRUE]
                                               /\ pc' = [pc EXCEPT ![self] = "RecoverRecord"]
                                               /\ UNCHANGED checked_writes
                             ELSE /\ pc' = [pc EXCEPT ![self] = "RecoverRecord"]
                                  /\ UNCHANGED << tscache, checked_writes >>
                       /\ UNCHANGED << record, intent_writes, to_write, 
                                       have_staged_record >>

RecoverRecord(self) == /\ pc[self] = "RecoverRecord"
                       /\ LET prevented == checked_writes[self] /= KEYS IN
                            IF record = "pending"
                               THEN /\ Assert(FALSE, 
                                              "Failure of assertion at line 102, column 9.")
                                    /\ UNCHANGED record
                               ELSE /\ IF record = "staging"
                                          THEN /\ IF prevented
                                                     THEN /\ record' = "aborted"
                                                     ELSE /\ record' = "committed"
                                          ELSE /\ IF record = "committed"
                                                     THEN /\ Assert(~prevented, 
                                                                    "Failure of assertion at line 110, column 9.")
                                                     ELSE /\ IF record = "aborted"
                                                                THEN /\ Assert(prevented, 
                                                                               "Failure of assertion at line 112, column 9.")
                                                                ELSE /\ TRUE
                                               /\ UNCHANGED record
                       /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                       /\ UNCHANGED << intent_writes, tscache, to_write, 
                                       have_staged_record, checked_writes >>

EndRecover(self) == /\ pc[self] = "EndRecover"
                    /\ TRUE
                    /\ pc' = [pc EXCEPT ![self] = "Done"]
                    /\ UNCHANGED << record, intent_writes, tscache, to_write, 
                                    have_staged_record, checked_writes >>

preventer(self) == PushRecord(self) \/ PreventWrites(self)
                      \/ RecoverRecord(self) \/ EndRecover(self)

Next == committer
           \/ (\E self \in PREVENTERS: preventer(self))
           \/ (* Disjunct to prevent deadlock on termination *)
              ((\A self \in ProcSet: pc[self] = "Done") /\ UNCHANGED vars)

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in PREVENTERS : WF_vars(preventer(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION



=============================================================================
\* Modification History
\* Last modified Tue May 14 18:50:26 EDT 2019 by nathan
\* Created Mon May 13 10:03:40 EDT 2019 by nathan
