-------------------------- MODULE ParallelCommits --------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences
CONSTANTS KEYS, PREVENTERS, MAX_EPOCH
ASSUME Cardinality(KEYS) > 0

Max(a, b) == IF a > b THEN a ELSE b

(*--algorithm parallelcommits
variables
  record = [status |-> "pending", epoch |-> 0];
  intent_writes = [k \in KEYS |-> 0];
  tscache = [k \in KEYS |-> 0];
  commit_ack = FALSE;
  
define
  RecordStatuses  == {"pending", "staging", "committed", "aborted"}
  RecordCommitted == record.status = "committed"
  RecordAborted   == record.status = "aborted"
  RecordFinalized == RecordCommitted \/ RecordAborted

  TypeInvariant ==
    /\ record \in [status: RecordStatuses, epoch: 0..MAX_EPOCH]
    /\ DOMAIN intent_writes = KEYS
      /\ \A k \in KEYS: intent_writes[k] \in 0..MAX_EPOCH
    /\ DOMAIN tscache = KEYS
      /\ \A k \in KEYS: tscache[k] \in 0..MAX_EPOCH
    
  TemporalTxnRecordProperties ==
    \* The txn record always ends with either a COMMITTED or ABORTED status.
    /\ <>[]RecordFinalized
    \* Once the txn record moves to a finalized status, it stays there.
    /\ [](RecordCommitted => []RecordCommitted)
    /\ [](RecordAborted   => []RecordAborted)
    \* The txn record's epoch must always grow.
    /\ [][record'.epoch >= record.epoch]_record
    
  TemporalIntentProperties ==
    \* Intent writes' epochs must always grow.
    /\ [][\A k \in KEYS: intent_writes'[k] >= intent_writes[k]]_intent_writes
    
  TemporalTSCacheProperties ==
    \* The timestamp cache always advances.
    /\ [][\A k \in KEYS: tscache'[k] >= tscache[k]]_tscache
    
  \* If the client is acked, the record should eventually be committed.
  AckLeadsToCommit == commit_ack ~> RecordCommitted
end define;

process committer = "committer"
variables
  txn_epoch = 0;
  to_write = {};
  have_staged_record = FALSE;
begin
  ParallelCommitLoop:
    \* Reset variables for each epoch.
    txn_epoch := txn_epoch + 1;
    to_write := KEYS;
    have_staged_record := FALSE;
    
    \* Give up after MAX_EPOCH attempts.
    if txn_epoch > MAX_EPOCH then
      goto EndCommitter;
    end if;
    
    TryStageWrites:
      while to_write /= {} \/ ~have_staged_record do
        either
          await to_write /= {};
          with key \in to_write do
            if tscache[key] >= txn_epoch then
              \* Write prevented. Try again at new epoch.
              \* TODO attempt refresh within current epoch.
              goto ParallelCommitLoop;
            else
              \* Write successful.
              intent_writes[key] := txn_epoch;
              to_write := to_write \ {key};
            end if;
          end with;
        or
          await ~have_staged_record;
          have_staged_record := TRUE;
          if record.status = "pending" then
            \* Move to staging status.
            record := [status |-> "staging", epoch |-> txn_epoch];
          elsif record.status = "staging" then
            \* Bump record epoch.
            assert record.epoch < txn_epoch;
            record.epoch := txn_epoch;
          elsif record.status = "aborted" then
            \* Aborted before STAGING transaction record.
            goto EndCommitter;
          elsif record.status = "committed" then
            assert FALSE;
          end if;
        end either
      end while;
      
      \* Ack the client now that all writes have succeeded.
      AckClient:
        commit_ack := TRUE;
  
  ExplicitCommit:
    if record.status = "staging" then
      \* Make implicit commit explicit.
      record.status := "committed";
    elsif record.status = "committed" then
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
  prevent_epoch = 0;
  found_writes = {};
begin
  PreventLoop:
    found_writes := {};
    
    PushRecord:
      if record.status = "pending" then
        \* Transaction not yet staged, abort.
        record.status := "aborted";
        goto EndRecover;
      elsif record.status = "staging" then
        \* Transaction staging, kick off recovery process.
        prevent_epoch := record.epoch;
      elsif record.status = "committed" then
        goto EndRecover;
      elsif record.status = "aborted" then
        goto EndRecover;
      end if;
      
    PreventWrites:
      while found_writes /= KEYS do
        with key \in KEYS \ found_writes do
          with intent_epoch = intent_writes[key] do
            if intent_epoch = prevent_epoch then
              \* Intent found. Did not prevent.
              found_writes := found_writes \union {key}
            else 
              \* Intent missing. Prevent.
              tscache[key] := Max(tscache[key], prevent_epoch);
              goto RecoverRecord;
            end if;
          end with;
        end with;
      end while;
    
    RecoverRecord:
      with prevented = found_writes /= KEYS do
        if prevented then
          if record.status = "aborted" then
            \* Already aborted, nothing to do.
            skip;
          elsif record.status = "pending" then
            assert FALSE;
          elsif record.status = "committed" then
            \* This must have been at a later epoch.
            assert record.epoch > prevent_epoch;
          elsif record.status = "staging" then
            if record.epoch > prevent_epoch then
              \* Try to prevent at higher epoch.
              goto PreventLoop;
            else
              \* Can abort as result of recovery.
              record.status := "aborted";
            end if;
          end if;
        else
          \* The transaction was implicitly committed.
          if record.status = "aborted" then
            assert FALSE;
          elsif record.status = "pending" then
            assert FALSE;
          elsif record.status = "committed" then
            assert record.epoch = prevent_epoch;
          elsif record.status = "staging" then
            assert record.epoch = prevent_epoch;
            \* Can commit as result of recovery.
            record.status := "committed";
          end if;
        end if;
      end with;
    
  EndRecover:
    skip;
    
end process;  
end algorithm;*)
\* BEGIN TRANSLATION
VARIABLES record, intent_writes, tscache, commit_ack, pc

(* define statement *)
RecordStatuses  == {"pending", "staging", "committed", "aborted"}
RecordCommitted == record.status = "committed"
RecordAborted   == record.status = "aborted"
RecordFinalized == RecordCommitted \/ RecordAborted

TypeInvariant ==
  /\ record \in [status: RecordStatuses, epoch: 0..MAX_EPOCH]
  /\ DOMAIN intent_writes = KEYS
    /\ \A k \in KEYS: intent_writes[k] \in 0..MAX_EPOCH
  /\ DOMAIN tscache = KEYS
    /\ \A k \in KEYS: tscache[k] \in 0..MAX_EPOCH

TemporalTxnRecordProperties ==

  /\ <>[]RecordFinalized

  /\ [](RecordCommitted => []RecordCommitted)
  /\ [](RecordAborted   => []RecordAborted)

  /\ [][record'.epoch >= record.epoch]_record

TemporalIntentProperties ==

  /\ [][\A k \in KEYS: intent_writes'[k] >= intent_writes[k]]_intent_writes

TemporalTSCacheProperties ==

  /\ [][\A k \in KEYS: tscache'[k] >= tscache[k]]_tscache


AckLeadsToCommit == commit_ack ~> RecordCommitted

VARIABLES txn_epoch, to_write, have_staged_record, prevent_epoch, 
          found_writes

vars == << record, intent_writes, tscache, commit_ack, pc, txn_epoch, 
           to_write, have_staged_record, prevent_epoch, found_writes >>

ProcSet == {"committer"} \cup (PREVENTERS)

Init == (* Global variables *)
        /\ record = [status |-> "pending", epoch |-> 0]
        /\ intent_writes = [k \in KEYS |-> 0]
        /\ tscache = [k \in KEYS |-> 0]
        /\ commit_ack = FALSE
        (* Process committer *)
        /\ txn_epoch = 0
        /\ to_write = {}
        /\ have_staged_record = FALSE
        (* Process preventer *)
        /\ prevent_epoch = [self \in PREVENTERS |-> 0]
        /\ found_writes = [self \in PREVENTERS |-> {}]
        /\ pc = [self \in ProcSet |-> CASE self = "committer" -> "ParallelCommitLoop"
                                        [] self \in PREVENTERS -> "PreventLoop"]

ParallelCommitLoop == /\ pc["committer"] = "ParallelCommitLoop"
                      /\ txn_epoch' = txn_epoch + 1
                      /\ to_write' = KEYS
                      /\ have_staged_record' = FALSE
                      /\ IF txn_epoch' > MAX_EPOCH
                            THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                            ELSE /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                      /\ UNCHANGED << record, intent_writes, tscache, 
                                      commit_ack, prevent_epoch, found_writes >>

TryStageWrites == /\ pc["committer"] = "TryStageWrites"
                  /\ IF to_write /= {} \/ ~have_staged_record
                        THEN /\ \/ /\ to_write /= {}
                                   /\ \E key \in to_write:
                                        IF tscache[key] >= txn_epoch
                                           THEN /\ pc' = [pc EXCEPT !["committer"] = "ParallelCommitLoop"]
                                                /\ UNCHANGED << intent_writes, 
                                                                to_write >>
                                           ELSE /\ intent_writes' = [intent_writes EXCEPT ![key] = txn_epoch]
                                                /\ to_write' = to_write \ {key}
                                                /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                                   /\ UNCHANGED <<record, have_staged_record>>
                                \/ /\ ~have_staged_record
                                   /\ have_staged_record' = TRUE
                                   /\ IF record.status = "pending"
                                         THEN /\ record' = [status |-> "staging", epoch |-> txn_epoch]
                                              /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                                         ELSE /\ IF record.status = "staging"
                                                    THEN /\ Assert(record.epoch < txn_epoch, 
                                                                   "Failure of assertion at line 89, column 13.")
                                                         /\ record' = [record EXCEPT !.epoch = txn_epoch]
                                                         /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                                                    ELSE /\ IF record.status = "aborted"
                                                               THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                                                               ELSE /\ IF record.status = "committed"
                                                                          THEN /\ Assert(FALSE, 
                                                                                         "Failure of assertion at line 95, column 13.")
                                                                          ELSE /\ TRUE
                                                                    /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                                                         /\ UNCHANGED record
                                   /\ UNCHANGED <<intent_writes, to_write>>
                        ELSE /\ pc' = [pc EXCEPT !["committer"] = "AckClient"]
                             /\ UNCHANGED << record, intent_writes, to_write, 
                                             have_staged_record >>
                  /\ UNCHANGED << tscache, commit_ack, txn_epoch, 
                                  prevent_epoch, found_writes >>

AckClient == /\ pc["committer"] = "AckClient"
             /\ commit_ack' = TRUE
             /\ pc' = [pc EXCEPT !["committer"] = "ExplicitCommit"]
             /\ UNCHANGED << record, intent_writes, tscache, txn_epoch, 
                             to_write, have_staged_record, prevent_epoch, 
                             found_writes >>

ExplicitCommit == /\ pc["committer"] = "ExplicitCommit"
                  /\ IF record.status = "staging"
                        THEN /\ record' = [record EXCEPT !.status = "committed"]
                        ELSE /\ IF record.status = "committed"
                                   THEN /\ TRUE
                                   ELSE /\ Assert(FALSE, 
                                                  "Failure of assertion at line 112, column 7.")
                             /\ UNCHANGED record
                  /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                  /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                  txn_epoch, to_write, have_staged_record, 
                                  prevent_epoch, found_writes >>

EndCommitter == /\ pc["committer"] = "EndCommitter"
                /\ TRUE
                /\ pc' = [pc EXCEPT !["committer"] = "Done"]
                /\ UNCHANGED << record, intent_writes, tscache, commit_ack, 
                                txn_epoch, to_write, have_staged_record, 
                                prevent_epoch, found_writes >>

committer == ParallelCommitLoop \/ TryStageWrites \/ AckClient
                \/ ExplicitCommit \/ EndCommitter

PreventLoop(self) == /\ pc[self] = "PreventLoop"
                     /\ found_writes' = [found_writes EXCEPT ![self] = {}]
                     /\ pc' = [pc EXCEPT ![self] = "PushRecord"]
                     /\ UNCHANGED << record, intent_writes, tscache, 
                                     commit_ack, txn_epoch, to_write, 
                                     have_staged_record, prevent_epoch >>

PushRecord(self) == /\ pc[self] = "PushRecord"
                    /\ IF record.status = "pending"
                          THEN /\ record' = [record EXCEPT !.status = "aborted"]
                               /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                               /\ UNCHANGED prevent_epoch
                          ELSE /\ IF record.status = "staging"
                                     THEN /\ prevent_epoch' = [prevent_epoch EXCEPT ![self] = record.epoch]
                                          /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                     ELSE /\ IF record.status = "committed"
                                                THEN /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                ELSE /\ IF record.status = "aborted"
                                                           THEN /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                           ELSE /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                          /\ UNCHANGED prevent_epoch
                               /\ UNCHANGED record
                    /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                    txn_epoch, to_write, have_staged_record, 
                                    found_writes >>

PreventWrites(self) == /\ pc[self] = "PreventWrites"
                       /\ IF found_writes[self] /= KEYS
                             THEN /\ \E key \in KEYS \ found_writes[self]:
                                       LET intent_epoch == intent_writes[key] IN
                                         IF intent_epoch = prevent_epoch[self]
                                            THEN /\ found_writes' = [found_writes EXCEPT ![self] = found_writes[self] \union {key}]
                                                 /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                                 /\ UNCHANGED tscache
                                            ELSE /\ tscache' = [tscache EXCEPT ![key] = Max(tscache[key], prevent_epoch[self])]
                                                 /\ pc' = [pc EXCEPT ![self] = "RecoverRecord"]
                                                 /\ UNCHANGED found_writes
                             ELSE /\ pc' = [pc EXCEPT ![self] = "RecoverRecord"]
                                  /\ UNCHANGED << tscache, found_writes >>
                       /\ UNCHANGED << record, intent_writes, commit_ack, 
                                       txn_epoch, to_write, have_staged_record, 
                                       prevent_epoch >>

RecoverRecord(self) == /\ pc[self] = "RecoverRecord"
                       /\ LET prevented == found_writes[self] /= KEYS IN
                            IF prevented
                               THEN /\ IF record.status = "aborted"
                                          THEN /\ TRUE
                                               /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                               /\ UNCHANGED record
                                          ELSE /\ IF record.status = "pending"
                                                     THEN /\ Assert(FALSE, 
                                                                    "Failure of assertion at line 165, column 13.")
                                                          /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                          /\ UNCHANGED record
                                                     ELSE /\ IF record.status = "committed"
                                                                THEN /\ Assert(record.epoch > prevent_epoch[self], 
                                                                               "Failure of assertion at line 168, column 13.")
                                                                     /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                                     /\ UNCHANGED record
                                                                ELSE /\ IF record.status = "staging"
                                                                           THEN /\ IF record.epoch > prevent_epoch[self]
                                                                                      THEN /\ pc' = [pc EXCEPT ![self] = "PreventLoop"]
                                                                                           /\ UNCHANGED record
                                                                                      ELSE /\ record' = [record EXCEPT !.status = "aborted"]
                                                                                           /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                                           ELSE /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                                                /\ UNCHANGED record
                               ELSE /\ IF record.status = "aborted"
                                          THEN /\ Assert(FALSE, 
                                                         "Failure of assertion at line 181, column 13.")
                                               /\ UNCHANGED record
                                          ELSE /\ IF record.status = "pending"
                                                     THEN /\ Assert(FALSE, 
                                                                    "Failure of assertion at line 183, column 13.")
                                                          /\ UNCHANGED record
                                                     ELSE /\ IF record.status = "committed"
                                                                THEN /\ Assert(record.epoch = prevent_epoch[self], 
                                                                               "Failure of assertion at line 185, column 13.")
                                                                     /\ UNCHANGED record
                                                                ELSE /\ IF record.status = "staging"
                                                                           THEN /\ Assert(record.epoch = prevent_epoch[self], 
                                                                                          "Failure of assertion at line 187, column 13.")
                                                                                /\ record' = [record EXCEPT !.status = "committed"]
                                                                           ELSE /\ TRUE
                                                                                /\ UNCHANGED record
                                    /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                       /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                       txn_epoch, to_write, have_staged_record, 
                                       prevent_epoch, found_writes >>

EndRecover(self) == /\ pc[self] = "EndRecover"
                    /\ TRUE
                    /\ pc' = [pc EXCEPT ![self] = "Done"]
                    /\ UNCHANGED << record, intent_writes, tscache, commit_ack, 
                                    txn_epoch, to_write, have_staged_record, 
                                    prevent_epoch, found_writes >>

preventer(self) == PreventLoop(self) \/ PushRecord(self)
                      \/ PreventWrites(self) \/ RecoverRecord(self)
                      \/ EndRecover(self)

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
\* Last modified Wed May 15 17:47:28 EDT 2019 by nathan
\* Created Mon May 13 10:03:40 EDT 2019 by nathan
