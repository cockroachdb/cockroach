-------------------------- MODULE ParallelCommits --------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences
CONSTANTS KEYS, PREVENTERS, MAX_ATTEMPTS
ASSUME Cardinality(KEYS) > 0

Max(a, b) == IF a > b THEN a ELSE b

(*--algorithm parallelcommits
variables
  record = [status |-> "pending", epoch |-> 0, ts |-> 0];
  intent_writes = [k \in KEYS |-> <<0, 0>>];
  tscache = [k \in KEYS |-> 0];
  commit_ack = FALSE;
  
define
  RecordStatuses  == {"pending", "staging", "committed", "aborted"}
  RecordCommitted == record.status = "committed"
  RecordAborted   == record.status = "aborted"
  RecordFinalized == RecordCommitted \/ RecordAborted

  TypeInvariants ==
    /\ record \in [status: RecordStatuses, epoch: 0..MAX_ATTEMPTS, ts: 0..MAX_ATTEMPTS]
    /\ DOMAIN intent_writes = KEYS
      /\ \A k \in KEYS: intent_writes[k] \in [1..2 -> 0..MAX_ATTEMPTS]
    /\ DOMAIN tscache = KEYS
      /\ \A k \in KEYS: tscache[k] \in 0..MAX_ATTEMPTS
    
  TemporalTxnRecordProperties ==
    \* The txn record always ends with either a COMMITTED or ABORTED status.
    /\ <>[]RecordFinalized
    \* Once the txn record moves to a finalized status, it stays there.
    /\ [](RecordCommitted => []RecordCommitted)
    /\ [](RecordAborted   => []RecordAborted)
    \* The txn record's epoch must always grow.
    /\ [][record'.epoch >= record.epoch]_record
    \* The txn record's timestamp must always grow.
    /\ [][record'.ts >= record.ts]_record
    
  TemporalIntentProperties ==
    \* Intent writes' epochs must always grow.
    /\ [][\A k \in KEYS: intent_writes'[k][1] >= intent_writes[k][1]]_intent_writes
    \* Intent writes' timestamps must always grow.
    /\ [][\A k \in KEYS: intent_writes'[k][2] >= intent_writes[k][2]]_intent_writes
    
  TemporalTSCacheProperties ==
    \* The timestamp cache always advances.
    /\ [][\A k \in KEYS: tscache'[k] >= tscache[k]]_tscache
    
  \* If the client is acked, the record should eventually be committed.
  AckLeadsToCommit == commit_ack ~> RecordCommitted
end define;

process committer = "committer"
variables
  txn_epoch = 1;
  txn_ts = 1;
  attempt = 0;
  to_write = KEYS;
  have_staged_record = FALSE;
begin
  \* Attempt to perform all intent writes and
  \* stage the transaction record in parallel.
  StageWrites:
    \* Give up after MAX_ATTEMPTS attempts.
    attempt := attempt + 1;
    if attempt > MAX_ATTEMPTS then
      goto EndCommitter;
    end if;
  
    TryStageWrites:
      while to_write /= {} \/ ~have_staged_record do
        either
          await to_write /= {};
          with key \in to_write do
            if tscache[key] >= txn_ts then
              \* Write prevented.
              either
                \* Successful refresh. Try again at new epoch.
                \* No need to re-write existing intents at new timestamp.
                txn_ts := txn_ts + 1;
                have_staged_record := FALSE;
                goto StageWrites;
              or
                \* Failed refresh. Try again at new epoch.
                \* Must re-write all intents at new epoch.
                txn_epoch := txn_epoch + 1;
                txn_ts := txn_ts + 1;
                to_write := KEYS;
                have_staged_record := FALSE;
                goto StageWrites;
              end either;
            else
              \* Write successful.
              intent_writes[key] := <<txn_epoch, txn_ts>>;
              to_write := to_write \ {key};
            end if;
          end with;
        or
          await ~have_staged_record;
          have_staged_record := TRUE;
          if record.status = "pending" then
            \* Move to staging status.
            record := [status |-> "staging", epoch |-> txn_epoch, ts |-> txn_ts];
          elsif record.status = "staging" then
            \* Bump record timestamp and maybe epoch.
            assert record.epoch <= txn_epoch /\ record.ts < txn_ts;
            record := [status |-> "staging", epoch |-> txn_epoch, ts |-> txn_ts];
          elsif record.status = "aborted" then
            \* Aborted before STAGING transaction record.
            goto EndCommitter;
          elsif record.status = "committed" then
            \* Should not already be committed.
            assert FALSE;
          end if;
        end either
      end while;
      
  \* Ack the client now that all writes have succeeded
  \* and the transaction is implicitly committed.
  AckClient:
    commit_ack := TRUE;
  
  \* Now that the transaction is implicitly committed,
  \* asynchronously make the commit explicit.
  ExplicitCommit:
    if record.status = "staging" then
      \* Make implicit commit explicit.
      record.status := "committed";
    elsif record.status = "committed" then
      \* Already committed by a recovery process.
      skip;
    else
      \* Should not be pending or aborted at this point.
      assert FALSE;
    end if;
    
  EndCommitter:
    skip;
  
end process;

fair process preventer \in PREVENTERS
variable
  prevent_epoch = 0;
  prevent_ts = 0;
  found_writes = {};
begin
  PreventLoop:
    found_writes := {};
    
    \* Push the transaction record to determine its
    \* status, epoch, and timestamp.
    PushRecord:
      if record.status = "pending" then
        \* Transaction not yet staged, abort.
        record.status := "aborted";
        goto EndRecover;
      elsif record.status = "staging" then
        \* Transaction staging, kick off recovery process.
        prevent_epoch := record.epoch;
        prevent_ts := record.ts;
      elsif record.status \in {"committed", "aborted"} then
        \* Already finalized, nothing to do.
        goto EndRecover;
      end if;
      
    \* Attempt to prevent any of its in-flight intent writes.
    PreventWrites:
      while found_writes /= KEYS do
        with key \in KEYS \ found_writes do
          with intent = intent_writes[key];
               intent_epoch = intent[1];
               intent_ts    = intent[2] do
            if intent_epoch = prevent_epoch /\ intent_ts <= prevent_ts then
              \* Intent found. Could not prevent.
              found_writes := found_writes \union {key}
            else 
              \* Intent missing. Prevent.
              tscache[key] := Max(tscache[key], prevent_ts);
              goto RecoverRecord;
            end if;
          end with;
        end with;
      end while;
    
    \* Recover based on whether any of its in-flight writes
    \* were prevented. If not, the transaction is already
    \* implicitly committed.
    RecoverRecord:
      with prevented = found_writes /= KEYS do
        if prevented then
          with legal_change = record.epoch >= prevent_epoch 
                           /\ record.ts    > prevent_ts do
            if record.status = "aborted" then
              \* Already aborted, nothing to do.
              skip;
            elsif record.status = "pending" then
              \* Should not be pending at this point.
              assert FALSE;
            elsif record.status = "committed" then
              \* This must have been at a later timestamp.
              assert legal_change;
            elsif record.status = "staging" then
              if legal_change then
                \* Try to prevent at higher epoch.
                goto PreventLoop;
              else
                \* Can abort as result of recovery.
                record.status := "aborted";
              end if;
            end if;
          end with;
        else
          \* The transaction was implicitly committed.
          if record.status \in {"pending", "aborted"} then
            \* Should not be pending or aborted at this point.
            assert FALSE;
          elsif record.status \in {"staging", "committed"} then
            \* The epoch and timestamp should be what we expect.
            assert record.epoch = prevent_epoch;
            assert record.ts    = prevent_ts;
            
            \* Can commit as result of recovery.
            if record.status = "staging" then
              record.status := "committed";
            end if;
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

TypeInvariants ==
  /\ record \in [status: RecordStatuses, epoch: 0..MAX_ATTEMPTS, ts: 0..MAX_ATTEMPTS]
  /\ DOMAIN intent_writes = KEYS
    /\ \A k \in KEYS: intent_writes[k] \in [1..2 -> 0..MAX_ATTEMPTS]
  /\ DOMAIN tscache = KEYS
    /\ \A k \in KEYS: tscache[k] \in 0..MAX_ATTEMPTS

TemporalTxnRecordProperties ==

  /\ <>[]RecordFinalized

  /\ [](RecordCommitted => []RecordCommitted)
  /\ [](RecordAborted   => []RecordAborted)

  /\ [][record'.epoch >= record.epoch]_record

  /\ [][record'.ts >= record.ts]_record

TemporalIntentProperties ==

  /\ [][\A k \in KEYS: intent_writes'[k][1] >= intent_writes[k][1]]_intent_writes

  /\ [][\A k \in KEYS: intent_writes'[k][2] >= intent_writes[k][2]]_intent_writes

TemporalTSCacheProperties ==

  /\ [][\A k \in KEYS: tscache'[k] >= tscache[k]]_tscache


AckLeadsToCommit == commit_ack ~> RecordCommitted

VARIABLES txn_epoch, txn_ts, attempt, to_write, have_staged_record, 
          prevent_epoch, prevent_ts, found_writes

vars == << record, intent_writes, tscache, commit_ack, pc, txn_epoch, txn_ts, 
           attempt, to_write, have_staged_record, prevent_epoch, prevent_ts, 
           found_writes >>

ProcSet == {"committer"} \cup (PREVENTERS)

Init == (* Global variables *)
        /\ record = [status |-> "pending", epoch |-> 0, ts |-> 0]
        /\ intent_writes = [k \in KEYS |-> <<0, 0>>]
        /\ tscache = [k \in KEYS |-> 0]
        /\ commit_ack = FALSE
        (* Process committer *)
        /\ txn_epoch = 1
        /\ txn_ts = 1
        /\ attempt = 0
        /\ to_write = KEYS
        /\ have_staged_record = FALSE
        (* Process preventer *)
        /\ prevent_epoch = [self \in PREVENTERS |-> 0]
        /\ prevent_ts = [self \in PREVENTERS |-> 0]
        /\ found_writes = [self \in PREVENTERS |-> {}]
        /\ pc = [self \in ProcSet |-> CASE self = "committer" -> "StageWrites"
                                        [] self \in PREVENTERS -> "PreventLoop"]

StageWrites == /\ pc["committer"] = "StageWrites"
               /\ attempt' = attempt + 1
               /\ IF attempt' > MAX_ATTEMPTS
                     THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                     ELSE /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
               /\ UNCHANGED << record, intent_writes, tscache, commit_ack, 
                               txn_epoch, txn_ts, to_write, have_staged_record, 
                               prevent_epoch, prevent_ts, found_writes >>

TryStageWrites == /\ pc["committer"] = "TryStageWrites"
                  /\ IF to_write /= {} \/ ~have_staged_record
                        THEN /\ \/ /\ to_write /= {}
                                   /\ \E key \in to_write:
                                        IF tscache[key] >= txn_ts
                                           THEN /\ \/ /\ txn_ts' = txn_ts + 1
                                                      /\ have_staged_record' = FALSE
                                                      /\ pc' = [pc EXCEPT !["committer"] = "StageWrites"]
                                                      /\ UNCHANGED <<txn_epoch, to_write>>
                                                   \/ /\ txn_epoch' = txn_epoch + 1
                                                      /\ txn_ts' = txn_ts + 1
                                                      /\ to_write' = KEYS
                                                      /\ have_staged_record' = FALSE
                                                      /\ pc' = [pc EXCEPT !["committer"] = "StageWrites"]
                                                /\ UNCHANGED intent_writes
                                           ELSE /\ intent_writes' = [intent_writes EXCEPT ![key] = <<txn_epoch, txn_ts>>]
                                                /\ to_write' = to_write \ {key}
                                                /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                                                /\ UNCHANGED << txn_epoch, 
                                                                txn_ts, 
                                                                have_staged_record >>
                                   /\ UNCHANGED record
                                \/ /\ ~have_staged_record
                                   /\ have_staged_record' = TRUE
                                   /\ IF record.status = "pending"
                                         THEN /\ record' = [status |-> "staging", epoch |-> txn_epoch, ts |-> txn_ts]
                                              /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                                         ELSE /\ IF record.status = "staging"
                                                    THEN /\ Assert(record.epoch <= txn_epoch /\ record.ts < txn_ts, 
                                                                   "Failure of assertion at line 106, column 13.")
                                                         /\ record' = [status |-> "staging", epoch |-> txn_epoch, ts |-> txn_ts]
                                                         /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                                                    ELSE /\ IF record.status = "aborted"
                                                               THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                                                               ELSE /\ IF record.status = "committed"
                                                                          THEN /\ Assert(FALSE, 
                                                                                         "Failure of assertion at line 113, column 13.")
                                                                          ELSE /\ TRUE
                                                                    /\ pc' = [pc EXCEPT !["committer"] = "TryStageWrites"]
                                                         /\ UNCHANGED record
                                   /\ UNCHANGED <<intent_writes, txn_epoch, txn_ts, to_write>>
                        ELSE /\ pc' = [pc EXCEPT !["committer"] = "AckClient"]
                             /\ UNCHANGED << record, intent_writes, txn_epoch, 
                                             txn_ts, to_write, 
                                             have_staged_record >>
                  /\ UNCHANGED << tscache, commit_ack, attempt, prevent_epoch, 
                                  prevent_ts, found_writes >>

AckClient == /\ pc["committer"] = "AckClient"
             /\ commit_ack' = TRUE
             /\ pc' = [pc EXCEPT !["committer"] = "ExplicitCommit"]
             /\ UNCHANGED << record, intent_writes, tscache, txn_epoch, txn_ts, 
                             attempt, to_write, have_staged_record, 
                             prevent_epoch, prevent_ts, found_writes >>

ExplicitCommit == /\ pc["committer"] = "ExplicitCommit"
                  /\ IF record.status = "staging"
                        THEN /\ record' = [record EXCEPT !.status = "committed"]
                        ELSE /\ IF record.status = "committed"
                                   THEN /\ TRUE
                                   ELSE /\ Assert(FALSE, 
                                                  "Failure of assertion at line 134, column 7.")
                             /\ UNCHANGED record
                  /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                  /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                  txn_epoch, txn_ts, attempt, to_write, 
                                  have_staged_record, prevent_epoch, 
                                  prevent_ts, found_writes >>

EndCommitter == /\ pc["committer"] = "EndCommitter"
                /\ TRUE
                /\ pc' = [pc EXCEPT !["committer"] = "Done"]
                /\ UNCHANGED << record, intent_writes, tscache, commit_ack, 
                                txn_epoch, txn_ts, attempt, to_write, 
                                have_staged_record, prevent_epoch, prevent_ts, 
                                found_writes >>

committer == StageWrites \/ TryStageWrites \/ AckClient \/ ExplicitCommit
                \/ EndCommitter

PreventLoop(self) == /\ pc[self] = "PreventLoop"
                     /\ found_writes' = [found_writes EXCEPT ![self] = {}]
                     /\ pc' = [pc EXCEPT ![self] = "PushRecord"]
                     /\ UNCHANGED << record, intent_writes, tscache, 
                                     commit_ack, txn_epoch, txn_ts, attempt, 
                                     to_write, have_staged_record, 
                                     prevent_epoch, prevent_ts >>

PushRecord(self) == /\ pc[self] = "PushRecord"
                    /\ IF record.status = "pending"
                          THEN /\ record' = [record EXCEPT !.status = "aborted"]
                               /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                               /\ UNCHANGED << prevent_epoch, prevent_ts >>
                          ELSE /\ IF record.status = "staging"
                                     THEN /\ prevent_epoch' = [prevent_epoch EXCEPT ![self] = record.epoch]
                                          /\ prevent_ts' = [prevent_ts EXCEPT ![self] = record.ts]
                                          /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                     ELSE /\ IF record.status \in {"committed", "aborted"}
                                                THEN /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                ELSE /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                          /\ UNCHANGED << prevent_epoch, 
                                                          prevent_ts >>
                               /\ UNCHANGED record
                    /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                    txn_epoch, txn_ts, attempt, to_write, 
                                    have_staged_record, found_writes >>

PreventWrites(self) == /\ pc[self] = "PreventWrites"
                       /\ IF found_writes[self] /= KEYS
                             THEN /\ \E key \in KEYS \ found_writes[self]:
                                       LET intent == intent_writes[key] IN
                                         LET intent_epoch == intent[1] IN
                                           LET intent_ts == intent[2] IN
                                             IF intent_epoch = prevent_epoch[self] /\ intent_ts <= prevent_ts[self]
                                                THEN /\ found_writes' = [found_writes EXCEPT ![self] = found_writes[self] \union {key}]
                                                     /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                                     /\ UNCHANGED tscache
                                                ELSE /\ tscache' = [tscache EXCEPT ![key] = Max(tscache[key], prevent_ts[self])]
                                                     /\ pc' = [pc EXCEPT ![self] = "RecoverRecord"]
                                                     /\ UNCHANGED found_writes
                             ELSE /\ pc' = [pc EXCEPT ![self] = "RecoverRecord"]
                                  /\ UNCHANGED << tscache, found_writes >>
                       /\ UNCHANGED << record, intent_writes, commit_ack, 
                                       txn_epoch, txn_ts, attempt, to_write, 
                                       have_staged_record, prevent_epoch, 
                                       prevent_ts >>

RecoverRecord(self) == /\ pc[self] = "RecoverRecord"
                       /\ LET prevented == found_writes[self] /= KEYS IN
                            IF prevented
                               THEN /\ LET legal_change ==    record.epoch >= prevent_epoch[self]
                                                           /\ record.ts    > prevent_ts[self] IN
                                         IF record.status = "aborted"
                                            THEN /\ TRUE
                                                 /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                 /\ UNCHANGED record
                                            ELSE /\ IF record.status = "pending"
                                                       THEN /\ Assert(FALSE, 
                                                                      "Failure of assertion at line 199, column 15.")
                                                            /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                            /\ UNCHANGED record
                                                       ELSE /\ IF record.status = "committed"
                                                                  THEN /\ Assert(legal_change, 
                                                                                 "Failure of assertion at line 202, column 15.")
                                                                       /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                                       /\ UNCHANGED record
                                                                  ELSE /\ IF record.status = "staging"
                                                                             THEN /\ IF legal_change
                                                                                        THEN /\ pc' = [pc EXCEPT ![self] = "PreventLoop"]
                                                                                             /\ UNCHANGED record
                                                                                        ELSE /\ record' = [record EXCEPT !.status = "aborted"]
                                                                                             /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                                             ELSE /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                                                                                  /\ UNCHANGED record
                               ELSE /\ IF record.status \in {"pending", "aborted"}
                                          THEN /\ Assert(FALSE, 
                                                         "Failure of assertion at line 217, column 13.")
                                               /\ UNCHANGED record
                                          ELSE /\ IF record.status \in {"staging", "committed"}
                                                     THEN /\ Assert(record.epoch = prevent_epoch[self], 
                                                                    "Failure of assertion at line 220, column 13.")
                                                          /\ Assert(record.ts    = prevent_ts[self], 
                                                                    "Failure of assertion at line 221, column 13.")
                                                          /\ IF record.status = "staging"
                                                                THEN /\ record' = [record EXCEPT !.status = "committed"]
                                                                ELSE /\ TRUE
                                                                     /\ UNCHANGED record
                                                     ELSE /\ TRUE
                                                          /\ UNCHANGED record
                                    /\ pc' = [pc EXCEPT ![self] = "EndRecover"]
                       /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                       txn_epoch, txn_ts, attempt, to_write, 
                                       have_staged_record, prevent_epoch, 
                                       prevent_ts, found_writes >>

EndRecover(self) == /\ pc[self] = "EndRecover"
                    /\ TRUE
                    /\ pc' = [pc EXCEPT ![self] = "Done"]
                    /\ UNCHANGED << record, intent_writes, tscache, commit_ack, 
                                    txn_epoch, txn_ts, attempt, to_write, 
                                    have_staged_record, prevent_epoch, 
                                    prevent_ts, found_writes >>

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
\* Last modified Thu May 16 21:14:24 EDT 2019 by nathan
\* Created Mon May 13 10:03:40 EDT 2019 by nathan
