-------------------------- MODULE ParallelCommits --------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences
CONSTANTS KEYS, PREVENTERS, MAX_ATTEMPTS
ASSUME Cardinality(KEYS) > 0
ASSUME Cardinality(PREVENTERS) > 0
ASSUME MAX_ATTEMPTS > 0

(*************************************************************************)
(* Parallel commits is the process in which a transaction can perform    *)
(* all writes and mark its transaction record as committed in a single   *)
(* round of distributed consensus. The primary source of documentation   *)
(* on this process lives in pkg/kv/txn_interceptor_committer.go and in   *)
(* docs/RFCS/20180324_parallel_commit.md.                                *)
(*                                                                       *)
(* This spec is modeling a transaction attempting a parallel commit and  *)
(* set of concurrent transactions attempting to "recover" from the       *)
(* parallel commit at the same time. The transaction performing the      *)
(* commit can fail at any time but the concurrent transactions will      *)
(* always eventually complete.                                           *)
(*                                                                       *)
(*                                                                       *)
(* The spec asserts the following safety properties:                     *)
(* - the transaction record makes only valid state transitions.          *)
(* - if implicitly committed, the commit must eventually become made     *)
(*   explicit by moving the transaction record to the "committed" state. *)
(* - if the commit to acknowledged to the client, the commit must        *)
(*   eventually become made explicit by moving the transaction record to *)
(*   the "committed" state.                                              *)
(*                                                                       *)
(* The spec asserts the following liveness properties:                   *)
(* - the transaction record is eventually aborted or committed.          *)
(* - all of the transaction's intents are eventually resolved.           *)
(*                                                                       *)
(*                                                                       *)
(* The "committer" process corresponds to logic in the following files:  *)
(* - pkg/kv/txn_interceptor_committer.go                                 *)
(* - pkg/kv/txn_interceptor_pipeliner.go                                 *)
(* - pkg/storage/batcheval/cmd_end_transaction.go                        *)
(* - pkg/storage/batcheval/cmd_query_intent.go                           *)
(* - pkg/storage/replica_tscache.go                                      *)
(*                                                                       *)
(* The "preventer" process corresponds to logic in the following files:  *)
(* - pkg/storage/txnrecovery/manager.go                                  *)
(* - pkg/storage/batcheval/cmd_push_txn.go                               *)
(* - pkg/storage/batcheval/cmd_query_intent.go                           *)
(* - pkg/storage/batcheval/cmd_recover_txt.go                            *)
(* - pkg/storage/replica_tscache.go                                      *)
(*************************************************************************)

(*--algorithm parallelcommits
variables
  record = [status |-> "pending", epoch |-> 0, ts |-> 0];
  intent_writes = [k \in KEYS |-> [epoch |-> 0, ts |-> 0, resolved |-> FALSE]];
  tscache = [k \in KEYS |-> 0];
  commit_ack = FALSE;

define
  \* Simulates a QueryIntent request, taking care to model the exact
  \* condition in which the request considers an intent to be found.
  QueryIntent(key, query_epoch, query_ts) ==
    LET
      intent == intent_writes[key]
    IN
      /\ intent.epoch = query_epoch
      /\ intent.ts <= query_ts
      \* The loss of information from intent resolution that is reflected
      \* here has a few unfortunate effects:
      \* 1. it is ambiguous whether a QueryIntent issued in parallel with
      \*    a parallel commit is due to a missing intent or intent resolution
      \*    after transaction finalization. In order to reduce this ambiguity,
      \*    we're forced to query the transaction record after we detect this
      \*    condition (#37866).
      \* 2. a transaction recovery process that detects a missing intent cannot
      \*    definitively conclude that the transaction being recovered was not
      \*    committed without checking the transaction record first (#37784).
      \*
      \* We could address part of this by storing transaction IDs in resolved
      \* values and allowing QueryIntent to correctly identify resolved values
      \* that correspond to the desired intent (i.e. removing this condition).
      \* However, there will still be complications with value GC.
      /\ intent.resolved = FALSE

  RecordStatuses  == {"pending", "staging", "committed", "aborted"}
  RecordStaging   == record.status = "staging"
  RecordCommitted == record.status = "committed"
  RecordAborted   == record.status = "aborted"
  RecordFinalized == RecordCommitted \/ RecordAborted

  ImplicitlyCommitted ==
    /\ RecordStaging
    /\ \A k \in KEYS:
      /\ intent_writes[k].epoch = record.epoch
      /\ intent_writes[k].ts   <= record.ts
  ExplicitlyCommitted == RecordCommitted
  Committed           == ImplicitlyCommitted \/ ExplicitlyCommitted

  TypeInvariants ==
    /\ record \in [status: RecordStatuses, epoch: 0..MAX_ATTEMPTS, ts: 0..MAX_ATTEMPTS]
    /\ DOMAIN intent_writes = KEYS
      /\ \A k \in KEYS:
        intent_writes[k] \in [
          epoch:    0..MAX_ATTEMPTS, 
          ts:       0..MAX_ATTEMPTS, 
          resolved: BOOLEAN
        ]
    /\ DOMAIN tscache = KEYS
      /\ \A k \in KEYS: tscache[k] \in 0..MAX_ATTEMPTS

  TemporalTxnRecordProperties ==
    \* The txn record always ends with either a COMMITTED or ABORTED status.
    /\ <>[]RecordFinalized
    \* Once the txn record moves to a finalized status, it stays there.
    /\ [](RecordCommitted => []RecordCommitted)
    /\ [](RecordAborted   => []RecordAborted)
    \* Once the txn is committed, it remains committed.
    /\ [](Committed => []Committed)
    \* The txn record's epoch must always grow.
    /\ [][record'.epoch >= record.epoch]_record
    \* The txn record's timestamp must always grow.
    /\ [][record'.ts >= record.ts]_record

  TemporalIntentProperties ==
    \* Intent writes' epochs must always grow.
    /\ [][\A k \in KEYS: intent_writes'[k].epoch >= intent_writes[k].epoch]_intent_writes
    \* Intent writes' timestamps must always grow.
    /\ [][\A k \in KEYS: intent_writes'[k].ts >= intent_writes[k].ts]_intent_writes
    \* All intents are eventually resolved and stay resolved.
    /\ <>[](\A k \in KEYS: intent_writes[k].resolved)

  TemporalTSCacheProperties ==
    \* The timestamp cache always advances.
    /\ [][\A k \in KEYS: tscache'[k] >= tscache[k]]_tscache

  \* If the transaction ever becomes implicitly committed, it should
  \* eventually become explicitly committed.
  ImplicitCommitLeadsToExplicitCommit == ImplicitlyCommitted ~> ExplicitlyCommitted

  \* If the client is acked, the transaction must be committed.
  AckImpliesCommit == commit_ack => Committed

  \* If the client is acked, the record should eventually be explicitly committed.
  AckLeadsToExplicitCommit == commit_ack ~> ExplicitlyCommitted
end define;

\* Give up after MAX_ATTEMPTS attempts. This bounds the state space for the
\* spec and ensures that it terminates. A real transaction coordinator will not
\* give up after a certain number of attempts. However, real transactions will
\* probabilistically terminate because concurrent transactions will not attempt
\* to recover a parallel commit (i.e. serve as a "preventer" process) until the
\* parallel committing transaction's heartbeat expires.
macro maybe_abandon_retry()
begin
  if attempt > MAX_ATTEMPTS then
    goto EndCommitter;
  end if;
end macro;

process committer = "committer"
variables
  \* -- constants --
  \* Represents keys that are written before the final Batch.
  pipelined_keys \in SUBSET KEYS;
  \* Represents keys that are written in the final Batch.
  parallel_keys = KEYS \ pipelined_keys;

  \* -- variables --
  attempt   = 1;
  txn_epoch = 0;
  txn_ts    = 0;
  to_write  = {};
  to_check  = {};
  have_staging_record = FALSE;
begin
  \* Begin a new transaction epoch.
  BeginTxnEpoch:
    txn_epoch := txn_epoch + 1;
    txn_ts := txn_ts + 1;
    to_write := pipelined_keys;
    maybe_abandon_retry();

  \* Attempt to perform all pipelined intent writes. These are writes that
  \* occur before the final Batch containing the EndTransaction request.
  \* These writes are ordered, but it's more hassle than it's worth to model
  \* them that way.
  PipelineWrites:
    while to_write /= {} do
      with key \in to_write do
        to_write := to_write \ {key};
        if intent_writes[key].resolved then
          \* Can't write over resolved write. In reality, this would result
          \* in laying down an (uncommitable) intent at a higher timestamp
          \* and returning a WriteTooOld error. For the sake of this model,
          \* we don't write anything. The pre-commit QueryIntent sent to
          \* this key during the parallel commit will fail.
        elsif tscache[key] >= txn_ts then
          \* Write prevented. This shouldn't happen.
          assert FALSE;
        else
          either
            \* Async consensus successful.
            intent_writes[key] := [
              epoch    |-> txn_epoch,
              ts       |-> txn_ts,
              resolved |-> FALSE
            ];
          or
            \* Async consensus unsuccessful. Should be
            \* discovered by a pre-commit QueryIntent.
            skip;
          end either;
        end if;
      end with;
    end while;

  \* Attempt to perform all final-batch intent writes, query all pipelined
  \* writes, and stage the transaction record in parallel.
  StageWritesAndRecord:
    to_write := parallel_keys;
    to_check := pipelined_keys;
    have_staging_record := FALSE;
    maybe_abandon_retry();

    StageWritesAndRecordLoop:
      while to_check /= {} \/ to_write /= {} \/ ~have_staging_record do
        either
          await to_check /= {};
          QueryPipelinedWrite:
            with key \in to_check do
              if QueryIntent(key, txn_epoch, txn_ts) then
                \* Intent found. Pipelined write succeeded.
                to_check := to_check \ {key}
              else
                \* Intent missing. Pipelined write failed.
                \* Check the transaction record to see whether it has already
                \* been finalized using a QueryTxn request. This would indicate
                \* that the missing intent is due to intent resolution.
                if record.status \in {"pending", "staging"} then
                  \* Unambiguously not finalized. Perform a transaction restart
                  \* at new epoch.
                  attempt := attempt + 1;
                  goto BeginTxnEpoch;
                elsif record.status = "aborted" then
                  \* Unambiguously aborted here, but in the implementation this is
                  \* ambiguous because "aborted" may indicate an aborted record or
                  \* a committed record that was GCed.
                  goto EndCommitter;
                elsif record.status = "committed" then
                  \* Unambiguously committed.
                  goto AckClient;
                end if;
              end if;
            end with;
        or
          await to_write /= {};
          ParallelWrite:
            with key \in to_write,
                 cur_intent = intent_writes[key] do
              to_write := to_write \ {key};
              if cur_intent.epoch = txn_epoch then
                \* Write already succeeded before refresh. Writes should be idempotent,
                \* so there's nothing to do. In practice, this is not strictly true (e.g.
                \* after intents are resolved), which is why we currently reject retry
                \* attempts that would rely on idempotence with MixedSuccessErrors.
              elsif tscache[key] >= txn_ts \/ cur_intent.resolved then
                \* Write prevented.
                either
                  \* Successful refresh. Try again at same epoch.
                  \* No need to re-write existing intents at new timestamp.
                  txn_ts := txn_ts + 1;
                  attempt := attempt + 1;
                  goto StageWritesAndRecord;
                or
                  \* Failed refresh. Try again at new epoch.
                  \* Must re-write all intents at new epoch.
                  attempt := attempt + 1;
                  goto BeginTxnEpoch;
                end either;
              else
                \* Write successful.
                intent_writes[key] := [
                  epoch    |-> txn_epoch,
                  ts       |-> txn_ts,
                  resolved |-> FALSE
                ];
              end if;
            end with;
        or
          await ~have_staging_record;
          StageRecord:
            have_staging_record := TRUE;
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
    assert Committed;
    commit_ack := TRUE;

  \* Now that the transaction is implicitly committed,
  \* asynchronously make the commit explicit.
  AsyncExplicitlyCommitted:
    if record.status = "staging" then
      assert ImplicitlyCommitted;
      \* Make implicit commit explicit.
      record.status := "committed";
    elsif record.status = "committed" then
      \* Already committed by a recovery process.
      skip;
    else
      \* Should not be pending or aborted at this point.
      assert FALSE;
    end if;

  \* Now that the commit is explicit, asynchronously resolve
  \* all intents. Re-use the to_write variable for convenience.
  to_write := KEYS;
  AsyncResolveIntents:
    while to_write /= {} do
      with key \in to_write do
        if ~intent_writes[key].resolved then
          intent_writes[key].resolved := TRUE;
        end if;
        to_write := to_write \ {key};
      end with;
    end while;

  EndCommitter:
    skip;

end process;

fair process preventer \in PREVENTERS
variable
  prevent_epoch = 0;
  prevent_ts    = 0;
  found_writes  = {};
  to_resolve    = KEYS;
begin
  PreventLoop:
    found_writes := {};

    \* Push the transaction record to determine its
    \* status, epoch, and timestamp.
    PushRecord:
      if record.status = "pending" then
        \* Transaction not yet staged, abort.
        record.status := "aborted";
        goto ResolveIntents;
      elsif record.status = "staging" then
        \* Transaction staging, kick off recovery process.
        prevent_epoch := record.epoch;
        prevent_ts := record.ts;
      elsif record.status \in {"committed", "aborted"} then
        \* Already finalized, nothing to do.
        goto ResolveIntents;
      end if;

    \* Attempt to prevent any of its in-flight intent writes.
    PreventWrites:
      while found_writes /= KEYS do
        with key \in KEYS \ found_writes do
          if QueryIntent(key, prevent_epoch, prevent_ts) then
            \* Intent found. Could not prevent.
            found_writes := found_writes \union {key}
          else
            \* Intent missing. Prevent.
            if tscache[key] < prevent_ts then
              tscache[key] := prevent_ts;
            end if;
            goto RecoverRecord;
          end if;
        end with;
      end while;

    \* Recover based on whether any of its in-flight writes
    \* were prevented. If not, the transaction is already
    \* implicitly committed.
    RecoverRecord:
      with prevented = found_writes /= KEYS do
        if prevented then
          with legal_change = record.epoch >= prevent_epoch
                           /\ record.ts    >  prevent_ts do
            if record.status = "aborted" then
              \* Already aborted, nothing to do.
              skip;
            elsif record.status = "committed" then
              \* Already committed, nothing to do.
              skip;
            elsif record.status = "pending" then
              \* Should not be pending at this point.
              assert FALSE;
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
              assert ImplicitlyCommitted;
              record.status := "committed";
            end if;
          end if;
        end if;
      end with;

  \* Now that the transaction is finalized, synchronously resolve
  \* all of its intents. After this point, the conflicting transaction
  \* can return to doing whatever it was doing.
  ResolveIntents:
    while to_resolve /= {} do
      with key \in to_resolve do
        if ~intent_writes[key].resolved then
          intent_writes[key].resolved := TRUE;
        end if;
        to_resolve := to_resolve \ {key};
      end with;
    end while;

end process;
end algorithm;*)
\* BEGIN TRANSLATION - the hash of the PCal code: PCal-7847e3ffca2156d2f95a169911409dbb
VARIABLES record, intent_writes, tscache, commit_ack, pc

(* define statement *)
QueryIntent(key, query_epoch, query_ts) ==
  LET
    intent == intent_writes[key]
  IN
    /\ intent.epoch = query_epoch
    /\ intent.ts <= query_ts















    /\ intent.resolved = FALSE

RecordStatuses  == {"pending", "staging", "committed", "aborted"}
RecordStaging   == record.status = "staging"
RecordCommitted == record.status = "committed"
RecordAborted   == record.status = "aborted"
RecordFinalized == RecordCommitted \/ RecordAborted

ImplicitlyCommitted ==
  /\ RecordStaging
  /\ \A k \in KEYS:
    /\ intent_writes[k].epoch = record.epoch
    /\ intent_writes[k].ts   <= record.ts
ExplicitlyCommitted == RecordCommitted
Committed           == ImplicitlyCommitted \/ ExplicitlyCommitted

TypeInvariants ==
  /\ record \in [status: RecordStatuses, epoch: 0..MAX_ATTEMPTS, ts: 0..MAX_ATTEMPTS]
  /\ DOMAIN intent_writes = KEYS
    /\ \A k \in KEYS:
      intent_writes[k] \in [
        epoch:    0..MAX_ATTEMPTS,
        ts:       0..MAX_ATTEMPTS,
        resolved: BOOLEAN
      ]
  /\ DOMAIN tscache = KEYS
    /\ \A k \in KEYS: tscache[k] \in 0..MAX_ATTEMPTS

TemporalTxnRecordProperties ==

  /\ <>[]RecordFinalized

  /\ [](RecordCommitted => []RecordCommitted)
  /\ [](RecordAborted   => []RecordAborted)

  /\ [](Committed => []Committed)

  /\ [][record'.epoch >= record.epoch]_record

  /\ [][record'.ts >= record.ts]_record

TemporalIntentProperties ==

  /\ [][\A k \in KEYS: intent_writes'[k].epoch >= intent_writes[k].epoch]_intent_writes

  /\ [][\A k \in KEYS: intent_writes'[k].ts >= intent_writes[k].ts]_intent_writes

  /\ <>[](\A k \in KEYS: intent_writes[k].resolved)

TemporalTSCacheProperties ==

  /\ [][\A k \in KEYS: tscache'[k] >= tscache[k]]_tscache



ImplicitCommitLeadsToExplicitCommit == ImplicitlyCommitted ~> ExplicitlyCommitted


AckImpliesCommit == commit_ack => Committed


AckLeadsToExplicitCommit == commit_ack ~> ExplicitlyCommitted

VARIABLES pipelined_keys, parallel_keys, attempt, txn_epoch, txn_ts, to_write, 
          to_check, have_staging_record, prevent_epoch, prevent_ts, 
          found_writes, to_resolve

vars == << record, intent_writes, tscache, commit_ack, pc, pipelined_keys, 
           parallel_keys, attempt, txn_epoch, txn_ts, to_write, to_check, 
           have_staging_record, prevent_epoch, prevent_ts, found_writes, 
           to_resolve >>

ProcSet == {"committer"} \cup (PREVENTERS)

Init == (* Global variables *)
        /\ record = [status |-> "pending", epoch |-> 0, ts |-> 0]
        /\ intent_writes = [k \in KEYS |-> [epoch |-> 0, ts |-> 0, resolved |-> FALSE]]
        /\ tscache = [k \in KEYS |-> 0]
        /\ commit_ack = FALSE
        (* Process committer *)
        /\ pipelined_keys \in SUBSET KEYS
        /\ parallel_keys = KEYS \ pipelined_keys
        /\ attempt = 1
        /\ txn_epoch = 0
        /\ txn_ts = 0
        /\ to_write = {}
        /\ to_check = {}
        /\ have_staging_record = FALSE
        (* Process preventer *)
        /\ prevent_epoch = [self \in PREVENTERS |-> 0]
        /\ prevent_ts = [self \in PREVENTERS |-> 0]
        /\ found_writes = [self \in PREVENTERS |-> {}]
        /\ to_resolve = [self \in PREVENTERS |-> KEYS]
        /\ pc = [self \in ProcSet |-> CASE self = "committer" -> "BeginTxnEpoch"
                                        [] self \in PREVENTERS -> "PreventLoop"]

BeginTxnEpoch == /\ pc["committer"] = "BeginTxnEpoch"
                 /\ txn_epoch' = txn_epoch + 1
                 /\ txn_ts' = txn_ts + 1
                 /\ to_write' = pipelined_keys
                 /\ IF attempt > MAX_ATTEMPTS
                       THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                       ELSE /\ pc' = [pc EXCEPT !["committer"] = "PipelineWrites"]
                 /\ UNCHANGED << record, intent_writes, tscache, commit_ack, 
                                 pipelined_keys, parallel_keys, attempt, 
                                 to_check, have_staging_record, prevent_epoch, 
                                 prevent_ts, found_writes, to_resolve >>

PipelineWrites == /\ pc["committer"] = "PipelineWrites"
                  /\ IF to_write /= {}
                        THEN /\ \E key \in to_write:
                                  /\ to_write' = to_write \ {key}
                                  /\ IF intent_writes[key].resolved
                                        THEN /\ UNCHANGED intent_writes
                                        ELSE /\ IF tscache[key] >= txn_ts
                                                   THEN /\ Assert(FALSE, 
                                                                  "Failure of assertion at line 197, column 11.")
                                                        /\ UNCHANGED intent_writes
                                                   ELSE /\ \/ /\ intent_writes' = [intent_writes EXCEPT ![key] =                       [
                                                                                                                   epoch    |-> txn_epoch,
                                                                                                                   ts       |-> txn_ts,
                                                                                                                   resolved |-> FALSE
                                                                                                                 ]]
                                                           \/ /\ TRUE
                                                              /\ UNCHANGED intent_writes
                             /\ pc' = [pc EXCEPT !["committer"] = "PipelineWrites"]
                        ELSE /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecord"]
                             /\ UNCHANGED << intent_writes, to_write >>
                  /\ UNCHANGED << record, tscache, commit_ack, pipelined_keys, 
                                  parallel_keys, attempt, txn_epoch, txn_ts, 
                                  to_check, have_staging_record, prevent_epoch, 
                                  prevent_ts, found_writes, to_resolve >>

StageWritesAndRecord == /\ pc["committer"] = "StageWritesAndRecord"
                        /\ to_write' = parallel_keys
                        /\ to_check' = pipelined_keys
                        /\ have_staging_record' = FALSE
                        /\ IF attempt > MAX_ATTEMPTS
                              THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                              ELSE /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecordLoop"]
                        /\ UNCHANGED << record, intent_writes, tscache, 
                                        commit_ack, pipelined_keys, 
                                        parallel_keys, attempt, txn_epoch, 
                                        txn_ts, prevent_epoch, prevent_ts, 
                                        found_writes, to_resolve >>

StageWritesAndRecordLoop == /\ pc["committer"] = "StageWritesAndRecordLoop"
                            /\ IF to_check /= {} \/ to_write /= {} \/ ~have_staging_record
                                  THEN /\ \/ /\ to_check /= {}
                                             /\ pc' = [pc EXCEPT !["committer"] = "QueryPipelinedWrite"]
                                          \/ /\ to_write /= {}
                                             /\ pc' = [pc EXCEPT !["committer"] = "ParallelWrite"]
                                          \/ /\ ~have_staging_record
                                             /\ pc' = [pc EXCEPT !["committer"] = "StageRecord"]
                                  ELSE /\ pc' = [pc EXCEPT !["committer"] = "AckClient"]
                            /\ UNCHANGED << record, intent_writes, tscache, 
                                            commit_ack, pipelined_keys, 
                                            parallel_keys, attempt, txn_epoch, 
                                            txn_ts, to_write, to_check, 
                                            have_staging_record, prevent_epoch, 
                                            prevent_ts, found_writes, 
                                            to_resolve >>

QueryPipelinedWrite == /\ pc["committer"] = "QueryPipelinedWrite"
                       /\ \E key \in to_check:
                            IF QueryIntent(key, txn_epoch, txn_ts)
                               THEN /\ to_check' = to_check \ {key}
                                    /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecordLoop"]
                                    /\ UNCHANGED attempt
                               ELSE /\ IF record.status \in {"pending", "staging"}
                                          THEN /\ attempt' = attempt + 1
                                               /\ pc' = [pc EXCEPT !["committer"] = "BeginTxnEpoch"]
                                          ELSE /\ IF record.status = "aborted"
                                                     THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                                                     ELSE /\ IF record.status = "committed"
                                                                THEN /\ pc' = [pc EXCEPT !["committer"] = "AckClient"]
                                                                ELSE /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecordLoop"]
                                               /\ UNCHANGED attempt
                                    /\ UNCHANGED to_check
                       /\ UNCHANGED << record, intent_writes, tscache, 
                                       commit_ack, pipelined_keys, 
                                       parallel_keys, txn_epoch, txn_ts, 
                                       to_write, have_staging_record, 
                                       prevent_epoch, prevent_ts, found_writes, 
                                       to_resolve >>

ParallelWrite == /\ pc["committer"] = "ParallelWrite"
                 /\ \E key \in to_write:
                      LET cur_intent == intent_writes[key] IN
                        /\ to_write' = to_write \ {key}
                        /\ IF cur_intent.epoch = txn_epoch
                              THEN /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecordLoop"]
                                   /\ UNCHANGED << intent_writes, attempt, 
                                                   txn_ts >>
                              ELSE /\ IF tscache[key] >= txn_ts \/ cur_intent.resolved
                                         THEN /\ \/ /\ txn_ts' = txn_ts + 1
                                                    /\ attempt' = attempt + 1
                                                    /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecord"]
                                                 \/ /\ attempt' = attempt + 1
                                                    /\ pc' = [pc EXCEPT !["committer"] = "BeginTxnEpoch"]
                                                    /\ UNCHANGED txn_ts
                                              /\ UNCHANGED intent_writes
                                         ELSE /\ intent_writes' = [intent_writes EXCEPT ![key] =                       [
                                                                                                   epoch    |-> txn_epoch,
                                                                                                   ts       |-> txn_ts,
                                                                                                   resolved |-> FALSE
                                                                                                 ]]
                                              /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecordLoop"]
                                              /\ UNCHANGED << attempt, txn_ts >>
                 /\ UNCHANGED << record, tscache, commit_ack, pipelined_keys, 
                                 parallel_keys, txn_epoch, to_check, 
                                 have_staging_record, prevent_epoch, 
                                 prevent_ts, found_writes, to_resolve >>

StageRecord == /\ pc["committer"] = "StageRecord"
               /\ have_staging_record' = TRUE
               /\ IF record.status = "pending"
                     THEN /\ record' = [status |-> "staging", epoch |-> txn_epoch, ts |-> txn_ts]
                          /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecordLoop"]
                     ELSE /\ IF record.status = "staging"
                                THEN /\ Assert(record.epoch <= txn_epoch /\ record.ts < txn_ts, 
                                               "Failure of assertion at line 296, column 15.")
                                     /\ record' = [status |-> "staging", epoch |-> txn_epoch, ts |-> txn_ts]
                                     /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecordLoop"]
                                ELSE /\ IF record.status = "aborted"
                                           THEN /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                                           ELSE /\ IF record.status = "committed"
                                                      THEN /\ Assert(FALSE, 
                                                                     "Failure of assertion at line 303, column 15.")
                                                      ELSE /\ TRUE
                                                /\ pc' = [pc EXCEPT !["committer"] = "StageWritesAndRecordLoop"]
                                     /\ UNCHANGED record
               /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                               pipelined_keys, parallel_keys, attempt, 
                               txn_epoch, txn_ts, to_write, to_check, 
                               prevent_epoch, prevent_ts, found_writes, 
                               to_resolve >>

AckClient == /\ pc["committer"] = "AckClient"
             /\ Assert(Committed, 
                       "Failure of assertion at line 311, column 5.")
             /\ commit_ack' = TRUE
             /\ pc' = [pc EXCEPT !["committer"] = "AsyncExplicitlyCommitted"]
             /\ UNCHANGED << record, intent_writes, tscache, pipelined_keys, 
                             parallel_keys, attempt, txn_epoch, txn_ts, 
                             to_write, to_check, have_staging_record, 
                             prevent_epoch, prevent_ts, found_writes, 
                             to_resolve >>

AsyncExplicitlyCommitted == /\ pc["committer"] = "AsyncExplicitlyCommitted"
                            /\ IF record.status = "staging"
                                  THEN /\ Assert(ImplicitlyCommitted, 
                                                 "Failure of assertion at line 318, column 7.")
                                       /\ record' = [record EXCEPT !.status = "committed"]
                                  ELSE /\ IF record.status = "committed"
                                             THEN /\ TRUE
                                             ELSE /\ Assert(FALSE, 
                                                            "Failure of assertion at line 326, column 7.")
                                       /\ UNCHANGED record
                            /\ to_write' = KEYS
                            /\ pc' = [pc EXCEPT !["committer"] = "AsyncResolveIntents"]
                            /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                            pipelined_keys, parallel_keys, 
                                            attempt, txn_epoch, txn_ts, 
                                            to_check, have_staging_record, 
                                            prevent_epoch, prevent_ts, 
                                            found_writes, to_resolve >>

AsyncResolveIntents == /\ pc["committer"] = "AsyncResolveIntents"
                       /\ IF to_write /= {}
                             THEN /\ \E key \in to_write:
                                       /\ IF ~intent_writes[key].resolved
                                             THEN /\ intent_writes' = [intent_writes EXCEPT ![key].resolved = TRUE]
                                             ELSE /\ TRUE
                                                  /\ UNCHANGED intent_writes
                                       /\ to_write' = to_write \ {key}
                                  /\ pc' = [pc EXCEPT !["committer"] = "AsyncResolveIntents"]
                             ELSE /\ pc' = [pc EXCEPT !["committer"] = "EndCommitter"]
                                  /\ UNCHANGED << intent_writes, to_write >>
                       /\ UNCHANGED << record, tscache, commit_ack, 
                                       pipelined_keys, parallel_keys, attempt, 
                                       txn_epoch, txn_ts, to_check, 
                                       have_staging_record, prevent_epoch, 
                                       prevent_ts, found_writes, to_resolve >>

EndCommitter == /\ pc["committer"] = "EndCommitter"
                /\ TRUE
                /\ pc' = [pc EXCEPT !["committer"] = "Done"]
                /\ UNCHANGED << record, intent_writes, tscache, commit_ack, 
                                pipelined_keys, parallel_keys, attempt, 
                                txn_epoch, txn_ts, to_write, to_check, 
                                have_staging_record, prevent_epoch, prevent_ts, 
                                found_writes, to_resolve >>

committer == BeginTxnEpoch \/ PipelineWrites \/ StageWritesAndRecord
                \/ StageWritesAndRecordLoop \/ QueryPipelinedWrite
                \/ ParallelWrite \/ StageRecord \/ AckClient
                \/ AsyncExplicitlyCommitted \/ AsyncResolveIntents
                \/ EndCommitter

PreventLoop(self) == /\ pc[self] = "PreventLoop"
                     /\ found_writes' = [found_writes EXCEPT ![self] = {}]
                     /\ pc' = [pc EXCEPT ![self] = "PushRecord"]
                     /\ UNCHANGED << record, intent_writes, tscache, 
                                     commit_ack, pipelined_keys, parallel_keys, 
                                     attempt, txn_epoch, txn_ts, to_write, 
                                     to_check, have_staging_record, 
                                     prevent_epoch, prevent_ts, to_resolve >>

PushRecord(self) == /\ pc[self] = "PushRecord"
                    /\ IF record.status = "pending"
                          THEN /\ record' = [record EXCEPT !.status = "aborted"]
                               /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                               /\ UNCHANGED << prevent_epoch, prevent_ts >>
                          ELSE /\ IF record.status = "staging"
                                     THEN /\ prevent_epoch' = [prevent_epoch EXCEPT ![self] = record.epoch]
                                          /\ prevent_ts' = [prevent_ts EXCEPT ![self] = record.ts]
                                          /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                     ELSE /\ IF record.status \in {"committed", "aborted"}
                                                THEN /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                                                ELSE /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                          /\ UNCHANGED << prevent_epoch, 
                                                          prevent_ts >>
                               /\ UNCHANGED record
                    /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                    pipelined_keys, parallel_keys, attempt, 
                                    txn_epoch, txn_ts, to_write, to_check, 
                                    have_staging_record, found_writes, 
                                    to_resolve >>

PreventWrites(self) == /\ pc[self] = "PreventWrites"
                       /\ IF found_writes[self] /= KEYS
                             THEN /\ \E key \in KEYS \ found_writes[self]:
                                       IF QueryIntent(key, prevent_epoch[self], prevent_ts[self])
                                          THEN /\ found_writes' = [found_writes EXCEPT ![self] = found_writes[self] \union {key}]
                                               /\ pc' = [pc EXCEPT ![self] = "PreventWrites"]
                                               /\ UNCHANGED tscache
                                          ELSE /\ IF tscache[key] < prevent_ts[self]
                                                     THEN /\ tscache' = [tscache EXCEPT ![key] = prevent_ts[self]]
                                                     ELSE /\ TRUE
                                                          /\ UNCHANGED tscache
                                               /\ pc' = [pc EXCEPT ![self] = "RecoverRecord"]
                                               /\ UNCHANGED found_writes
                             ELSE /\ pc' = [pc EXCEPT ![self] = "RecoverRecord"]
                                  /\ UNCHANGED << tscache, found_writes >>
                       /\ UNCHANGED << record, intent_writes, commit_ack, 
                                       pipelined_keys, parallel_keys, attempt, 
                                       txn_epoch, txn_ts, to_write, to_check, 
                                       have_staging_record, prevent_epoch, 
                                       prevent_ts, to_resolve >>

RecoverRecord(self) == /\ pc[self] = "RecoverRecord"
                       /\ LET prevented == found_writes[self] /= KEYS IN
                            IF prevented
                               THEN /\ LET legal_change ==    record.epoch >= prevent_epoch[self]
                                                           /\ record.ts    >  prevent_ts[self] IN
                                         IF record.status = "aborted"
                                            THEN /\ TRUE
                                                 /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                                                 /\ UNCHANGED record
                                            ELSE /\ IF record.status = "committed"
                                                       THEN /\ TRUE
                                                            /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                                                            /\ UNCHANGED record
                                                       ELSE /\ IF record.status = "pending"
                                                                  THEN /\ Assert(FALSE, 
                                                                                 "Failure of assertion at line 406, column 15.")
                                                                       /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                                                                       /\ UNCHANGED record
                                                                  ELSE /\ IF record.status = "staging"
                                                                             THEN /\ IF legal_change
                                                                                        THEN /\ pc' = [pc EXCEPT ![self] = "PreventLoop"]
                                                                                             /\ UNCHANGED record
                                                                                        ELSE /\ record' = [record EXCEPT !.status = "aborted"]
                                                                                             /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                                                                             ELSE /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                                                                                  /\ UNCHANGED record
                               ELSE /\ IF record.status \in {"pending", "aborted"}
                                          THEN /\ Assert(FALSE, 
                                                         "Failure of assertion at line 421, column 13.")
                                               /\ UNCHANGED record
                                          ELSE /\ IF record.status \in {"staging", "committed"}
                                                     THEN /\ Assert(record.epoch = prevent_epoch[self], 
                                                                    "Failure of assertion at line 424, column 13.")
                                                          /\ Assert(record.ts    = prevent_ts[self], 
                                                                    "Failure of assertion at line 425, column 13.")
                                                          /\ IF record.status = "staging"
                                                                THEN /\ Assert(ImplicitlyCommitted, 
                                                                               "Failure of assertion at line 429, column 15.")
                                                                     /\ record' = [record EXCEPT !.status = "committed"]
                                                                ELSE /\ TRUE
                                                                     /\ UNCHANGED record
                                                     ELSE /\ TRUE
                                                          /\ UNCHANGED record
                                    /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                       /\ UNCHANGED << intent_writes, tscache, commit_ack, 
                                       pipelined_keys, parallel_keys, attempt, 
                                       txn_epoch, txn_ts, to_write, to_check, 
                                       have_staging_record, prevent_epoch, 
                                       prevent_ts, found_writes, to_resolve >>

ResolveIntents(self) == /\ pc[self] = "ResolveIntents"
                        /\ IF to_resolve[self] /= {}
                              THEN /\ \E key \in to_resolve[self]:
                                        /\ IF ~intent_writes[key].resolved
                                              THEN /\ intent_writes' = [intent_writes EXCEPT ![key].resolved = TRUE]
                                              ELSE /\ TRUE
                                                   /\ UNCHANGED intent_writes
                                        /\ to_resolve' = [to_resolve EXCEPT ![self] = to_resolve[self] \ {key}]
                                   /\ pc' = [pc EXCEPT ![self] = "ResolveIntents"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                   /\ UNCHANGED << intent_writes, to_resolve >>
                        /\ UNCHANGED << record, tscache, commit_ack, 
                                        pipelined_keys, parallel_keys, attempt, 
                                        txn_epoch, txn_ts, to_write, to_check, 
                                        have_staging_record, prevent_epoch, 
                                        prevent_ts, found_writes >>

preventer(self) == PreventLoop(self) \/ PushRecord(self)
                      \/ PreventWrites(self) \/ RecoverRecord(self)
                      \/ ResolveIntents(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == committer
           \/ (\E self \in PREVENTERS: preventer(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in PREVENTERS : WF_vars(preventer(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION - the hash of the generated TLA code (remove to silence divergence warnings): TLA-4b99a68dacd4c127554eb3f72922c6c6



=============================================================================
\* Modification History
\* Last modified Sat Sep 12 18:07:57 JST 2020 by ytaka23
\* Last modified Mon Sep 23 17:38:55 EDT 2019 by nathan
\* Created Mon May 13 10:03:40 EDT 2019 by nathan
