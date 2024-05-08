-------------------------- MODULE ReplicaLifetimes --------------------------
EXTENDS TLC, Integers, FiniteSets, Sequences
CONSTANTS REPLICATION_FACTOR, FOLLOWERS, MAX_RANGE_ID, MAX_STEPS
ASSUME REPLICATION_FACTOR > 0
ASSUME Cardinality(FOLLOWERS) > REPLICATION_FACTOR
ASSUME MAX_RANGE_ID > 0
ASSUME MAX_STEPS > 0

SYMMETRY         == Permutations(FOLLOWERS)
RANGE_IDS        == 1..MAX_RANGE_ID
ALL_KEYS         == 1..2^(MAX_RANGE_ID-1)
EMPTY_RANGE_DESC == [keys |-> {}, followers |-> {}, next_replica_id |-> 0]
EMPTY_REPLICA    == [present |-> FALSE, initialized |-> FALSE, replica_id |-> 0, keys |-> {}]
LOG_ENTRY_TYPES  == {"write", "rebalance", "split", "merge"}

(*************************************************************************)
(* Helpful TLA+ definitions.                                             *)
(*************************************************************************)
Range(f) == {f[x] : x \in DOMAIN f}
Pick(S) == CHOOSE s \in S : TRUE
RECURSIVE SetReduce(_, _, _)
SetReduce(Op(_, _), S, value) ==
  IF S = {} THEN value
  ELSE LET s == Pick(S)
       IN IF Op(s, value) = Op(value, s)
       THEN SetReduce(Op, S \ {s}, Op(s, value))
       ELSE Assert(FALSE, "not commutative")
Sum(S) == LET _op(a, b) == a + b
          IN SetReduce(_op, S, 0)
EnumerateOne(S) == {<<Pick(S), 1>>}
EnumerateTwo(S) == LET first == Pick(S)
                   IN {<<first, 1>>, <<CHOOSE x \in S: x /= first, 2>>}
EnumerateN(S, n) == CASE n = 1 -> EnumerateOne(S)
                      [] n = 2 -> EnumerateTwo(S)
                      [] OTHER -> Assert(FALSE, "undefined")

(*************************************************************************)
(* Replica Lifetimes models replication group configuration changes and  *)
(* the interleaving of lifecycle events during these changes on each of  *)
(* the group's individual replicas. A replication group (i.e. Range) can *)
(* change its configuration in three ways. It can:                       *)
(* 1. rebalance: move one of its replicas to a different machine         *)
(* 2. split:     separate into two independent replication groups        *)
(* 3. merge:     combine with a second neighboring replication group     *)
(*                                                                       *)
(* The specification tests an initial Range and the various paths it can *)
(* take through these replication changes. It does so through a single   *)
(* "leader" process which drives the replication changes and a set of    *)
(* "follower" processes which are capable of holding a replica for each  *)
(* of the Ranges that the initial Range may eventually evolve into.      *)
(* These followers interface with both their local state and with the    *)
(* authoritative state of the leader to navigate replication changes for *)
(* Ranges which they are a member of.                                    *)
(*************************************************************************)

(*--algorithm replicalifetimes
variables
  \* Authoritative global state.
  active_ranges       = {};
  destroyed_ranges    = {};
  next_range_id       = 1;
  range_descs         = [id \in RANGE_IDS |-> EMPTY_RANGE_DESC];
  range_to_right      = [id \in RANGE_IDS |-> 0];
  raft_logs           = [id \in RANGE_IDS |-> <<>>];
  upgrade_to_learners = FALSE;
  leader_done         = FALSE;
  \* Local follower state. Global to permit invariant checking.
  replicas            = [follower \in FOLLOWERS |-> [id \in RANGE_IDS |-> EMPTY_REPLICA]];
  raft_tombstones     = [follower \in FOLLOWERS |-> [id \in RANGE_IDS |-> 0]];
  applied_log_indexes = [follower \in FOLLOWERS |-> [id \in RANGE_IDS |-> 0]];
  applied_writes      = [follower \in FOLLOWERS |-> [id \in RANGE_IDS |-> {}]];

define
  \* -------------------
  \* Helper definitions.
  \* -------------------
  \* FollowersFromDesc returns a set of followers from a Range descriptor.
  FollowersFromDesc(range_desc) == {f[1]: f \in range_desc.followers}
  \* FollowersFromDesc returns the specified follower's replica ID for the
  \* given Range. The follower must be part of the Range.
  ReplicaIDForFollower(range_desc, follower) ==
    LET repl_desc == CHOOSE repl_desc \in range_desc.followers: repl_desc[1] = follower
    IN  repl_desc[2]
  \* SplitKey returns a two-element tuple with the keys on the LHS and the
  \* keys on the RHS if a split of the provided keys was to occur.
  SplitKey(keys) ==
    IF Cardinality(keys) < 2
    THEN Assert(FALSE, "not enough keys to split")
    ELSE LET split_point == Sum(keys) \div Cardinality(keys)
        left_keys   == {k \in keys: k <= split_point}
    IN <<left_keys, keys \ left_keys>>
  \* MergeableRanges returns the set of ranges that are mergeable into their
  \* right-hand neighbor.
  MergeableRanges == {
    range_id \in RANGE_IDS:
      /\ range_to_right[range_id] /= 0
      /\ FollowersFromDesc(range_descs[range_id]) = FollowersFromDesc(range_descs[range_to_right[range_id]])
  }
  \* AllReplicasCaughtUp returns whether all replicas are caught-up for the
  \* provided Range. A replica is "caught-up" if it is present on its follower
  \* and it has applied the entire Raft log.
  AllReplicasCaughtUp(range_id) ==
    \A follower \in FollowersFromDesc(range_descs[range_id]):
      /\ replicas[follower][range_id].present
      /\ replicas[follower][range_id].initialized
      /\ applied_log_indexes[follower][range_id] = Len(raft_logs[range_id])
  \* InitializedRangesWithUnapplied returns all Ranges that are initialized on
  \* the specified follower and which have Raft log entries to apply.
  InitializedRangesWithUnapplied(follower) == {
    range_id \in RANGE_IDS:
      /\ replicas[follower][range_id].initialized
      /\ applied_log_indexes[follower][range_id] < Len(raft_logs[range_id])
  }
  \* NonInitializedRanges returns all Ranges that are meant to have an
  \* initialized replica on the specified follower but that don't.
  NonInitializedRanges(follower) == {
    range_id \in RANGE_IDS:
      /\ ~replicas[follower][range_id].initialized
      /\ follower \in FollowersFromDesc(range_descs[range_id])
  }

  \* ----------------
  \* Type Invariants.
  \* ----------------
  RangeTypeInvariants ==
    \* All Range IDs are valid.
    /\ active_ranges    \subseteq RANGE_IDS
    /\ destroyed_ranges \subseteq RANGE_IDS
    /\ DOMAIN range_descs      =  RANGE_IDS
    /\ DOMAIN range_to_right   =  RANGE_IDS
    /\ DOMAIN raft_logs        =  RANGE_IDS
    \* The keys in each Range are distinct.
    /\ ~(\E id1, id2 \in active_ranges:
           /\ id1 /= id2
           /\ range_descs[id1].keys \intersect range_descs[id2].keys /= {})
    \* The next_range_id remains valid.
    /\ next_range_id <= MAX_RANGE_ID + 1
    /\ Cardinality(active_ranges) + Cardinality(destroyed_ranges) = next_range_id - 1
    \* The active and destroyed range sets are distinct.
    /\ active_ranges \intersect destroyed_ranges = {}
    \* Invariants that hold after the initial state:
    /\ (active_ranges = {} \/ (
      \* The keys in all Ranges combines to the full keyspace.
      /\ ALL_KEYS = LET _op(range_keys, key_agg) == key_agg \union range_keys
                        _range_keys              == {range_descs[id].keys : id \in active_ranges}
                    IN SetReduce(_op, _range_keys, {})
      \* There is exactly one Range with no RHS neighbo.
      /\   \E id \in active_ranges: range_to_right[id] = 0
      /\ ~(\E id1, id2 \in active_ranges:
                   /\ id1 /= id2
                   /\ range_to_right[id1] = 0 /\ range_to_right[id2] = 0)
      ))

  FollowerTypeInvariants ==
    \* All Followers are valid.
    /\ DOMAIN replicas            = FOLLOWERS
    /\ DOMAIN raft_tombstones     = FOLLOWERS
    /\ DOMAIN applied_log_indexes = FOLLOWERS
    /\ DOMAIN applied_writes      = FOLLOWERS

  ReplicaTypeInvariants ==
    /\ \A f \in FOLLOWERS:
      \* All replicas reference valid Ranges.
      /\ DOMAIN replicas[f]            = RANGE_IDS
      /\ DOMAIN raft_tombstones[f]     = RANGE_IDS
      /\ DOMAIN applied_log_indexes[f] = RANGE_IDS
      /\ DOMAIN applied_writes[f]      = RANGE_IDS
      /\ \A id \in RANGE_IDS:
        \* All present replicas ...
        IF replicas[f][id].present
        THEN \* ... are or were active.
             /\ id \in (active_ranges \union destroyed_ranges)
             \* ... are or were part of the Range.
             /\ (\/ f \in FollowersFromDesc(range_descs[id])
                 \/ id \in destroyed_ranges
                 \/ applied_log_indexes[f][id] < Len(raft_logs[id]))
        \* All non-present replicas ...
        ELSE \* ... are empty.
             /\ replicas[f][id] = EMPTY_REPLICA
      /\ \A id \in RANGE_IDS:
        \* All initialized replicas ...
        IF replicas[f][id].initialized
        THEN \* ... have keys.
             /\ replicas[f][id].keys /= {}
        \* All uninitialized replicas ...
        ELSE \* ... have no keys.
             /\ replicas[f][id].keys = {}

  TypeInvariants == /\ RangeTypeInvariants
                    /\ FollowerTypeInvariants
                    /\ ReplicaTypeInvariants

  \* ---------------------------
  \* Domain-Specific Invariants.
  \* ---------------------------
  RaftLogInvariants ==
    \* All Raft log entries are valid.
    /\ \A range_id \in RANGE_IDS:
      \A entry \in Range(raft_logs[range_id]):
        /\ entry.type \in LOG_ENTRY_TYPES
        /\ CASE entry.type = "write"     -> TRUE
             [] entry.type = "rebalance" -> {"added", "removed"}                     \subseteq DOMAIN entry
             [] entry.type = "split"     -> {"new_range_id", "lhs_keys", "rhs_keys"} \subseteq DOMAIN entry
             [] entry.type = "merge"     -> "merged_range_id"                              \in DOMAIN entry
             [] OTHER                    -> FALSE
    \* All followers have applied a prefix of each global Raft log.
    /\ \A f \in FOLLOWERS:
      \A range_id \in RANGE_IDS:
        applied_log_indexes[f][range_id] <= Len(raft_logs[range_id])

  StoreInvariants ==
    \* No two replicas on a Store overlap.
    /\ \A f \in FOLLOWERS:
      ~(\E id1, id2 \in RANGE_IDS:
          /\ id1 /= id2
          /\ replicas[f][id1].initialized
          /\ replicas[f][id2].initialized
          /\ replicas[f][id1].keys \intersect replicas[f][id2].keys /= {})

  \* --------------------
  \* Temporal Properties.
  \* --------------------
  TemporalReplicaProperties ==
    \* All replicas in the final config eventually apply all log entries.
    /\ <>[](\A id \in active_ranges:
              \A f \in FollowersFromDesc(range_descs[id]):
                applied_log_indexes[f][id] = Len(raft_logs[id]))
    \* All destroyed replicas are eventually removed.
    \* NB: this property only holds for initialized replicas. Uninitialized
    \*     replicas may never learn that they have been rebalanced away
    \*     because the spec doesn't implement replica GC.
    /\ <>[](\A id \in destroyed_ranges:
              ~(\E f \in FOLLOWERS: replicas[f][id].initialized))
end define;

fair process leader = "leader"
variables
  step = 0;
  to_merge_lhs = 0;
  to_merge_rhs = 0;
begin
  \* Begin by creating the first Range. Range 1 is given all keys and is
  \* assigned to the initial set of followers.
  CreateFirstRange:
    with init_followers = EnumerateN(FOLLOWERS, REPLICATION_FACTOR) do
      range_descs[next_range_id] := [
        keys            |-> ALL_KEYS,
        followers       |-> init_followers,
        next_replica_id |-> Cardinality(init_followers) + 1
      ];
    end with;
    active_ranges := active_ranges \union {next_range_id};
    next_range_id := next_range_id + 1;

  ProcessLeader:
    while step < MAX_STEPS do
      step := step + 1;
      either
        Write:
          with range_id \in active_ranges do
            \* Append a "write" raft entry to the Range's Raft log.
            raft_logs[range_id] := Append(raft_logs[range_id], [type |-> "write"]);
          end with;
      or
        Rebalance:
          with range_id \in active_ranges do
            with old_followers   = FollowersFromDesc(range_descs[range_id]),
                 next_replica_id = range_descs[range_id].next_replica_id do
              with remove \in {follower \in FOLLOWERS: follower \in    old_followers},
                   add    \in {follower \in FOLLOWERS: follower \notin old_followers} do
                with updated_desc = [
                  keys            |-> range_descs[range_id].keys,
                  followers       |-> {f \in range_descs[range_id].followers: f[1] /= remove}
                                      \union {<<add, next_replica_id>>},
                  next_replica_id |-> next_replica_id + 1
                ] do
                  \* Append a "rebalance" raft entry to the Range's Raft log.
                  raft_logs[range_id] := Append(raft_logs[range_id], [
                    type    |-> "rebalance",
                    added   |-> add,
                    removed |-> remove
                  ]);

                  \* Adjust the range descriptors and authoritative range state.
                  range_descs[range_id] := updated_desc;
                end with;
              end with;
            end with;
          end with;
      or
        await next_range_id <= MAX_RANGE_ID;
        Split:
          with range_id \in active_ranges do
            with split_keys = SplitKey(range_descs[range_id].keys) do
              \* Append a "split" raft entry to the LHS's Raft log.
              raft_logs[range_id] := Append(raft_logs[range_id], [
                type         |-> "split",
                new_range_id |-> next_range_id,
                lhs_keys     |-> split_keys[1],
                rhs_keys     |-> split_keys[2]
              ]);

              \* Adjust the range descriptors and authoritative range state.
              range_descs := [id \in RANGE_IDS |->
                CASE id = range_id      -> [keys |-> split_keys[1]] @@ range_descs[range_id]
                  [] id = next_range_id -> [keys |-> split_keys[2]] @@ range_descs[range_id]
                  [] OTHER              -> range_descs[id]
              ];
              range_to_right := [id \in {range_id, next_range_id} |->
                IF id = range_id THEN next_range_id ELSE range_to_right[range_id]
              ] @@ range_to_right;
              active_ranges := active_ranges \union {next_range_id};
              next_range_id := next_range_id + 1;
            end with;
          end with;
      or
        await MergeableRanges /= {};
        Merge:
          with range_id \in MergeableRanges do
            to_merge_lhs := range_id;
            to_merge_rhs := range_to_right[range_id];
            \* NB: break to work around no labels in `with` blocks.
          end with;

          ProcessMerge:
            \* Wait for all replicas on both sides of the merge to be caught
            \* up before initializing it. This ensures that the RHS is present
            \* when each Replica of the LHS applies the merge Raft command.
            await AllReplicasCaughtUp(to_merge_lhs);
            await AllReplicasCaughtUp(to_merge_rhs);

            with merged_keys = range_descs[to_merge_lhs].keys \union 
                               range_descs[to_merge_rhs].keys do
              \* Append a "merge" raft entry to the LHS's Raft log.
              raft_logs[to_merge_lhs] := Append(raft_logs[to_merge_lhs], [
                type            |-> "merge",
                merged_range_id |-> to_merge_rhs
              ]);

              \* Adjust the range descriptors and authoritative range state.
              range_descs := [id \in RANGE_IDS |->
               CASE id = to_merge_lhs -> [keys |-> merged_keys] @@ range_descs[to_merge_lhs]
                 [] id = to_merge_rhs -> EMPTY_RANGE_DESC
                 [] OTHER             -> range_descs[id]
              ];
              range_to_right := [id \in RANGE_IDS |->
                IF id = to_merge_lhs THEN range_to_right[to_merge_rhs] ELSE range_to_right[id]
              ];
              active_ranges := active_ranges \ {to_merge_rhs};
              destroyed_ranges := destroyed_ranges \union {to_merge_rhs};
            end with;
      end either;
    end while;

  leader_done := TRUE;
end process;

fair process follower \in FOLLOWERS
variable
  \* None so far.
begin
  ProcessFollower:
    while \/ ~leader_done
          \/ InitializedRangesWithUnapplied(self) /= {}
          \/ NonInitializedRanges(self)           /= {} do
      either
        \* Process a resident replica that has Raft entries to apply.
        await InitializedRangesWithUnapplied(self) /= {};
        with range_id \in InitializedRangesWithUnapplied(self) do
          with index_to_apply = applied_log_indexes[self][range_id] + 1,
               entry          = raft_logs[range_id][index_to_apply] do
            if entry.type = "write" then
              \* Apply the write.
              applied_writes[self][range_id] := applied_writes[self][range_id] \union {index_to_apply};
            elsif entry.type = "rebalance" then
              \* Apply the rebalance. This may be removing our replica.
              if entry.removed = self then
                raft_tombstones[self][range_id] := replicas[self][range_id].replica_id;
                replicas[self][range_id]        := EMPTY_REPLICA;
              end if;
            elsif entry.type = "split" then
              \* Apply the split.
              assert raft_tombstones[self][entry.new_range_id] = 0;
              \* We can't make the following assertion, because the Range may be present.
              \*  assert replicas[self][entry.merged_range_id].present;
              \* However, the range cannot be initialized.
              assert ~replicas[self][entry.new_range_id].initialized;

              replicas[self] := [id \in RANGE_IDS |->
                CASE id = range_id           -> [keys |-> entry.lhs_keys] @@ replicas[self][range_id]
                  [] id = entry.new_range_id -> [keys |-> entry.rhs_keys] @@ replicas[self][range_id]
                  [] OTHER                   -> replicas[self][id]
              ];
            elsif entry.type = "merge" then
              \* Apply the merge.
              \* The RHS range must be present, initialized, and caught-up on its log.
              assert replicas[self][entry.merged_range_id].present;
              assert replicas[self][entry.merged_range_id].initialized;
              assert applied_log_indexes[self][entry.merged_range_id] = Len(raft_logs[entry.merged_range_id]);

              with lhs_keys = replicas[self][range_id].keys,
                   rhs_keys = replicas[self][entry.merged_range_id].keys do
                raft_tombstones[self][entry.merged_range_id] := MAX_RANGE_ID + 1;
                replicas[self] := [id \in RANGE_IDS |->
                  CASE id = range_id              -> [keys |-> lhs_keys \union rhs_keys] @@ replicas[self][range_id]
                    [] id = entry.merged_range_id -> EMPTY_REPLICA
                    [] OTHER                      -> replicas[self][id]
                ];
              end with;
            else
              assert FALSE;
            end if;
            applied_log_indexes[self][range_id] := index_to_apply;
          end with;
        end with;
      or
        \* Process a non-resident replica.
        await NonInitializedRanges(self) /= {};
        either
          \* Attempt to receive a snapshot to initiailize the range.
          \* TODO: model both preemptive and learner snapshots.
          with range_id \in NonInitializedRanges(self) do
            if \E existing_replica \in DOMAIN replicas[self]:
              /\ replicas[self][existing_replica].present
              /\ replicas[self][existing_replica].initialized
              /\ replicas[self][existing_replica].keys \intersect range_descs[range_id].keys /= {} then
              \* Do nothing. The snapshot overlaps an existing range.
            else
              \* Initialize the replica.
              with replica_id = ReplicaIDForFollower(range_descs[range_id], self) do
                assert raft_tombstones[self][range_id] < replica_id;
                replicas[self][range_id] := [
                  present     |-> TRUE,
                  initialized |-> TRUE,
                  replica_id  |-> replica_id,
                  keys        |-> range_descs[range_id].keys
                ];
                applied_log_indexes[self][range_id] := Len(raft_logs[range_id]);
              end with;
            end if;
          end with;
        or
          \* Vote without being initialized.
          \* TODO: model HardState and assert that it never gets lost.
          with range_id \in NonInitializedRanges(self) do
            with replica_id = ReplicaIDForFollower(range_descs[range_id], self) do
              assert raft_tombstones[self][range_id] < replica_id;
              replicas[self][range_id] := [
                present     |-> TRUE,
                initialized |-> FALSE,
                replica_id  |-> replica_id,
                keys        |-> {}
              ];
            end with;
          end with;
        end either;
      end either;
    end while;
end process;

fair process upgrade = "upgrade"
begin
  DoVersionUpgrade:
    upgrade_to_learners := TRUE;
end process;
end algorithm;*)
\* BEGIN TRANSLATION
VARIABLES active_ranges, destroyed_ranges, next_range_id, range_descs, 
          range_to_right, raft_logs, upgrade_to_learners, leader_done, 
          replicas, raft_tombstones, applied_log_indexes, applied_writes, pc

(* define statement *)
FollowersFromDesc(range_desc) == {f[1]: f \in range_desc.followers}


ReplicaIDForFollower(range_desc, follower) ==
  LET repl_desc == CHOOSE repl_desc \in range_desc.followers: repl_desc[1] = follower
  IN  repl_desc[2]


SplitKey(keys) ==
  IF Cardinality(keys) < 2
  THEN Assert(FALSE, "not enough keys to split")
  ELSE LET split_point == Sum(keys) \div Cardinality(keys)
      left_keys   == {k \in keys: k <= split_point}
  IN <<left_keys, keys \ left_keys>>


MergeableRanges == {
  range_id \in RANGE_IDS:
    /\ range_to_right[range_id] /= 0
    /\ FollowersFromDesc(range_descs[range_id]) = FollowersFromDesc(range_descs[range_to_right[range_id]])
}



AllReplicasCaughtUp(range_id) ==
  \A follower \in FollowersFromDesc(range_descs[range_id]):
    /\ replicas[follower][range_id].present
    /\ replicas[follower][range_id].initialized
    /\ applied_log_indexes[follower][range_id] = Len(raft_logs[range_id])


InitializedRangesWithUnapplied(follower) == {
  range_id \in RANGE_IDS:
    /\ replicas[follower][range_id].initialized
    /\ applied_log_indexes[follower][range_id] < Len(raft_logs[range_id])
}


NonInitializedRanges(follower) == {
  range_id \in RANGE_IDS:
    /\ ~replicas[follower][range_id].initialized
    /\ follower \in FollowersFromDesc(range_descs[range_id])
}




RangeTypeInvariants ==

  /\ active_ranges    \subseteq RANGE_IDS
  /\ destroyed_ranges \subseteq RANGE_IDS
  /\ DOMAIN range_descs      =  RANGE_IDS
  /\ DOMAIN range_to_right   =  RANGE_IDS
  /\ DOMAIN raft_logs        =  RANGE_IDS

  /\ ~(\E id1, id2 \in active_ranges:
         /\ id1 /= id2
         /\ range_descs[id1].keys \intersect range_descs[id2].keys /= {})

  /\ next_range_id <= MAX_RANGE_ID + 1
  /\ Cardinality(active_ranges) + Cardinality(destroyed_ranges) = next_range_id - 1

  /\ active_ranges \intersect destroyed_ranges = {}

  /\ (active_ranges = {} \/ (

    /\ ALL_KEYS = LET _op(range_keys, key_agg) == key_agg \union range_keys
                      _range_keys              == {range_descs[id].keys : id \in active_ranges}
                  IN SetReduce(_op, _range_keys, {})

    /\   \E id \in active_ranges: range_to_right[id] = 0
    /\ ~(\E id1, id2 \in active_ranges:
                 /\ id1 /= id2
                 /\ range_to_right[id1] = 0 /\ range_to_right[id2] = 0)
    ))

FollowerTypeInvariants ==

  /\ DOMAIN replicas            = FOLLOWERS
  /\ DOMAIN raft_tombstones     = FOLLOWERS
  /\ DOMAIN applied_log_indexes = FOLLOWERS
  /\ DOMAIN applied_writes      = FOLLOWERS

ReplicaTypeInvariants ==
  /\ \A f \in FOLLOWERS:

    /\ DOMAIN replicas[f]            = RANGE_IDS
    /\ DOMAIN raft_tombstones[f]     = RANGE_IDS
    /\ DOMAIN applied_log_indexes[f] = RANGE_IDS
    /\ DOMAIN applied_writes[f]      = RANGE_IDS
    /\ \A id \in RANGE_IDS:

      IF replicas[f][id].present
      THEN
           /\ id \in (active_ranges \union destroyed_ranges)

           /\ (\/ f \in FollowersFromDesc(range_descs[id])
               \/ id \in destroyed_ranges
               \/ applied_log_indexes[f][id] < Len(raft_logs[id]))

      ELSE
           /\ replicas[f][id] = EMPTY_REPLICA
    /\ \A id \in RANGE_IDS:

      IF replicas[f][id].initialized
      THEN
           /\ replicas[f][id].keys /= {}

      ELSE
           /\ replicas[f][id].keys = {}

TypeInvariants == /\ RangeTypeInvariants
                  /\ FollowerTypeInvariants
                  /\ ReplicaTypeInvariants




RaftLogInvariants ==

  /\ \A range_id \in RANGE_IDS:
    \A entry \in Range(raft_logs[range_id]):
      /\ entry.type \in LOG_ENTRY_TYPES
      /\ CASE entry.type = "write"     -> TRUE
           [] entry.type = "rebalance" -> {"added", "removed"}                     \subseteq DOMAIN entry
           [] entry.type = "split"     -> {"new_range_id", "lhs_keys", "rhs_keys"} \subseteq DOMAIN entry
           [] entry.type = "merge"     -> "merged_range_id"                              \in DOMAIN entry
           [] OTHER                    -> FALSE

  /\ \A f \in FOLLOWERS:
    \A range_id \in RANGE_IDS:
      applied_log_indexes[f][range_id] <= Len(raft_logs[range_id])

StoreInvariants ==

  /\ \A f \in FOLLOWERS:
    ~(\E id1, id2 \in RANGE_IDS:
        /\ id1 /= id2
        /\ replicas[f][id1].initialized
        /\ replicas[f][id2].initialized
        /\ replicas[f][id1].keys \intersect replicas[f][id2].keys /= {})




TemporalReplicaProperties ==

  /\ <>[](\A id \in active_ranges:
            \A f \in FollowersFromDesc(range_descs[id]):
              applied_log_indexes[f][id] = Len(raft_logs[id]))




  /\ <>[](\A id \in destroyed_ranges:
            ~(\E f \in FOLLOWERS: replicas[f][id].initialized))

VARIABLES step, to_merge_lhs, to_merge_rhs

vars == << active_ranges, destroyed_ranges, next_range_id, range_descs, 
           range_to_right, raft_logs, upgrade_to_learners, leader_done, 
           replicas, raft_tombstones, applied_log_indexes, applied_writes, pc, 
           step, to_merge_lhs, to_merge_rhs >>

ProcSet == {"leader"} \cup (FOLLOWERS) \cup {"upgrade"}

Init == (* Global variables *)
        /\ active_ranges = {}
        /\ destroyed_ranges = {}
        /\ next_range_id = 1
        /\ range_descs = [id \in RANGE_IDS |-> EMPTY_RANGE_DESC]
        /\ range_to_right = [id \in RANGE_IDS |-> 0]
        /\ raft_logs = [id \in RANGE_IDS |-> <<>>]
        /\ upgrade_to_learners = FALSE
        /\ leader_done = FALSE
        /\ replicas = [follower \in FOLLOWERS |-> [id \in RANGE_IDS |-> EMPTY_REPLICA]]
        /\ raft_tombstones = [follower \in FOLLOWERS |-> [id \in RANGE_IDS |-> 0]]
        /\ applied_log_indexes = [follower \in FOLLOWERS |-> [id \in RANGE_IDS |-> 0]]
        /\ applied_writes = [follower \in FOLLOWERS |-> [id \in RANGE_IDS |-> {}]]
        (* Process leader *)
        /\ step = 0
        /\ to_merge_lhs = 0
        /\ to_merge_rhs = 0
        /\ pc = [self \in ProcSet |-> CASE self = "leader" -> "CreateFirstRange"
                                        [] self \in FOLLOWERS -> "ProcessFollower"
                                        [] self = "upgrade" -> "DoVersionUpgrade"]

CreateFirstRange == /\ pc["leader"] = "CreateFirstRange"
                    /\ LET init_followers == EnumerateN(FOLLOWERS, REPLICATION_FACTOR) IN
                         range_descs' = [range_descs EXCEPT ![next_range_id] =                               [
                                                                                 keys            |-> ALL_KEYS,
                                                                                 followers       |-> init_followers,
                                                                                 next_replica_id |-> Cardinality(init_followers) + 1
                                                                               ]]
                    /\ active_ranges' = (active_ranges \union {next_range_id})
                    /\ next_range_id' = next_range_id + 1
                    /\ pc' = [pc EXCEPT !["leader"] = "ProcessLeader"]
                    /\ UNCHANGED << destroyed_ranges, range_to_right, 
                                    raft_logs, upgrade_to_learners, 
                                    leader_done, replicas, raft_tombstones, 
                                    applied_log_indexes, applied_writes, step, 
                                    to_merge_lhs, to_merge_rhs >>

ProcessLeader == /\ pc["leader"] = "ProcessLeader"
                 /\ IF step < MAX_STEPS
                       THEN /\ step' = step + 1
                            /\ \/ /\ pc' = [pc EXCEPT !["leader"] = "Write"]
                               \/ /\ pc' = [pc EXCEPT !["leader"] = "Rebalance"]
                               \/ /\ next_range_id <= MAX_RANGE_ID
                                  /\ pc' = [pc EXCEPT !["leader"] = "Split"]
                               \/ /\ MergeableRanges /= {}
                                  /\ pc' = [pc EXCEPT !["leader"] = "Merge"]
                            /\ UNCHANGED leader_done
                       ELSE /\ leader_done' = TRUE
                            /\ pc' = [pc EXCEPT !["leader"] = "Done"]
                            /\ step' = step
                 /\ UNCHANGED << active_ranges, destroyed_ranges, 
                                 next_range_id, range_descs, range_to_right, 
                                 raft_logs, upgrade_to_learners, replicas, 
                                 raft_tombstones, applied_log_indexes, 
                                 applied_writes, to_merge_lhs, to_merge_rhs >>

Write == /\ pc["leader"] = "Write"
         /\ \E range_id \in active_ranges:
              raft_logs' = [raft_logs EXCEPT ![range_id] = Append(raft_logs[range_id], [type |-> "write"])]
         /\ pc' = [pc EXCEPT !["leader"] = "ProcessLeader"]
         /\ UNCHANGED << active_ranges, destroyed_ranges, next_range_id, 
                         range_descs, range_to_right, upgrade_to_learners, 
                         leader_done, replicas, raft_tombstones, 
                         applied_log_indexes, applied_writes, step, 
                         to_merge_lhs, to_merge_rhs >>

Rebalance == /\ pc["leader"] = "Rebalance"
             /\ \E range_id \in active_ranges:
                  LET old_followers == FollowersFromDesc(range_descs[range_id]) IN
                    LET next_replica_id == range_descs[range_id].next_replica_id IN
                      \E remove \in {follower \in FOLLOWERS: follower \in    old_followers}:
                        \E add \in {follower \in FOLLOWERS: follower \notin old_followers}:
                          LET updated_desc ==                     [
                                                keys            |-> range_descs[range_id].keys,
                                                followers       |-> {f \in range_descs[range_id].followers: f[1] /= remove}
                                                                    \union {<<add, next_replica_id>>},
                                                next_replica_id |-> next_replica_id + 1
                                              ] IN
                            /\ raft_logs' = [raft_logs EXCEPT ![range_id] =                        Append(raft_logs[range_id], [
                                                                              type    |-> "rebalance",
                                                                              added   |-> add,
                                                                              removed |-> remove
                                                                            ])]
                            /\ range_descs' = [range_descs EXCEPT ![range_id] = updated_desc]
             /\ pc' = [pc EXCEPT !["leader"] = "ProcessLeader"]
             /\ UNCHANGED << active_ranges, destroyed_ranges, next_range_id, 
                             range_to_right, upgrade_to_learners, leader_done, 
                             replicas, raft_tombstones, applied_log_indexes, 
                             applied_writes, step, to_merge_lhs, to_merge_rhs >>

Split == /\ pc["leader"] = "Split"
         /\ \E range_id \in active_ranges:
              LET split_keys == SplitKey(range_descs[range_id].keys) IN
                /\ raft_logs' = [raft_logs EXCEPT ![range_id] =                        Append(raft_logs[range_id], [
                                                                  type         |-> "split",
                                                                  new_range_id |-> next_range_id,
                                                                  lhs_keys     |-> split_keys[1],
                                                                  rhs_keys     |-> split_keys[2]
                                                                ])]
                /\ range_descs' =                [id \in RANGE_IDS |->
                                    CASE id = range_id      -> [keys |-> split_keys[1]] @@ range_descs[range_id]
                                      [] id = next_range_id -> [keys |-> split_keys[2]] @@ range_descs[range_id]
                                      [] OTHER              -> range_descs[id]
                                  ]
                /\ range_to_right' =                   [id \in {range_id, next_range_id} |->
                                       IF id = range_id THEN next_range_id ELSE range_to_right[range_id]
                                     ] @@ range_to_right
                /\ active_ranges' = (active_ranges \union {next_range_id})
                /\ next_range_id' = next_range_id + 1
         /\ pc' = [pc EXCEPT !["leader"] = "ProcessLeader"]
         /\ UNCHANGED << destroyed_ranges, upgrade_to_learners, leader_done, 
                         replicas, raft_tombstones, applied_log_indexes, 
                         applied_writes, step, to_merge_lhs, to_merge_rhs >>

Merge == /\ pc["leader"] = "Merge"
         /\ \E range_id \in MergeableRanges:
              /\ to_merge_lhs' = range_id
              /\ to_merge_rhs' = range_to_right[range_id]
         /\ pc' = [pc EXCEPT !["leader"] = "ProcessMerge"]
         /\ UNCHANGED << active_ranges, destroyed_ranges, next_range_id, 
                         range_descs, range_to_right, raft_logs, 
                         upgrade_to_learners, leader_done, replicas, 
                         raft_tombstones, applied_log_indexes, applied_writes, 
                         step >>

ProcessMerge == /\ pc["leader"] = "ProcessMerge"
                /\ AllReplicasCaughtUp(to_merge_lhs)
                /\ AllReplicasCaughtUp(to_merge_rhs)
                /\ LET merged_keys == range_descs[to_merge_lhs].keys \union
                                      range_descs[to_merge_rhs].keys IN
                     /\ raft_logs' = [raft_logs EXCEPT ![to_merge_lhs] =                            Append(raft_logs[to_merge_lhs], [
                                                                           type            |-> "merge",
                                                                           merged_range_id |-> to_merge_rhs
                                                                         ])]
                     /\ range_descs' =                [id \in RANGE_IDS |->
                                        CASE id = to_merge_lhs -> [keys |-> merged_keys] @@ range_descs[to_merge_lhs]
                                          [] id = to_merge_rhs -> EMPTY_RANGE_DESC
                                          [] OTHER             -> range_descs[id]
                                       ]
                     /\ range_to_right' =                   [id \in RANGE_IDS |->
                                            IF id = to_merge_lhs THEN range_to_right[to_merge_rhs] ELSE range_to_right[id]
                                          ]
                     /\ active_ranges' = active_ranges \ {to_merge_rhs}
                     /\ destroyed_ranges' = (destroyed_ranges \union {to_merge_rhs})
                /\ pc' = [pc EXCEPT !["leader"] = "ProcessLeader"]
                /\ UNCHANGED << next_range_id, upgrade_to_learners, 
                                leader_done, replicas, raft_tombstones, 
                                applied_log_indexes, applied_writes, step, 
                                to_merge_lhs, to_merge_rhs >>

leader == CreateFirstRange \/ ProcessLeader \/ Write \/ Rebalance \/ Split
             \/ Merge \/ ProcessMerge

ProcessFollower(self) == /\ pc[self] = "ProcessFollower"
                         /\ IF \/ ~leader_done
                               \/ InitializedRangesWithUnapplied(self) /= {}
                               \/ NonInitializedRanges(self)           /= {}
                               THEN /\ \/ /\ InitializedRangesWithUnapplied(self) /= {}
                                          /\ \E range_id \in InitializedRangesWithUnapplied(self):
                                               LET index_to_apply == applied_log_indexes[self][range_id] + 1 IN
                                                 LET entry == raft_logs[range_id][index_to_apply] IN
                                                   /\ IF entry.type = "write"
                                                         THEN /\ applied_writes' = [applied_writes EXCEPT ![self][range_id] = applied_writes[self][range_id] \union {index_to_apply}]
                                                              /\ UNCHANGED << replicas, 
                                                                              raft_tombstones >>
                                                         ELSE /\ IF entry.type = "rebalance"
                                                                    THEN /\ IF entry.removed = self
                                                                               THEN /\ raft_tombstones' = [raft_tombstones EXCEPT ![self][range_id] = replicas[self][range_id].replica_id]
                                                                                    /\ replicas' = [replicas EXCEPT ![self][range_id] = EMPTY_REPLICA]
                                                                               ELSE /\ TRUE
                                                                                    /\ UNCHANGED << replicas, 
                                                                                                    raft_tombstones >>
                                                                    ELSE /\ IF entry.type = "split"
                                                                               THEN /\ Assert(raft_tombstones[self][entry.new_range_id] = 0, 
                                                                                              "Failure of assertion at line 383, column 15.")
                                                                                    /\ Assert(~replicas[self][entry.new_range_id].initialized, 
                                                                                              "Failure of assertion at line 387, column 15.")
                                                                                    /\ replicas' = [replicas EXCEPT ![self] =                   [id \in RANGE_IDS |->
                                                                                                                                CASE id = range_id           -> [keys |-> entry.lhs_keys] @@ replicas[self][range_id]
                                                                                                                                  [] id = entry.new_range_id -> [keys |-> entry.rhs_keys] @@ replicas[self][range_id]
                                                                                                                                  [] OTHER                   -> replicas[self][id]
                                                                                                                              ]]
                                                                                    /\ UNCHANGED raft_tombstones
                                                                               ELSE /\ IF entry.type = "merge"
                                                                                          THEN /\ Assert(replicas[self][entry.merged_range_id].present, 
                                                                                                         "Failure of assertion at line 397, column 15.")
                                                                                               /\ Assert(replicas[self][entry.merged_range_id].initialized, 
                                                                                                         "Failure of assertion at line 398, column 15.")
                                                                                               /\ Assert(applied_log_indexes[self][entry.merged_range_id] = Len(raft_logs[entry.merged_range_id]), 
                                                                                                         "Failure of assertion at line 399, column 15.")
                                                                                               /\ LET lhs_keys == replicas[self][range_id].keys IN
                                                                                                    LET rhs_keys == replicas[self][entry.merged_range_id].keys IN
                                                                                                      /\ raft_tombstones' = [raft_tombstones EXCEPT ![self][entry.merged_range_id] = MAX_RANGE_ID + 1]
                                                                                                      /\ replicas' = [replicas EXCEPT ![self] =                   [id \in RANGE_IDS |->
                                                                                                                                                  CASE id = range_id              -> [keys |-> lhs_keys \union rhs_keys] @@ replicas[self][range_id]
                                                                                                                                                    [] id = entry.merged_range_id -> EMPTY_REPLICA
                                                                                                                                                    [] OTHER                      -> replicas[self][id]
                                                                                                                                                ]]
                                                                                          ELSE /\ Assert(FALSE, 
                                                                                                         "Failure of assertion at line 411, column 15.")
                                                                                               /\ UNCHANGED << replicas, 
                                                                                                               raft_tombstones >>
                                                              /\ UNCHANGED applied_writes
                                                   /\ applied_log_indexes' = [applied_log_indexes EXCEPT ![self][range_id] = index_to_apply]
                                       \/ /\ NonInitializedRanges(self) /= {}
                                          /\ \/ /\ \E range_id \in NonInitializedRanges(self):
                                                     IF  \E existing_replica \in DOMAIN replicas[self]:
                                                        /\ replicas[self][existing_replica].present
                                                        /\ replicas[self][existing_replica].initialized
                                                        /\ replicas[self][existing_replica].keys \intersect range_descs[range_id].keys /= {}
                                                        THEN /\ UNCHANGED << replicas, 
                                                                             applied_log_indexes >>
                                                        ELSE /\ LET replica_id == ReplicaIDForFollower(range_descs[range_id], self) IN
                                                                  /\ Assert(raft_tombstones[self][range_id] < replica_id, 
                                                                            "Failure of assertion at line 431, column 17.")
                                                                  /\ replicas' = [replicas EXCEPT ![self][range_id] =                             [
                                                                                                                        present     |-> TRUE,
                                                                                                                        initialized |-> TRUE,
                                                                                                                        replica_id  |-> replica_id,
                                                                                                                        keys        |-> range_descs[range_id].keys
                                                                                                                      ]]
                                                                  /\ applied_log_indexes' = [applied_log_indexes EXCEPT ![self][range_id] = Len(raft_logs[range_id])]
                                             \/ /\ \E range_id \in NonInitializedRanges(self):
                                                     LET replica_id == ReplicaIDForFollower(range_descs[range_id], self) IN
                                                       /\ Assert(raft_tombstones[self][range_id] < replica_id, 
                                                                 "Failure of assertion at line 447, column 15.")
                                                       /\ replicas' = [replicas EXCEPT ![self][range_id] =                             [
                                                                                                             present     |-> TRUE,
                                                                                                             initialized |-> FALSE,
                                                                                                             replica_id  |-> replica_id,
                                                                                                             keys        |-> {}
                                                                                                           ]]
                                                /\ UNCHANGED applied_log_indexes
                                          /\ UNCHANGED <<raft_tombstones, applied_writes>>
                                    /\ pc' = [pc EXCEPT ![self] = "ProcessFollower"]
                               ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                                    /\ UNCHANGED << replicas, raft_tombstones, 
                                                    applied_log_indexes, 
                                                    applied_writes >>
                         /\ UNCHANGED << active_ranges, destroyed_ranges, 
                                         next_range_id, range_descs, 
                                         range_to_right, raft_logs, 
                                         upgrade_to_learners, leader_done, 
                                         step, to_merge_lhs, to_merge_rhs >>

follower(self) == ProcessFollower(self)

DoVersionUpgrade == /\ pc["upgrade"] = "DoVersionUpgrade"
                    /\ upgrade_to_learners' = TRUE
                    /\ pc' = [pc EXCEPT !["upgrade"] = "Done"]
                    /\ UNCHANGED << active_ranges, destroyed_ranges, 
                                    next_range_id, range_descs, range_to_right, 
                                    raft_logs, leader_done, replicas, 
                                    raft_tombstones, applied_log_indexes, 
                                    applied_writes, step, to_merge_lhs, 
                                    to_merge_rhs >>

upgrade == DoVersionUpgrade

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == leader \/ upgrade
           \/ (\E self \in FOLLOWERS: follower(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(leader)
        /\ \A self \in FOLLOWERS : WF_vars(follower(self))
        /\ WF_vars(upgrade)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

=============================================================================
\* Modification History
\* Last modified Mon Sep 23 02:02:54 EDT 2019 by nathan
\* Created Fri Sep 20 12:13:06 EDT 2019 by nathan
