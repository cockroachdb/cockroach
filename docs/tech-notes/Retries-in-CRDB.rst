===============
Replays in CRDB
===============

:Authors: original author: Andrei Matei


.. contents::

Introduction
------------

This note talks about the different kinds of retries in CRDB, the hazards they
introduce, and the way we protect against them to maintain correctness. There
are four types of retries we're going to be discussing: reproposals of Raft
commands, re-evaluations of KV requests, retries of KV requests after conflict
resolution and retries of KV requests by the ``DistSender`` on RPC errors.

These are all actions that the code does consciously; none of them are
introduced transparently by a network transport or such. Executing things twice,
or executing them at a later point in time then when we intended the execution -
perhaps (but not necessarily) with some other operations having executed in the
meantime - can clearly lead to problems (e.g. a write executing at a later point
than it was intended can overwrite data that it was not intended to overwrite).
CRDB’s use of MVCC storage limits the consequences of such executions in some
ways (e.g. a write at a set timestamp cannot overwrite another write at a newer
timestamp), but that’s not sufficient for the semantics we need from our
operations. We’ll discuss how the batch sequence numbers and the Raft
``LeaseAppliedIndex`` prevent the hazards, and how losing track of an answer to
an operation can lead to an ``AmbiguousResultError``/``AmbiguousCommitError``.


Terminology
-----------

First of all, this note does not use the word "replay", even though our code has
used that word historically: in colloquial speech "replay" refers generally to
any kind of operation being executed a second (or 3rd, etc.) time. Of course,
operations in CRDB are not generally executed more than once out of thin air -
that’d be a terrible world to program in. Specific operations are attempted a
second time when we choose to do so. Different cases of repeated execution have
different consequences, and the code has different means of preventing the bad
ones, so we’ll refer specifically to the type of execution and the operation in
question when describing them.

This note will discuss the following types of retries, in different sections. They are ordered by the level at which they happen, from lowest level to highest.

.. note::

    Words have specific meaning in what follows; if you’re not familiar with
    terms like requests/commands/proposals/applications, see `the terminology
    section in this codelab
    <https://paper.dropbox.com/doc/Codelab-SQLKV-hacking-Ts6diBSjt89JDnJZ4uQRo#:uid=779028144703169196250253&h2=Intermezzo-3:-Terminology>`_.

Raft reproposal
  There are cases where we’ve *proposed* a
  command, it hasn’t *applied* yet, and we’ve grown impatient waiting for
  it to apply (or we suspect that it will never apply, but are not sure).
  In these cases, we will *repropose the command*: simply propose the
  command to Raft again. One thing to remember is that results of
  reproposals are not ambiguous: responses for the reproposals are
  indistinguishable for responses for the original proposal, and they
  represent the result of the **first application of the command**.

Re-evaluation of requests
  There are cases where we had previously proposed a command but something went
  wrong and the proposer finds out the the command cannot apply in the future
  (however, in some cases it's unclear whether it has applied in the past or
  not). In these cases we have to fully *re-evaluate the request* that generated
  the command, and propose the new command that resulted from the re-evaluation.

Conflict resolution retries
  FIXME - the Store retries replica.Send() upon a conflict having been resolved.

``DistSender`` retries
  The ``DistSender`` sends *requests* to a remote node for *evaluation* (and the
  proposal and application). In case of network errors, we don’t know if the
  remote node has performed all this, or not (depending on the error, sometimes
  we know that it hasn’t (e.g. failure to connect)). So, the DistSender sends
  the request again, to a different node (it’s true that generally a single
  node -  the lease holder of the respective range - can service any given
  request, but it’s also true that a ``DistSender`` doesn’t generally know with
  certainty which node that is - and in fact the lease can change hands when
  nodes go down - so retries may help).


Raft-level Issues
-----------------

The lowest level of retries happen because of Raft-level conditions. These
retries are of two types: reproposals and re-evaluations. Both of them are
triggered by Raft processing; they're triggered by
```replica.handleRaftReady()``. Reproposals are lower-level operation - they're
handled at this ```handleRaftReady()`` level. Re-evaluations are handled at a
higher level - ``replica.executeWriteBatch()``.

Let's discuss when they happen (more technicalities will follow in the `Logic
for triggering reproposals and re-evaluations`_ section):

#. When commands have been proposed but never made it to the leader (usually
   because we didn’t know who the leader is - think initial leader election
   after a range split). When we do find out about a new leader, we *repropose*
   pending commands.
#. When it’s been too long since a command was proposed and we haven’t heard
   back. In such cases, we *repropose*.
#. When the proposer finds out about a command attempting to apply out of order.
   In this case, we *re-evaluate* the request.
#. When the leaseholder replica has just applied a snapshot and the snapshot
   contained commands with log positions above the positions of some of our
   pending proposals. We don’t know what was inside the snapshot, so we don’t
   know which of the pending proposals have been applied. We’ll *re-evaluate*
   the request. Note that this snapshot case requires that the Raft leader be
   divorced from the leaseholder (otherwise, a leader doesn’t apply snapshots),
   so it should hopefully be rare because we try to colocate the two.

Next we'll discuss reproposals and re-evaluations in more detail.


Raft command reproposals
^^^^^^^^^^^^^^^^^^^^^^^^

.. sidebar:: Raft commands reminder

  Raft commands contain serialized RocksDB “batches” - low-level write
  instructions (e.g. “set value of key *k* to *v*\ ” or “delete key *k”).* These
  batches have been produced by the evaluation process starting from a given
  Rocksdb snapshot. At least given the way we use them, these batches can, as
  far as RocksDB is concerned, be applied on top of a snapshot that diverged
  from the snapshot on top of which they were produced - no error will be
  generated and there’s no need to “merge changes”. So RocksDB does not protect
  us from overwriting values we didn’t intend to overwrite. Also keep in mind
  that most of the keys we write to contain a timestamp.

Let’s discuss Raft reproposals first. They're conceptually simple: we previously proposed a command, we haven't applied it yet, and for various reasons we decide that we don't want to wait any more and so *we propose the same command* again, hoping for a better outcome. As described above, this can happen either because the proposer found out about a new Raft leader or simply because it's been a while since we proposed and we grew impatient.

Since we're proposing the command again, there's now two identical proposals in
flight. Unless we took special precautions, we generally wouldn't know that both
of them would not apply. Even worse, we wouldn't know that *one of them wouldn't
apply at an arbitrary later time* (in particular, a point in time after we've
taken the respective request out of the ``CommandQueue``; see below), since we're
only going to be waiting for one application of either of the two proposals
before returning control to the higher layers. Would these double applications
be bad?

What could go wrong because of double applications?
```````````````````````````````````````````````````

The type of bad things that can happen because of double application differs based on whether the second application happens while the request is in the ``CommandQueue``.

What could go wrong because of double applications with reordering?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The clearer to see case is when the second application happens after the request
has been taken out of the ``CommandQueue``: command applications at arbitrary
times is bad because it can overwrite data that it was not supposed to
overwrite. Consider:

#. While executing request R1, we propose command A which writes (RocksDB)
   key/val K=V1.
#. The proposer waits a while, doesn't hear anything back about the application
   of A, and so it reproposes it as command B (A and B are equivalent; the have
   the same `CommandKey`).
#. We hear back and apply one of them, A or B (we can tell which).
#. The stack is unwound and R1 is taken out of the ``CommandQueue``.
#. We execute request R2, which applies command C, which writes K=V2 (note that
   this could not happen while R1 is still in the ``CommandQueue`` because R1
   and R2 are overlapping).
#. The other application of A/B happens at this point, writing K=V1.
#. We're now in an unintended state: K=V1 when we should have K=V2.

What we see here is that double application of a command *combined with the
reordering of the second application wrt other commands* can create problems.
The first question that comes to mind is whether such commands A/B and C can
actually exist. Given that our MVCC layer translates KV keys into RocksDB keys
by appending a timestamp to them, doesn't that guarantee that keys at the
RocksDB level are unique between commands? The answer is no, for several
reasons:

- Meta-keys don't have timestamps. Meta-keys exist when intents are present on a
  KV key. Two writes to a meta-key that are reordered, (or a write and a delete)
  would be bad.
- Within a transaction, different requests can modify the same key (because they
  operate at the same timestamp).
- The ``GCQueue`` deletes tombstones. If these deletes would be reordered with
  the writing of the tombstone, that'd be bad.

The second question that comes to mind is how exactly can command C run before
the second application of A/B, given that C was proposed later? The explanation
requires the Raft message flow sidebar.

.. sidebar:: Raft message flow

  1. A *range leaseholder* *proposes* a command by sending a *propose*
     Raft message. This messages makes its way from the leaseholder to the
     *Raft leader* using the *Raft transport*.
  2. Once it arrives on the leader, the leader assigns it a *log
     position,* appends it to its own copy of the log, and sends *append*
     messages to all the *followers*.
  3. The followers agree on that log position, and when enough of them do,
     the command is considered committed and replicas begin *applying* it.

With respect to reorderings or Raft commands, we have some things go in our
favor: once a leader has assigned a log position, it will never assign a lower
log position to another command. So, once commands make it to the leader,
there’s no more risk of reordering. There is, however, a risk of reordering
*before* commands make it to the leader: when the leaseholder and the Raft
leader are not collocated, the process of *proposal forwarding* is employed:
commands are sent using the Raft transport protocol, which is not currently
under CRDB control and does not make any ordering guarantees. So, a leaseholder
may propose/forward command A, then give up on waiting and propose/forward B,
then apply A/B (at which point the stack unwinds and the request that generated
A is taken out of the CommandQueue), then propose C (which relies on the
ordering with A/B), B gets applied, and then, at an arbitrary later point, the
*propose message* for A gets to the leader, which appends it to the log at the
wrong position. We've just gotten the reordering that we were affraid of.

What happened here is that the *Raft transport* (the communication protocol used
for proposal forwarding) failed to ensure ordering for the messages it
transported. We could devise a transport protocol without this shortcoming:
something like sequentially going through a series of TCP connections and
ensuring that once connection 2 starts being consumed, connection 1 is not
consumed any more. If we were to do it, we wouldn’t need to worry about
reorderings at the Raft level any more.

We should emphasize once more that reorderings are only a problem if a command
is allowed to "escape" the ``CommandQueue`` (i.e. apply after the respective
request has been taken out of the ``CommandQueue``). Reorderings while a request
is in the ``CommandQueue`` would not cause any problems: the whole point of the
``CommandQueue`` is to allow parallel execution of non-overlapping requests. Their
commands could be allowed to execute in any order without problems. We should
also emphasize that reorderings require the Raft leader to be divorced from the
proposer (i.e. the leaseholder); that's the only case in which the Raft
transport's lack of ordered delivery comes into play.

This all points to an invariant we need around the ``CommandQueue``: we have to
ensure somehow that, once a request has been taken out of the ``CommandQueue``,
no proposal of the command that was generated on behalf of that request will be
applied. How we guarantee this we'll see later. We should note that commands
escaping the ``CommandQueue`` happen excusively in conjunction with reproposals:
a request is only taken out of the ``CommandQueue`` once a result is produced
for one of its command's reproposals (so, once the application of one of them is
applied). This means that, if there are no reproposals, we only take the request
out of the ``CommandQueue`` when we get a result for the application of the (one
and only) proposal. We take care for this to be true - for example, in the event
of a ``Context`` cancellation, we will spawn a goroutine for the sole purpose of
waiting for one of the reproposals to complete and only then removing the
request from the CommandQueue. However, as we'll see when we discuss ambiguous
errors, we might take a request out of the ``CommandQueue`` even though we're
unsure whether the command was applied successfully or not. FIXME: reference the
right section for ambiguous errors.


What could go wrong because of double applications without reordering?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We’ve just seen how a command generated by a request *R* could be applied after
*R* has been taken out of the ``CommandQueue`` and why that would be bad (if we
hadn't build special protection against it). So, we’re afraid of commands coming
back from the dead at arbitrary times, causing reorderings. We’re also afraid of
a special case: the double application of a command in quick succession (without
other overlapping commands in between). This is a hazard that doesn’t involve
requests being taken out of the ``CommandQueue`` in order to happen; it can
happen while the respective request is in the ``CommandQueue``. It simply
requires us to propose a command twice (which we do, as we've seen).

.. note:: Double application of commands could happen even if we had
   an ordered Raft transport; as long as we propose commands twice, the
   transport's guarantees don't matter in this respect. What it would take from
   the transport to obviate the need to propose commands twice would be
   reliable *exactly once delivery* semantics, which we're unlikely to get from
   a transport layer (except if we'd make the commands idempotent; see below).

What could go wrong if a command is applied twice, but there’s no risk of
unwillingly overwriting keys? Why aren’t commands idempotent if nothing
significant sneaked in between the two applications? The answer is that a number
of things make commands not be idempotent; this is all because of the
``ReplicatedEvalResult`` structure - a payload that commands have besides the
RocksDB ``WriteBatch``. This payload is interpreted downstream of Raft and
affects changes to the replica state:

- Each command carries an ``MVCCStats`` delta which is added to the range's MVCC
  statistics upon application. Add the delta twice and you have corrupt stats.
- Similarly, each command carries a ``RaftLogDelta`` having to do with Raft
  statistics.
- Other special commands carry other special side-effects (splits, merges,
  rebalances, leases, log truncation). Some of these are currently not
  idempotent.

The question that comes to mind is whether we could overcome the problems
introduced by these side-effects and make commands idempotent. For each special
command there's likely ways to detect a previous application [#allornothing]_.
For the stats, we could replace the delta with absolute values. This implies
that each command would imply the successful execution of all commands proposed
previously (and so, once the application of some command fails, we'd need a
mechanism to "flush the pipeline"). If this would allow us to stop caring about
double application, that'd be a win [#assuming]_. More importantly, **making
requests idempotent would obviate the need for re-evaluations**: as we'll see in
a future section, the point of re-evaluations is detecting whether a previous
attempt applied; if requests were idempotent, then by definition we wouldn't
care if it succeeded or not. The cost, though, would be, arguably, less
concurrency in the request evaluation phase: for requests that have been allowed
in the ``CommandQueue`` at the same time, evaluation can run in parallel. In
fact, even the application of these commands could in theory run in parallel
(although we don't currently do that) [#btw]_. If we had to compute absolute
values for the statistics, then we'd need to synchronize proposing the commands
so that the stats after every single command are correct [#but]_.

.. [#allornothing] Note that we probably don't need to make all commands
   idempotent to get benefits: even if the "special" commands are not idempotent
   but all the "regular" commands are, we'd still have solved the problem of
   ambiguous errors related to re-evaluations bubbling to SQL clients.
.. [#assuming] Assuming we would also do something to get rid of the hazards
  related to reorderings discussed in the previous section. Otherwise, the
  ``LeaseAppliedIndex`` sequence numbers introduced to protect against those
  give us protection against double application for free.
.. [#btw] By the way, if we're evaluating requests in parallel, we could
  actually propose a single Raft command representing the sum of all the
  respective commands (i.e. ``WriteBatches``) if these commands are produced
  close in time to one another. That might save some Raft work at the cost of
  increased latency for some of the requests. This optimization would be
  possible with or without absolute values for the statistics.
.. [#but] However, the ``LeaseAppliedIndex`` mechanism already requires some
  degree of synchronization between proposals, for assigning the sequence
  number and for proposing in the sequence number order. So it's not clear to
  the author if the we'd lose any parallelism if we'd also assign the stats
  values in order.


The solution to the hazards: the ``LeaseAppliedIndex``
``````````````````````````````````````````````````````

The solution CRDB has for all these hazards is the ``LeaseAppliedIndex``
mechanism: each proposed command gets tagged with the proposer's current lease
and with a desired log position (relative to a lease command, which is related
to the point where the proposer can change), and replicas maintain the highest
such number that was applied. The ``command.MaxLeaseIndex`` is assigned at
propose time. At application time, the current lease is checked to be the same
as the one under which the command was proposed (so, when the lease changes, all
commands that are still in the pipeline are instantly invalidated [#evenif]_),
and the ``MaxLeaseIndex`` is checked against ``replica.LeaseAppliedIndex`` and
application is only allowed if ``MaxLeaseIndex > LeaseAppliedIndex``. So, the
``LeaseAppliedIndex`` prevents commands from being applied beyond their intended
log position. When we repropose commands, we repropose them with the same
``MaxLeaseIndex`` as the original proposal. When a command attempts to apply
beyond it's intended position and is rejected, we have the option of
re-evaluating the request (see `Re-evaluations of KV requests`_); we'll take
this option if we're still waiting for the application result for the command in
question (i.e. if we previously got a result for a reproposal, we will not take
this option). We do not have the option of simply reproposing (the reproposal
would keep failing in the same way).

The ``LeaseAppliedIndex`` mechanism prevents both reordering and double
applications (since reproposals have the same ``MaxLeaseIndex`` as the original
proposal). Again, if we had an ordered Raft transport, that would solve the
reordering problem but not the double application problem.

One thing to note is that the ``LeaseAppliedIndex`` has the effect of
serializing command application more than we really need: commands proposed by
requests that were part of the command queue at the same time (or, in fact,
requests that could have been in the ``CommandQueue`` at the same time) don't
need this serialization.

Another thing to note is that the ``MaxLeaseIndex > LeaseAppliedIndex``
comparison only prevents application of commands *beyond* their intended log
position. It does not prevent commands from applying *before* their intended log
position. Should it, considering that a command applying earlier than intended
implies a type of reordering? The answer is no, considering that our commands
have been crafted such that they don't depend on the successful application of
commands concurrent with them in the ``CommandQueue``. However, whenever we
apply a command before it's intended position, we're likely to cause other
commands to fail to apply because they will now be considered to have missed
their position.

.. [#evenif] Note that this is true even if the leaseholder moves and then comes
  back to the original node while a command is in the pipeline - that command
  will not be allowed to apply.

.. note:: Reminder that reproposals never introduce any ambiguity: we only repropose when we know that the original proposal has not been applied yet.


Re-evaluations of KV requests
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As we've seen in the previous section, commands cannot apply past their assigned
``MaxLeaseIndex``. So, what happens when a reordering causes a command to fail
that check? We saw that we can't just repropose (well, in fact, I believe
sometimes we could, see TODO_repropose_on_proposalIllegalLeaseIndex_). We have a
couple of options:

- If we know for a fact that the proposal did not apply, we can do a variant of reproposing: propose the same command but with an updated ``MaxLeaseIndex`` and a new ``CommandKey``. We know that the command has not applied and also that no reproposal that we may have made (if any) will also not apply, we know that no overlapping command has been proposed yet (we're still in the ``CommandQueue``), so we can just propose the command again at a new log position.
- If we don't know whether the proposal applied or not:

  - We could raise our hands and return an error to the client. The error would
    be ambiguous, though, reflecting the fact that we don't know if data has been
    written or not. If the command was transactional but it was not committing
    the transaction, a higher layer could translate it into a retriable
    transaction error (although we never do that now, see
    TODO_convert_to_retriable_). This option is quite unfortunate.
  - We could attempt to resolve the ambiguity in one direction: we can evaluate
    the request again and, if the evaluation succeeds, conclude that the command
    did not apply [#noreverse]_. The reasoning allowing this conclusion will be
    presented in the `How does MVCC detect previous applications of commands
    during evaluation?`_ section.

While writing this note, the author has come to think of re-evaluations as
fundamentally being about (and only about) attempting to prove that a command
did not previously apply in the hope that we can avoid bubbling up an ambiguous
error. Everything else about them is noise. Another interesting point is that
re-evaluations don't introduce any reordering hazards like reproposals do: once
we decide to re-evaluate something, the original proposal can no longer apply
(in case it hasn't applied already).

These were abstract considerations. Now let's see what the code actually
does. First of all, it doesn't take advantage of the fact that re-evaluation is
not needed when we know that the command did not apply; we do the re-evaluation
anyway, meaning that we have cases where re-evaluations can result in ambiguous
errors and cases where they don't. This should be improved: only the ambiguous
case should remain.

The interesting case is the ambiguous one. The code attempts to resolve the
ambiguity as described in our final option. If it can't (because re-evaluation
fails), an ambiguous error is bubbled up and will currently make its way to the
client (although it sometimes could be turned into a retriable error, as
described).

A good question is how exactly does the ambiguity appear in the first place?
We're doing reproposals when we're waiting for a command to apply and we've
heard that a command with a higher lease index has applied. Can't we conclude
from this that the command we've been waiting for has not applied? If it had,
wouldn't the proposer have known about it (because the proposer is one of the
replicas in the respective group)?  Well, that reasoning almost holds, except in
one case: if the local replica has just applied a snapshot (and so moved up it’s
``LeaseAppliedIndex`` that way), then it’s possible that the snapshot contained
the commands in question and the proposer wouldn’t have received a result (we
don’t notify the proposers when applying a snapshot, as we don’t know what’s
inside a snapshot). This snapshot applied case is the only case where
re-evaluations are truly necessary.


.. sidebar:: Re-evaluation is too heavy weight?

   We've said that the point of re-evaluations, fundamentally, is to detect
   whether the command has been previously applied by running the MVCC code. If
   the command did not, in fact, apply previously then we expect to end up with
   the *exact same Raft command as before* as the result of evaluation (modulo
   caveats noted below). The structure of our code, however, currently does not
   make that clear: when we do re-evaluations, we lose any context of the
   previous attempt, except the information that the error returned to client
   must be ambiguous if the re-evaluation fails. In the author's ideal world,
   we'd structure the code such that MVCC checks are divorced from Raft command
   generation (i.e. ``WriteBatch`` generation). Besides making this point
   apparent, this kind of structure could perhaps provide some performance
   improvements by saving on work - no duplicate ``WriteBatch``, no duplicate
   RocksDB snapshot (we'd use the original snapshot).

   Unfortunately, there's a complication that we've previously hinted to:
   because we take the request out of the ``CommandQueue`` and we re-insert it
   with every re-evaluation, we open the door to other overlapping requests
   sneaking in. This means that re-evaluations can actually legitimately result
   in different results. That's unfortunate; for this and other reasons I think
   we should keep requests in the ``CommandQueue`` throughout re-evaluations.
   One interesting point, though, is that prop-eval-KV has opened the door to
   introducing non-deterministic requests in the future. Whether that'd be sane
   and, even assuming that it is, whether we'd want such requests to actually
   produce different results on re-evaluations may be questionable.

.. note:: One important thing to note is that re-proposals are done at a high
   level in the ``replica`` code, in `replica.executeWriteBatch() <https://github.com/cockroachdb/cockroach/blob/eac867158e1cb6864046b7b5a14c1c7e62f6f62b/pkg/storage/replica.go#L2489>`_.
   This means that they're done without the request staying in the
   ``CommandQueue`` continuously throughout all the re-evaluations. I think
   that's unfortunate, as it opens the door to other overlapping requests
   sneaking in and causing ambiguity (see TODO_remove_snapshot_ambiguity_).
   This fact also causes re-evaluations to be needed in a way that they
   shouldn't be, muddying the conceptual waters; see the "Re-evaluation is too
   heavy weight?" sidebar. Also, by
   keeping the request in the CommandQueue we’d arguably also get a more
   favorable ordering of the commands - the arrival order, which resembles
   the timestamp order (so WriteTooOld errors could be reduced).


.. [#noreverse] The reverse does not hold, though: if the re-evaluation
  fails, then we can not currently conclude that the command applied. The
  re-evaluation may fail because of other commands that have been applied since
  the original proposal. See also TODO_remove_snapshot_ambiguity_.

How does MVCC detect previous applications of commands during evaluation?
`````````````````````````````````````````````````````````````````````````

We're owing an argument after the previous section; we've seen that
re-evaluations are done in order to try to remove the ambiguity about whether a
command applied or not by performing the evaluation again and hoping that it
succeeds. Why exactly is it that a successful evaluation proves that the command
did not previously apply? What's preventing the re-evaluation from succeeding
even if the command had previously apply? The answer depends on the type of the
request. But the point is that we've made sure there's always something that
makes this true:

- A non-transactional batch (or a 1PC transaction) relies on MVCC: a second
  evaluation will get a ``WriteTooOldError``. In some cases, there's no data to
  generate a ``WriteTooOldError``: for example, if the batch only consists of
  ``DelRange`` requests and don't write anything (because there was no data to
  delete). In such cases, we rely on hitting the timestamp cache check and
  refusing the second application that way? But I'm not sure about this -
  depending on when exactly we populate the timestamp cache, wouldn't we either
  hit it when we do the re-evaluation regardless of whether the first one
  applied or not or, if we populate it late, would it ever be populated by the
  first application?  Perhaps nothing prevents such commands from applying twice
  (and that's OK because they really are idempotent)? On the other hand, if a
  request doesn't write any data that would generate a ``WriteTooOldError`` on
  re-evaluation, do we even need to propose such commands to Raft? FIXME.
- A batch containing a ``BeginTransaction`` request relies on the transaction
  entry already existing (a second application would get a
  ``TransactionStatusError``).
- If there's an ``EndTransaction`` in the batch, then the transaction
  record might have been cleaned up by a previous application. In this case, we
  rely on the ``TimestampCache`` entry that the ``EndTransaction`` purposefully
  leaves behind for this purpose [#]_ [#]_. I think we could also rely on the
  transaction status check: if the ``EndTransaction`` previously applied but
  the txn record has not been cleaned up, then we can also rely on getting a
  ``TransactionStatusError``?
- Batches containing regular transactional writes rely on the sequence numbers
  mechanism (described later).
- Other special requests (leases, splits, etc.) rely on ad-hoc mechanism. Some
  of them are tied to an ``EndTransaction``, so they use its protections. The
  ``ProposeLease`` command, for example, relies on the current lease verification [#]_.

.. note:: The ``LeaseAppliedIndex`` mechanism has nothing to do with protections
  in case of re-evaluations, as re-evaluations propose commands intended for
  different log positions than the original command.

.. [#] If there's an ``EndTransaction`` in the batch, that means we also wrote
   something in that transaction, so we could also rely on ``WriteTooOldError``
   on the second application. However, relying on this and not on the
   ``TimestampCache`` entry would have the disadvantage that the second
   application would leave dangling intents and a dangling transaction record.

.. [#] I made a note here that the ``GCThreshold`` also somehow plays a role
   here but we should replace it with populating the timestamp cache, but now I
   can't reproduce what this was about. Since we have the ``EndTransaction``
   timestamp cache entry... Perhaps it was not about batches with
   ``EndTransaction``, but internal batches whose intents have been cleared.
   FIXME @tschottdorf

.. [#] The ``ProposeLease`` command is really special because it can be proposed
   by any follower.


Logic for triggering reproposals and re-evaluations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This section discusses the mechanics of the code that decides about reproposals
and re-evaluations.

The code triggers re-evaluations when an `application returns a special error
<https://github.com/cockroachdb/cockroach/blob/47287e0b94c99908515669866f71e9a3a704e4e6/pkg/storage/replica.go?utf8=%E2%9C%93#L4107>`_
and does both reproposals and re-evaluations when the pending proposals
are “refreshed” for any number of reasons in the `replica.refreshProposalsLocked(reason) <https://github.com/cockroachdb/cockroach/blob/47287e0b94c99908515669866f71e9a3a704e4e6/pkg/storage/replica.go#L3758)>`_
method. This method is written in an open-loop manner; it inspects all
pending proposals at points in time that can be considered arbitrary,
and decides what to do for every single one. There’s a number of cases:

#. A proposal can still apply according to their lease index slot
   (``command.MaxLeaseIndex > LeaseAppliedIndex``). They can still apply
   and they might apply, but it’s also likely that they’ve been dropped
   on the floor. We repropose these commands. If the original proposal
   still applies, the lease index will prevent the 2nd application.
   After the reproposal, any application of the command (the original or
   the reproposed one) will provide a ``proposalResult`` to the
   higher-level request’s control flow, which is blocked on receiving a
   result for the proposal. It doesn’t matter if the original gets
   applied; it’s all good. If the ``proposalResult`` we get indicates an
   application error, **there’s no ambiguity** - it’s the result of the
   first attempt to apply.
#. A proposal’s lease index slot was filled up: the proposal cannot be
   applied from this moment on (it’s application-time check will fail).
   These proposals are still pending, so they obviously haven’t received a
   result. We can't repropose (the reproposal would fail the lease index
   check), so we re-evaluate. If the reason why
   ``replica.refreshProposalsLocked()`` was called is a snapshot application,
   we keep track of the fact that the status is ambiguous and, if the
   re-evaluation fails (so, we haven't managed to resolve the ambiguity), we
   wrap the error into an ``AmbiguousResultError``. In other cases the error is
   not ambiguous and is returned directly (but these other cases should really
   be handled by a special type of reproposal, as discussed before).


The code that actually performs re-evaluations is, as we've discussed before, in `replica.executeWriteBatch() <https://github.com/cockroachdb/cockroach/blob/eac867158e1cb6864046b7b5a14c1c7e62f6f62b/pkg/storage/replica.go#L2489>`_. This is a high-level function, and so
each re-evaluation is done by re-entering the ``CommandQueue``.

Notes
^^^^^

In this section we’ve only considered reorderings of commands pertaining to
different KV requests (the ``CommandQueue`` serializes KV requests). A question
that might come to mind is whether there could be problems with reorderings of
commands within a request: could request 1 generate commands A and B that need
to be applied in this order? The answer here is that this is not a concern:
below the ``DistSender`` level, a request gets translated into a single command
(modulo reproposals/re-evaluations). The ``DistSender`` splits batch requests in
smaller batches such that each batch can be translated into a single command. It
does this by splitting the original batch at read points. There is one
complications here around conflict resolution - when the evaluation of a request
encounters a conflict, the conflict needs to be resolved by proposing some
commands. So, in a way, this contradicts what we’ve said above: that a request
evaluation results in a single command. The key here is that conflict
resolutions happens at the ``Store`` level, and request evaluation happens at
the ``Replica`` level: the request is taken out of the ``CommandQueue`` while the
conflict is being resolved, and put in the ``CommandQueue`` again afterwards.


.. _TODO_repropose_on_proposalIllegalLeaseIndex:

**TODO:** in the ``proposalIllegalLeaseIndex`` case (i.e. ``command.MaxLeaseIndex <
LeaseAppliedIndex`` and ``refreshReason != reasonSnapshotApplied``), we should
repropose instead of re-evaluate. But re-propose with a changed commandKey and
lease index, and update the key on which the proposer is waiting for a result.
We know that the original hasn’t applied and also that it will not apply, and we
know that there haven’t been overlapping requests evaluated after we proposed
(we’re still in the CommandQueue), so the re-evaluation is not needed.

**TODO:** `The comments on MaxLeaseIndex <https://github.com/cockroachdb/cockroach/blob/47287e0b94c99908515669866f71e9a3a704e4e6/pkg/storage/storagebase/proposer_kv.proto#L188>`_
say they should be updated when the CommandQueue has been fixed for
prop-eval-KV. They point to
`#10413 <https://github.com/cockroachdb/cockroach/issues/10413>`_,
which has since been closed. Should they be re-written to
mention arbitrary reorderings and corrupt statistics?

.. _TODO_convert_to_retriable:

**TODO:** ``AmbiguousResultErrors`` are never transformed to txn
retry errors, as far as I can see. They’re converted in SQL to errors
with a standard pg code: “statement completion unknown”. But this means
that we don’t do automatic retries when we could (to hide some of these
errors), and also that our documentation and client libraries don’t
handle the retries properly when they could (when the error is not
associated with a COMMIT). We should transform them to ``TxnRestartError`` when
no commit is involved.

.. _TODO_remove_snapshot_ambiguity:

**TODO** If we did re-evaluations *while
keeping the request in the CommandQueue* **the snapshot applied case would not
be ambiguous**. We'd be able to compare the re-evaluation result with the result
of the original evaluation and, if they're not exactly the same, then we could
conclude that the original proposal was applied. This way we'd resolve the
direction of the ambiguity that's currently unresolved. There may be difficulty
with variance in some conditions that make evaluation non-deterministic (e.g.
leases moving around and the timestamp cache overflowing), but it seems that at
least a category of MVCC errors would be unambiguous.

``DistSender`` retries
----------------------

.. warning:: EVERYTHING BELOW IS WIP, NOT READY FOR REVIEW

Besides Raft-level issues, the other big source of headaches around
“executing things multiple times” is the ``DistSender``, which retries
the sending of batches from one replica to another. Why does the
``DistSender`` retry batches, you ask?

Reminder: the ``DistSender`` is responsible for splitting a
``BatchRequest`` into range-specific batches and sending them to the
respective lease holders. At any given time, there’s a single node that
can evaluate a given request, but the idea is that the ``DistSender``
doesn’t necessarily know who that node is (e.g. it might have stale
lease information cached), and also that the lease holder can change
when there’s trouble (so, a retry to a different replica might cause
that replica to acquire the lease). Therefore, the ``DistSender`` retries
batches when they may execute successfully on another replica. The
``DistSender`` may also attempt to execute batches on the same node multiple
times. Currently, the policy around these retries seems more accidental
than intended and I don’t think it currently helps anything - see below.
But there are policies involving same-node retries that would make
sense.

Let’s talk specifics: the ``DistSender`` sends an RPC and gets a RPC
error (i.e. a network error). The error can be ambiguous about whether
the original replica might have received the RPC or not (in fact, as of
this writing, it would appear that **all gRPC network errors**
\*\*\*\*\*\*\*\*are ambiguous)\*\*. So, what is the DistSender to do
now? It has a few options:

1. Return the error to the client. The error should reflect the
   ambiguity about whether the batch (and, by extension, the respective
   SQL query) applied to the database or not, and so the client would
   have to deal with the pesky ambiguous error.
2. If the batch was transactional **but didn’t contain an
   EndTransaction**, it could return a retriable error to the client.
   The transaction would be restarted and we wouldn’t care if the
   preceding batch executed or not.
3. Try to resolve the ambiguity, at least in one direction: if we can
   figure out that the batch did not, in fact, execute, then we can (and
   should) attempt to execute it again.

The DistSender implements a combination of all these options. On RPC
errors, it first tries to prove that it hasn’t executed. Turns out that
“proving that it hasn’t executed” and “attempting to execute it again”
are merged: we attempt to prove that it hasn’t executed *by attempting
to execute it again*. If attempting to execute again succeeds, that
means that the previous attempt didn’t succeed (or, at least, that the
previous attempt *hasn’t happened yet*). Why is that? This ties into the
previous section on Raft-level retry behavior: that lower layer
guarantees that a request being evaluated after it’s already been
applied - reminder: these are commonly MVCC protections. We don’t need
to worry about two concurrent evaluations because the CommandQueue will
prevent them. So, the DistSender tries to execute the batch again.
Currently, the attempt is always done on another replica !!! explain
that if we never went back to the original replica, some problems would
go away!!!. This can have a couple of outcomes:

-  It succeeds. That’s great; we can tell the client that we succeeded.

It proceeds to try the RPC on another replica. What problems can arise
from executing the RPC twice, on two different replicas? The question is
legitimate: the combination of isolation between transactional semantics
and MVCC for timestamped writes makes the answer be non-obvious, at
least for transactional writes. One problem is that a request such as
increment is not idempotent (even considering that it operates at a
fixed timestamp): if executed a 2nd time inside a transaction, it will
increment the value a 2nd time (the transaction part is important
because transactional requests see the writes previously performed in
the same txn; if the increment is not transactional, I think a 2nd
execution would fail because it tries to write at the existing
timestamp?). Another problem is caused by request reordering. Consider
the following scenario:

1. a client sends *Put(x, 1)*
2. DistSender sends it and gets a connection error. However, the server
   has received the request.
3. DistSender sends the request again to a different replica, and this
   time the RPC succeeds.
4. the client sends *Put(x, 2)*
5. DistSender sends it and succeeds
6. the original *Put(x, 1)* comes back from the dead and is applied,
   thus overwriting the *x = 2*

CRDB protects against these hazards by attaching *sequence numbers* to
batches. These are assigned by the DistSender and are increasing
throughout a transaction. Writes persist their sequence number in
intents and MVCC refuses to evaluate a read or write if it encounters
data written by the same transaction at a lower or equal sequence
number, concluding that it must be an out of order or repeated
evaluation. So, in our example, the *Put(x, 1)* that comes from the dead
will error out instead of resulting in a Raft command proposal.

The problem with ``DistSender`` doing retries in situations where it’s
ambiguous whether a node it was trying to talk to received the RPC or
not is that, if all the subsequent attempts fail (for example, with
``NotLeaseHolderError``) then the ``DistSender`` has to return an
ambiguous error to its client - we don’t know if the request will be
applied or not.

There’s also another hazard induced by ``DistSender`` retries that’s not
covered by the sequence numbers: the retry of a ``BeginTxn`` can cause a
transaction record to be recreated after it has been cleaned up. In
turn, this can cause the transaction to be committed later on even
though it’s missing some intended writes. Concretely, the risk is:

-  the ``DistSender`` sends a batch with a ``BeginTxn``
-  the transaction is abandoned for whatever reason, and the intents it
   had already written are cleaned up.
-  the ``DistSender`` retries the ``BeginTxn``, and creates the txn
   record again
-  more intents are produced
-  the transaction is successfully (but wrongly) committed

The problem here is that the txn record has been recreated after it had
been cleaned up. This is protected against by placing a special entry in
the timestamp cache at ``EndTxn`` time, preventing future attempts from
writing that key. Note that the ``AbortCache`` does not have !!!

-  I should discuss splits and how that means that there’s two leases
   under which a request can be evaluated. But it’s OK because both of
   those leases are held by the same node (and the timestamp cache is
   shared); as soon as one is transferred, the timestamp cache is reset.

-  can we get rid of SeqNums because a request can only apply under one
   lease (courtesy of the timestamp cache)? Assuming the DistSender
   doesn’t send the same request twice to a node.
-  is there a subtle problem with CPut not updating the TimestampCache?
   Should that be discussed somewhere here? Or does it now update the
   TimestampCache, just like DelRange does? (see around here:
   https://github.com/cockroachdb/cockroach/issues/626#issuecomment-154159006)

Conflict resolution retries
---------------------------

AmbiguousError
--------------

Misc
----

-  AbortCache - protects against situations where reads miss previous
   writes because the intents have been cleaned up (txn has been
   aborted). Has nothing to do with retries.
