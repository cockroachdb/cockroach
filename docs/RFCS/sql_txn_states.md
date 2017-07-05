- Feature Name: SQL Executor clean-up: Transaction state machine
- Status: draft
- Start Date: 2017-07-05
- Authors: knz
- RFC PR: (PR # after acceptance of initial draft)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFCs proposes to clean up the code in `executor.go` that manages
SQL transactions and the interaction between SQL transactions and K/V
transactions.

To achieve this, the RFC:

1. "formalizes" the current behavior of transaction management by
   *describing* it outside of the Go code. The outcome of this is a
   *model* that can help us better *understand* what is going on.
2. outlines an algorithm that can reliably transform a description of
   any state machine description into Go code, with reasonable
   confidence that the code does the same thing as the
   specification. This algorithm is primarily intended for humans
   audiences but a translation to an automatic generation tool can be
   envisioned as well (presumably later).
3. proposes a new process whereby SQL contributors are invited to
   submit any changes to the txn machinery *first* as an update to the
   model, and *only then* as a change to the Go code following the
   principles outlined in point 2, so that reviewers can
   systematically enforce confidence that the code is actually
   achieving what the author of the change intends to achieve.

# Motivation

1. The [bus factor](https://en.wikipedia.org/wiki/Bus_factor) for the
   executor logic is currently 1 (Andrei).

2. The executor logic has grown rather large with multiple
   layers of functionality that influence and depend on the state a
   transaction can be in:

   - automatic server-side retries
   - client-directed retries
   - statement error handling
   - parallel statement execution
   - session tracing (behaves differently within and across txns)
   - table lease acquisition
   - determining permissible schema updates
   - AST caching, and later plan caching

   The current intricacy of the Go code makes it hard to
   change any of these functionalities with any confidence that
   the change won't break any of the others.

3. The complexity of the state being manipulated implies the number of
   possible different states is ginormous and thus cannot be fully
   exercised by unit testing alone. (This is incidentally the main
   reason why even every engineering-driven teams end up adopting
   some form of formal verification.)

4. We already have a couple of serious bugs (serious as in "the
   database may eat your data"-serious) where we have no idea what the
   cause is because this code is so complex. For example #7881 and #14282.

5. Some new developments are currently neigh-impossible because the
   code is too complex. For example:

   - detection of client abandonment of the session
   - implementing txn timeouts
   - implementing result streaming through pgwire

# Background: finite state machines

You may have heard about or studied
[finite state machines](https://en.wikipedia.org/wiki/Finite-state_machine)
(FSMs, also known as finite-state automata or FSAs).

## What FSMs are

A FSM is primarily a mathematical construct: it is defined as a graph
of a finite set of vertices called *states* and edges between them
called *transitions*. The transitions are in turn *labeled*.

A FSM on its own does not "do" anything. To "use" a FSM one either
uses it as input to FSM-to-FSM transformation algorithms, or pairs it
together with an algorithm that "applies" it to some input stream of
events; this latter algorithm is called *evaluation*.

## What FSMs look like

As a mathematical construct the primary "encoding" of a FSM is a
formula (not relevant here), whereas there are multiple
*representations* possible for human convenience: as a planar graph,
as a textual inventory of the edges (e.g. in the
[dot language](https://en.wikipedia.org/wiki/DOT_(graph_description_language)),
as flowchart diagrams, as
[UML](https://en.wikipedia.org/wiki/UML_state_machine#Basic_UML_state_diagrams),
as
[SDL](https://en.wikipedia.org/wiki/Specification_and_Description_Language),
but also as source code in some programming languages. These
representations can with discipline be made equivalent to each other
**so that transition from one representation to another becomes
effortless.**

## How and where are FSMs used

FSMs are commonly found in software systems that implement lexical or
syntactic analysis of input text (= scanning/parsing).

For example, good implementation of regular expression matching
engines will first translate a regexp to a naive FSM and then apply
two classical FSM-to-FSM optimization algorithms to turn it into a
simpler FSM —
[one](https://en.wikipedia.org/wiki/Powerset_construction) and
[two](https://en.wikipedia.org/wiki/DFA_minimization), and then
evaluate the result FSM over the input text, which is very fast.

FSMs are also sometimes used as a specification tool for simple
stateful systems. The key word here is "simple": when considered as a
machine that changes state over time, a FSM's only output is the
stream of state updates. In a real-world computer this can only be
used to update a single variable in memory or perhaps the state of a
LED on a panel or the variables involved in a conditional assertion to
guard the behavior of another system.

Also, FSMs are most commonly used as a modeling tool to
understand (a subset of) the state changes in a more complex
system. For example, one can use a FSM to understand at a high level
the lifecycle of a SQL transaction:

```
                        .-[stmt ok]-.                .--[stmt]-.
                        '--.     .--'                '--.   .--'
 (start)                    \   v                        \  v
  NoTxn ------[begin]------> Open -----[stmt error]----> Aborted
  ^  ^                        |                            |
  |  +---[rollback/commit]----+                            |
  +----------------------[rollback]------------------------+
```

In both the case where a FSM is used as a modeling tool or a
specification tool, what this buys the user is the ability to formally
*prove* that some states can never be reached under specific
conditions, or that some states can only be reached under some other
conditions. For example the small diagram above can be used to prove
that there must be at least one ROLLBACK and a new BEGIN to bring a
transaction back into the "open" state after an error has occurred.

(This sounds obvious? It is certainly when looking at this diagram!
However, consider that proving the same by looking only at the current
`executor.go` in CockroachDB would be a significant undertaking. The
point here is precisely that the simple model makes it easier to
reason about the behavior of the complex code.)

Finally, the FSM abstraction has convenient visualizations as state
diagrams, which can subjectively be argued to be simpler to understand
to humans than the equivalent fully textual description.

# Background - connected state machines

Usually a system contains two or more interconnected stateful objects which can each be
individually described by a state machine.

In CockroachDB this occurs for example for the following 4 objects:

- the SQL session, with states "open", "traced w/o txn", "traced w/ txn".
- the SQL transaction, with states "none", "open", "retryable", "retrywait", "aborted"
- the K/V transaction with states "none" and "open"
- the pgwire connection, with (currently) states "ready", "sending results", "copyin", "closed"

When this happens an apparent conflict of interest emerges:

- on the one hand, engineering best practices encourage to isolate the
  sub-components of the system to be studied in isolation. To achieve
  that one will implement software interfaces in code and reason about
  the local state changes as a function of incoming API calls. The
  *effects* of each action by the local state machine are considered
  to occur in a vacuum, i.e. there is no local reasoning possible
  about how an effect can influence future input.

- on the other hand, correctness demands that one be able to reason
  over the *combined* state changes, to determine some invariants with
  a high confidence, for example that "no KV transaction is open if
  there is no SQL transaction" or "no SQL txn is active when the
  pgwire connection is closed".
  For this, pure FSM theory would require to formalize the combined
  state machines where each state describes the current values in each
  of the sub-objects. The resulting model would be very complex.

The way forward from there is to ask "is there a FSM-like
formalization which enables one to describe the FSMs for sub-objects
separately, in a way that still provides reasoning power about the
combination?"  The answer is yes: to achieve that, one would use a
particular kind of FSM called a *transducer*, and "connect" the
transducers together by merely ensuring they are defined over the same
set of basic events. From there there exist algorithms that
automatically merge the transducers together.

## What FSTs are

A
[finite state transducer](https://en.wikipedia.org/wiki/Finite-state_transducer)
(FST) is an FSM where every transition label has two components: an
*input* condition that determines state changes during evaluation, and
an *output* event that is generated when the transition is taken.

## How FSTs can be used for checking connected FSMs

Let's take a semi-artificial example outside of CockroachDB to illustrate.
This example is about the control panel of a mechanical lift.

There are two sub-components here: the lamp on the panel and the lift itself.

The lamp on the panel has two colors green and red, and can only
blink when lit red. It has possible input events R "turn red", G "turn
green", B "start blink", XB "stop blink".

Its state machine that describes the restriction is as follows:

```
  (start)
  green  --[R]--> red --[B]--> blinking
    ^------[G]----' ^---[XB]----'
```

The lift control has a button GU "go up" and another GD "go
down". There is a lift sensor which tells U "is up already " and
D "is down already". The lift control can be thus modeled with this FSM:

```
  ,-[GU]-,           ,-[GD]-,           ,--[GD]-,
  \      v           \      v            \     v
  lift up ---[GD]--> going down --[D]-->  down(start)
        ^             |    ^              |
        |            [GU] [GD]            |
        |             v    |              |
        '----[U]---- going up <----[GU]---'
                      ^      \
				      '-[GU]-'
```

Now what if the specification says the lift control must ensure the
lamp blinks red when the lift is going up, red without blinking when
the lift is going down, and stays green in every other case?

For this we extend the labels of the lift control with output events taken
from the lamp's input event alphabet:

```
  ,-[GU]-,           ,-[GD]-,           ,--[GD]-,
  \      v           \      v            \     v
  lift up -[GD|R]--> going down --[D|G]--> down
        ^             |    ^                |
        |         [GU|B] [GD|XB]            |
        |             v    |                |
        '--[U|XB,G]- going up <--[GU|R,B]---'
                      ^      \
				      '-[GU]-'
```

For example, we changed the `GD` edge, from "lift up" to "going down",
to `GD|R`. This means, "when the lift is up, and the 'go down' button
is pressed, make the lift go down and also trigger the R event for the
lamp." The lift control FSM is otherwise structurally unchanged, which
gives confidence we are not doing something else entirely with the lift.

For this simple example, one can perhaps satisfy themselves by just
looking at the lift control FST that the lamp will never be brought in
a non-existent "blinking green" state. However, we can automate this
check by asking the following question to an algorithm:

"is the *output language* (the set of all possible strings of output
events) produced by the lift control FST a sub-language of the lamp's
FSM *input language* (the set of event strings that ensure no
impossible state is reached)?"

An algorithm can compute the expansion of the lamp FSM according to
the transitions in the lift control FST, and answer yes (all is good)
and no (spec is wrong) to the question above automatically. This beats hand
checking!

## Mealy machines: special casing FSTs

For implementation convenience and performance it is advantageous to
restrict one's use of FSTs to those that are *deterministic*: for any
given current state and input event, there is at most one transition
with that event to another state. A FST that is also deterministic is called a
[Mealy machine](https://en.wikipedia.org/wiki/Mealy_machine). There
exist algorithms that automate the translation of a Mealy machine (the
mathematical construct) to program code that does the same thing in
the real world, very fast.

# SQL Transaction FSMs

## A gentle introduction

We model the SQL txn FSM as a state machine using incoming SQL
statements and their results as alphabet. The events are:

- C: COMMIT
- R: ROLLBACK
- SOK: statement, run without error
- SERR: statement, produces error

The FSM description for this is in dot:

```
 digraph simplesql {
  NoTxn -> Open [label=B];
  Open -> NoTxn [label="C"];
  Open -> NoTxn [label="R"];
  Open -> Open [label=SOK];
  Open -> Aborted [label=SERR];
  Aborted -> NoTxn [label=R];
 }
```

You can copy-paste this in the online viewer here: http://sandbox.kidstrythisathome.com/erdos/

Note that the transitions that are not expressed in the FSM are by
definition impossible. This is realized in a code representation of
the FSM by an error condition that will reject the event (e.g. a
COMMIT when in the state NoTxn). There is no need to express this
impossibility explicitly.

## Separating states to isolate effects

The example above is doing something suspicious: it splits the
"incoming statement" event into two separate events "run without
error" and "produces error". Meanwhile the explanation above also
states that the FSM "automatically" rejects impossible
transitions.

How can that be?  In order for this FSM to reject an event it must
first know about that event. So for example if a statement occurs
while in state "Aborted", the FSM only knows there was a statement
when presented with either SOK or SERR. However to construct either of
these states, the statement must be run, and this is not what we wish:
a statement should not be run *at all* in that state.

To describe this, we must separate the presentation of a new statement
(when it appears on pgwire) from the results of executing it. We do
this by enumerating more input events:

- S: statement available for execution
- SERR: statement has executed and has produced an error
- SOK: statement has executed without error

Then modify the structure of the FSM:

```
 digraph simplesql {
  start -> NoTxn;
  NoTxn -> Open [label=B];
  Open -> NoTxn [label=C];
  Open -> NoTxn [label=R];
  Open -> Running [label=S];
  Running -> Open [label=SOK];
  Running -> Aborted [label=SERR];
  Aborted -> NoTxn [label=R];
 }
```

We then describe "outside of the FSM" that in the specific state
"Running" the program also actually runs the statement carried by the
S event to determine the next event SOK or SERR.

Although this additional specification is expressed outside of the
FSM, we can formally guarantee that the FSM is well-formed given that
specification, by proving that there is no possible sequence of input
events that presents SOK or SERR to the FSM without an S event
immediately preceding. (proof left as an exercise to the reader)

## Retriable transactions

CockroachDB implements this fancy user-directed protocol for txn retries. This uses the following additional 3 events:

- BS - SAVEPOINT cockroach_restart
- RS - ROLLBACK TO SAVEPOINT cockroach_restart
- CS - RELEASE SAVEPOINT cockroach_restart
- SRETRY - statement has executed and has produced a retryable error

```
digraph retrysql {
  start -> NoTxn;
  NoTxn -> Open [label=B];
  Open -> NoTxn [label=C];
  Open -> NoTxn [label=R];
  Open -> Running [label=S];
  Running -> Open [label=SOK];
  Running -> Aborted [label=SERR];
  Running -> Aborted [label=SRETRY];
  Aborted -> NoTxn [label=R];
  Open -> OpenRetryable [label=BS];
  OpenRetryable -> NoTxn [label=R];
  OpenRetryable -> NoTxn [label=C];
  OpenRetryable -> OpenRetryable [label=RS];
  OpenRetryable -> RunningRetryable [label=S];
  RunningRetryable -> OpenRetryable [label=SOK];
  RunningRetryable -> Aborted [label=SERR];
  RunningRetryable -> RetryWait [label=SRETRY];
  RetryWait -> OpenRetryable [label=RS];
  RetryWait -> Aborted [label=S];
  RetryWait -> NoTxn [label=R];
  OpenRetryable -> CommitWait [label=CS];
  CommitWait -> NoTxn [label=C];
  CommitWait -> CommitWait [label=S];
  CommitWait -> NoTxn [label=R];
  Open -> CommitWait [label=CS,color=blue];
  Open -> Open [label=RS,color=blue];
  OpenRetryable -> OpenRetryable [label=BS,color=blue];
}
```

The FSM above is a reflection of the current code in CockroachDB. The
exercise of translating the code to a FSM reveals three unexpected edges
(colored in blue) which are not clearly documented nor tested:

- the ability to e.g. issue `RELEASE SAVEPOINT` in a newly opened
  transaction even without a `SAVEPOINT` before that, without error,
  bringing the transaction in state CommitWait.
- the ability to issue `SAVEPOINT` two or more times without error.
- the ability to issue `ROLLBACK TO SAVEPOINT` without a `SAVEPOINT`
  before that, without error.

(Whether these are bugs or features is left as an exercise to the
reader. The point I would like to make here however is that the FSM description
makes this kind of discovery easier.)

## Connecting FSMs together

The FSMs from the two sections above were only describing what happens
with the SQL transaction state. They do not describe what happens with
the underlying storage, for example the K/V `Txn` object. For example,
both the events C (COMMIT) and R (ROLLBACK) transition from state Open
to state NoTxn, and from CommitWait to NoTxn. However, the
storage-level meaning of R is not the same depending on whether the
start state is Open or CommitWait - in the former case the KV
transaction is rolled back as expected, in the latter case it *remains
committed* even though the ROLLBACK statement was issued.

To be a bit more explicit here we need to look at the KV Txn FSM and
connect that one to the SQL txn FSM.

### Looking at the KV Txn FSM

The KV Txn FSM uses the following input events:

- cN: `NewTxn`
- cI: `SetIsolation`,  `SetFixedTimestamp`, `.OrigTimestamp = xxx`, `SetDebugName`, etc
- cE: `Exec`
- cR: `Rollback` (send EndTxnReq with commit=false)
- cC: `Commit` (send EndTxnReq with commit=true)
- cF: clear and set to nil

(The "c" stands for "client". We need to distinguish here so that we
can merge the FSMs below.)

```
digraph kvtxn {
 start -> txnNil;
 txnNil -> PENDINGNEW [label=cN];
 PENDINGNEW -> PENDINGNEW [label=cI];
 PENDINGNEW -> PENDING [label=cE];
 PENDING -> PENDING [label=cE];
 PENDING -> ABORTED [label=cR];
 PENDING -> COMMITTED [label=cC];
 ABORTED -> txnNil [label=cF];
 COMMITTED -> txnNil [label=cF];
}
```

### Connecting actions in the SQL FSM to inputs in the KV FSM

This is similar to what we did with the lift control example above: we
annotate the transitions in the SQL txn FSM with actions that are
inputs to the KV FSM.

To start with the simple FSM first:

```
 digraph simplesql_kv {
  start -> NoTxn;
  NoTxn -> Open [label="B|cN,cI"];
  Open -> NoTxn [label="C|cC,cF"];
  Open -> NoTxn [label="R|cR,cF"];
  Open -> Running [label="S|cE"];
  Running -> Open [label=SOK];
  Running -> Aborted [label=SERR];
  Aborted -> NoTxn [label="R|cR,cF"];
 }
```

The overall structure of the SQL FSM is not affected.

It is possible to prove that no combination of inputs in the
`simplesql` FSM can produce as output an invalid input for the `kvtxn`
FSM. Although we are specifying them separately, the theory supports
us when we want to "join them together". There are even algorithms
that generate the source code that implement the combination of both.

That said, this simplistic example already reveals the opportunity for
an important optimization: not creating the KV txn object until the
first SQL statement has been received. This allows us to avoid calling
`Rollback()` or `Commit()` (and incurring the overhead of processing
an `EndTxnReq` in KV) for an empty KV txn.

We can achieve this as follows: *split* the Open state into two.

```
 digraph simplesqlopt {
  start -> NoTxn;
  NoTxn -> OpenFirst [label="B"];
  OpenFirst -> Running [label="S|cN,cI,cE"];
  OpenFirst -> NoTxn [label="C"];
  OpenFirst -> NoTxn [label="R"];
  Open -> NoTxn [label="C|cC,cF"];
  Open -> NoTxn [label="R|cR,cF"];
  Open -> Running [label="S|cE"];
  Running -> Open [label=SOK];
  Running -> Aborted [label=SERR];
  Aborted -> NoTxn [label="R|cR,cF"];
 }
```

One can formally prove that the two states Open and OpenFirst are
"equivalent modulo the output actions", i.e. for a given
input sequence of SQL events, this extended FSM produces the same
state transitions as the one where the Open and OpenFirst states are
merged and rejects the same inputs.

In general there are multiple ways to "split states" in any given FST
with confidence that the resulting FST is equivalent modulo the
actions of the new intermediate edge.

The extension of this connection to the larger SQL FSM that also
supports retries is left as an exercise to the reader.

### Reporting results and errors from KV

We have assumed at the beginning that the SQL KV txn observes input
events "SERR" and "SOK" as a result of executing a statement.
This distinction is actually an effect of performing the cE action
on the KV txn FSM.

It is tempting here, and actually conceptually sound, to make a little
step forward and "close the loop": make the KV txn FSM an FST, give it
output actions, and connect this back to the SQL FST.

For example:

```
digraph kvtxn {
 start -> txnNil;
 txnNil -> PENDINGNEW [label=cN];
 PENDINGNEW -> PENDINGNEW [label=cI];
 PENDINGNEW -> RUNNING [label=cE];
 RUNNING -> PENDING [label="|SERR"];
 RUNNING -> PENDING [label="|SOK"];
 PENDING -> ABORTED [label=cR];
 PENDING -> COMMITTED [label=cC];
 ABORTED -> txnNil [label=cF];
 COMMITTED -> txnNil [label=cF];
}
```

Notice now how the FSM has turned into an FST which can "respond" by
either SERR or SOK after a cE event.  Once we have this we can then
specify that the output of the KV FST is connected back to the input
of the SQL FST, in addition ot the forward connection already defined
above.

This "works", and we can even implement it, and even also have it
automatically implemented, but there is a ginormous caveat: when
FSTs are looped back onto each other like this, the state space
explodes and unless a lot more precautions are taken it can become
impossible to prove simple things about the event streams any more.

So we'll keep the possibility in mind, in particular the understanding
that the KV txn is in charge of deciding whether an executing
statement succeeds or not, but we'll not make the extra step of
specifying that the FSTs are connected in a loop; preferring instead
to "externally" specify that the SQL txn FST magically observes either
SERR or SOK on its input as a consequence of issuing cE to the other FST.

# More SQL interactions

## Spontanous transactions

The examples above do not allow one to run statements out of a
transaction. This can be simply added with additional transitions to
the NoTxn state:

```
digraph spontaneous {
 start -> NoTxn;
 NoTxn -> ImplicitRun [label="S|cN,cI,cE"];
 ImplicitRun -> NoTxn [label="SOK|cC,cF"];
 ImplicitRun -> NoTxn [label="SERR|cR,cF"];
 ImplicitRun -> NoTxn [label="SRETRY|cR,cF"];
 NoTxn -> Open [label=B];
 Open [label="Open ... (as before)"];
}
```

## Server-side implicit retries, take one

We consider implicit retries for spontaneous transactions outside of explicit BEGIN/COMMIT
blocks. For this we add just one more state:

```
digraph spontaneousretry {
 start -> NoTxn;
 NoTxn -> ImplicitRunnable [label="S|cN,cI"];
 ImplicitRunnable -> ImplicitRunning [label="|cE"];
 ImplicitRunning -> NoTxn [label="SOK|cC,cF"];
 ImplicitRunning -> NoTxn [label="SERR|cR,cF"];
 ImplicitRunning -> ImplicitRunnable [label="SRETRY"];
 NoTxn -> Open [label=B];
 Open [label="Open ... (as before)"];
}
```

# Talking back to the client

To later talk about server-side retries inside a BEGIN/COMMIT block,
we first need to distinguish a "transaction prefix". This is done
using the predicate "the statements received together as a batch with
the BEGIN statement for which we have not yet sent results". For this
we need to introduce the notion of a SQL batch!

Let's first look at batches without any actions, retries, etc. A batch
can be modeled as a regular sequence of SQL events enclosed with a
"start batch" event, denoted "<", and an "end batch" event, denoted
">".

Let's first consider the hypothetical case where entire transactions
(from BEGIN up to and including COMMIT/ROLLBACK) must be enclosed in a
single batch:

```
 digraph simplesql_client {
  start -> NoTxn;
  NoTxn -> NoTxnB [label="<"];
  NoTxnB -> OpenB [label=B];
  OpenB -> NoTxnB [label=C];
  OpenB -> NoTxnB [label=R];
  OpenB -> RunningB [label=S];
  RunningB -> OpenB [label=SOK];
  RunningB -> AbortedB [label=SERR];
  AbortedB -> NoTxnB [label=R];
  NoTxnB -> NoTxn [label=">"];

  subgraph clusterInBatch {
    label="In batch";
    NoTxnB; OpenB; RunningB; AbortedB;
  }
}
```

We can then split the *transitions*  to allow receiving the transaction bits in separate batches:

```
 digraph simplesql_client {
  start -> NoTxn;
  NoTxn -> NoTxnB [label="<"];
  NoTxnB -> OpenB [label=B];
  OpenB -> Open [label=">"];
  Open -> OpenB [label="<"];
  OpenB -> NoTxnB [label=C];
  OpenB -> NoTxnB [label=R];
  OpenB -> RunningB [label=S];
  RunningB -> OpenB [label=SOK];
  RunningB -> AbortedB [label=SERR];
  AbortedB -> Aborted [label=">"];
  Aborted -> AbortedB [label="<"];
  AbortedB -> NoTxnB [label=R];
  NoTxnB -> NoTxn [label=">"];

  subgraph clusterInBatch {
    label="In batch";
    NoTxnB; OpenB; RunningB; AbortedB;
  }
}
```

With batches, we must also talk about when the SQL txn "sends results
back" to the client. For this we introduce the action V for "send
the result list" back to the client, and use it to annotate the last FSM to become a FST:

```
 digraph simplesql_client {
  start -> NoTxn;
  NoTxn -> NoTxnB [label="<"];
  NoTxnB -> OpenB [label=B];
  OpenB -> Open [label=">|V"];
  Open -> OpenB [label="<"];
  OpenB -> NoTxnB [label=C];
  OpenB -> NoTxnB [label=R];
  OpenB -> RunningB [label=S];
  RunningB -> OpenB [label=SOK];
  RunningB -> AbortedB [label=SERR];
  AbortedB -> Aborted [label=">|V"];
  Aborted -> AbortedB [label="<"];
  AbortedB -> NoTxnB [label=R];
  NoTxnB -> NoTxn [label=">|V"];
  
  subgraph clusterInBatch {
    label="In batch";
    NoTxnB; OpenB; RunningB; AbortedB;
  }
}
```

## Server-side implicit retries, take two

We consider here the implicit retries that take place on *batch
prefixes*. For this we split the Open and Running states into "in the
first batch" and "in the batches thereafter". In the running state
"for the first batch" we can automatically retry execution when SRETRY
is encountered for a statement (blue transition in diagram):

```
 digraph simplesql_client {
  start -> NoTxn;
  NoTxn -> NoTxnB1 [label="<"];
  NoTxnB1 -> OpenB1 [label=B];
  OpenB1 -> Open [label=">|V"];
  Open -> OpenBN [label="<"];
  OpenB1 -> NoTxnB [label=C];
  OpenB1 -> NoTxnB [label=R];
  OpenB1 -> RunningB1 [label=S];
  RunningB1 -> OpenB1 [label=SOK];
  RunningB1 -> RunningB1 [label=SRETRY,color=blue];
  RunningB1 -> AbortedB [label=SERR];
  AbortedB -> Aborted [label=">|V"];
  Aborted -> AbortedB [label="<"];
  AbortedB -> NoTxnB [label=R];
  NoTxnB -> NoTxn [label=">|V"];
  OpenBN -> Open [label=">"];
  OpenBN -> NoTxnB [label=C];
  OpenBN -> NoTxnB [label=R];
  OpenBN -> RunningBN [label=S];
  RunningBN -> OpenBN [label=SOK];
  RunningBN -> AbortedB [label=SERR];
  RunningBN -> AbortedB [label=SRETRY];
 }
```

We can then mechanically zip the actions from the `simplesqlopt` FST
above which talks to the KV txn FSM, into this new one. The states
named "OpenFirst" are about the KV txn optimization. The states named
"B1" vs "BN" are about the automatic retry.

```
 digraph simplesqlopt_client {
  start -> NoTxn;
  NoTxn -> NoTxnB1 [label="<"];
  NoTxnB1 -> OpenFirstB1 [label=B];
  OpenFirstB1 -> OpenFirst [label=">|V"];
  OpenFirst -> OpenFirstBN [label="<"];
  OpenFirstB1 -> NoTxnB [label=C];
  OpenFirstB1 -> NoTxnB [label=R];
  OpenFirstB1 -> RunningB1 [label="S|cN,cI,cE"];
  OpenB1 -> NoTxnB [label="C|cC,cF"];
  OpenB1 -> NoTxnB [label="R|cR,cF"];
  OpenB1 -> RunningB1 [label="S|cE"];
  RunningB1 -> OpenB1 [label=SOK];
  RunningB1 -> RunningB1 [label="SRETRY|cR,cI,cE"];
  RunningB1 -> AbortedB [label=SERR];
  AbortedB -> Aborted [label=">|V"];
  Aborted -> AbortedB [label="<"];
  AbortedB -> NoTxnB [label="R|cR,cF"];
  NoTxnB -> NoTxn [label=">|V"];
  OpenFirstBN -> OpenFirst [label=">|V"];
  OpenFirstBN -> NoTxnB [label=C];
  OpenFirstBN -> NoTxnB [label=R];
  OpenFirstBN -> RunningBN [label="S|cN,cI,cE"];
  OpenBN -> Open [label=">"];
  Open -> OpenBN [label="<|V"];
  OpenBN -> NoTxnB [label="C|cC,cF"];
  OpenBN -> NoTxnB [label="R|cR,cF"];
  OpenBN -> RunningBN [label="S|cE"];
  RunningBN -> OpenBN [label=SOK];
  RunningBN -> AbortedB [label=SERR];
  RunningBN -> AbortedB [label=SRETRY];
 }
```

This looks complex and daunting if one imagines oneself trying to
build this from scratch, but knowing this is actually the mechanical
combination of the two more simple FSTs above creates confidence the
result does the right thing.

**Disclaimer: here we are entering the realm of how things should be,
as opposed to how things are. I have not checked here that the current
code in CockroachDB actually obeys this FST. There may be bugs lurking.**

## Combine with client-directed retries

(Left as an exercise to the reader. It's still rather mechanical!)

# Next steps from here

The increamental construction of the FSMs/FSTs amount to small
reasoning steps that provide confidence that the layers of
functionality do what they intend to do.

However the resulting FSMs/FSTs are rather large. How can we check whether the model is correct? Or that the code does what the model say?

There are two approaches possible:

- we can use the FSM specification to generate tests automatically
  that exercise randomly generated valid sequences of events, and
  check that the executor logic arrives in the expected state for all
  of them.

  I'm not exactly knowledgeable about the algorithms that enable this,
  but I know they exist.

- we can generate the program code from the FSM specification (either
  automatically, or manually by obeying some very strict mechanical
  transformation rules from FSM to code). Then it's guaranteed correct
  by construction. :)

## Reflecting on the current code - txn closure and `Exec`

Currently the code is architectured so that the `Executor` passes a
closure of the SQL code to execute to a method `(*Txn).Exec`, in
charge of automatic server-side retries.

One should not let this obscure what is going on, this is just a
convenience architecture to concentrate the sub-part of the state
machine responsible for retries in a single place.

This architecture would not be necessary would one program from the
start the logic using a flat finite state machine, or a mechanically
constructed assembly of multiple FSTs.

## Transforming a FSM to code

There are multiple ways to do this, depending on whether we want a
"push" model (an API call to feed input events into the FSM) or a
"push" model (the FSM is a single routine that performs API calls to
elsewhere to receive its input events).

The combination of multiple FSTs together also has different
implementations, depending on whether the combination is push/push or
pull/pull (then the solution is coroutines) or push/pull or pull/push
(then the solution is co-simulations).

Given the current state of the code where the kv layer takes API
calls to commit/rollback transactions, the simplest and most elegant solution
would be a push/push combination.

This would in turn require "opening up" the pgwire logic into a
receiving part and a sending part. The receiving part would push
received events into the SQL txn FST, and the SQL txn FST would push
results/responses into the sending part of pgwire.

This sounds a bit invasive at first, but consider that once this is
done then result streaming from the executor back to the client "falls
into place" naturally.

# Alternatives

TLA/TLA+? Overkill, methinks.

# Unresolved questions

TBD
