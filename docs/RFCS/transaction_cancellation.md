- Feature Name: Transaction Cancellation
- Status: completed (PR [#17252](https://github.com/cockroachdb/cockroach/pull/17252))
- Start Date: 2017-07-30
- Author: Julian Gilyadov
- RFC PR: [#17252](https://github.com/cockroachdb/cockroach/pull/17252)
- Cockroach Issue: None

# Motivation

It is similar to the motivation behind query cancellation, currently it is possible to monitor sessions and queries, and cancelling queries.  
Extending this functionality with transaction cancellation would be a logical next step.  
It would be useful to cancel idle transactions for example.  
After those features are implemented, they can be used to implement session cancellation, which is a logical next step and would be useful for revoking unauthorized access to the database.

# Design

#### Syntax

A new `CANCEL TRANSACTION <id>` statement will be added to SQL, where `id` is a string.

#### Cancelling the transaction

The txn ID will be parsed from the SQL statement, all sessions will be linearly searched to find a session with a matching txn(local sessions will be searched first to potentially avoid dialing to remote nodes), if a matching txn wasn't found then an appropriate error will be returned.
When a matching session is found, an RPC call will be made to that node if it is not the current node, that node would look up the session and cancel the transaction by cancelling all running queries under that transaction and a flag will be set in the session object to indicate that the transaction is cancelled.

#### Aborting the transaction

A new boolean flag will be added into the session struct to indicate whether the transaction should transition to an aborted state.

The boolean flag in the session object will be set during transaction cancellation. This flag will be checked at key points by the executor who will be responsible for transitioning the txn into an aborted state if the flag is set. 

#### Tests

Tests for transaction cancellation should be written as logic and cluster tests in the `run_control` test suite.   
Fundamentally, it should be similar to the query cancellation tests.  
Multiple types of queries need to be tested:
*   When the txn is currently running a distributed query. (I understand this may not succeed currently because cancellation of distributed queries is not complete yet, but then your test should expect and validate that the cancellation fails.)
*   With a query running `crdb_internal.force_retry('10m')` so that it is busy with a retry loop.
*   With a query running `show trace for ...` with a long-running statement.

# Alternatives

To be updated.

# Drawbacks

Additional SQL statements.

# Unresolved Questions

To be updated.
