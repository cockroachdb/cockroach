// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

// A ConnExecutor is in charge of executing queries received on a given client
// connection. The ConnExecutor implements a state machine (mostly defined by
// the Postgres/pgwire session semantics). The state machine is supposed to run
// asynchronously wrt the client connection: it receives input statements
// through a stmtBuf and produces results through a clientComm interface. The
// ConnExecutor maintains a cursor over the statementBuffer and executes
// statements / produces results for one statement at a time. The cursor points
// at all times to the statement that the ConnExecutor is currently executing.
// Results for statements before the cursor have already been produced (but not
// necessarily delivered to the client). Statements after the cursor are queued
// for future execution. Keeping already executed statements in the buffer is
// useful in case of automatic retries (in which case statements from the
// retried transaction have to be executed again); the ConnExecutor is in charge
// of removing old statements that are no longer needed for retries from the
// (head of the) buffer. Separately, the implementer of the clientComm interface
// (e.g. the pgwire module) is in charge of keeping track of what results have
// been delivered to the client and what results haven't (yet).
//
// The ConnExecutor has two main responsibilities: to dispatch queries to the
// execution engine(s) and relay their results to the clientComm, and to
// implement the state machine maintaining the various aspects of a connection's
// state. The state machine implementation is further divided into two aspects:
// maintaining the transaction status of the connection (outside of a txn,
// inside a txn, in an aborted txn, in a txn awaiting client restart, etc.) and
// maintaining the cursor position (i.e. correctly jumping to whatever the
// "next" statement to execute is in various situations).
//
// The cursor normally advances one statement at a time, but it can also skip
// some statements (remaining statements in a query string are skipped once an
// error is encountered) and it can sometimes be rewound when performing
// automatic retries. Rewinding can only be done if results for the rewound
// statements have not actually been delivered to the client; see below.
//
//                                                  +-----------------+
//     +--------------------+                       |ConnExecutor     |
//     |stmtBuf             |                       |                 |
//     |                    | statements are read   |                 |
//     | +-+-+ +-+-+ +-+-+  +-----------------------+  +------------+ |
//     | | | | | | | | | |  |                       |  |txnState    | |
// +---> +-+-+ +++-+ +-+-+  |                       |  +------------+ |
// |   |        ^           |                       |  +------------+ |
// |   |        |   +-------------------------------+  |session data| |
// |   |        +   v       | cursor is advanced    |  +------------+ |
// |   |       cursor       |           +           |                 |
// |   +--------------------+           |           +------------+----+
// |                                    |                        |
// |                                    |                        |
// +-------------+                      |                        |
//               +--------+             |                        |
//               ||parser |             |results are produced    |
//               +--------+             |                        |
//               |                      |                        |
//               |                      |                        v
//               |                      |            +-----------+----+
//       +-------+------+               |            |execution engine|
//       | pgwire conn  |               |            |(local/DistSQL) |
//       |              |               |            +----------------+
//       |   +----------+               |
//       |   |clientComm<---------------+
//       |   +----------+
//       |              |
//       +-------^------+
//               |
//               |
//       +-------+------+
//       | SQL client   |
//       +--------------+
//
// The ConnExecutor is disconnected from client communication (i.e. generally
// network communication - pgwire); the module doing client communication is
// responsible for pushing statements into the buffer and for providing an
// implementation of the clientConn interface (and thus consume results and send
// them to the client). The ConnExecutor does not control when results are
// delivered to the client, but still it does have some influence over that;
// this is because of the fact that the possibility of doing automatic retries
// is broken the moment results for the transaction in question are delivered to
// the client. The communication module has full freedom in sending results
// whenever it sees fit; however the ConnExecutor influences communication in
// the following ways:
//
// a) The ConnExecutor calls clientComm.flush(), informing the implementer that
// all the previous results can be delivered to the client at will.
//
// b) When deciding whether an automatic retry can be performed for a
// transaction, the ConnExecutor needs to:
//
//   1) query the communication status to check that no results for the txn have
//   been delivered to the client and, if this check passes:
//   2) lock the communication so that no further results are delivered to the
//   client, and, eventually:
//   3) rewind the clientComm to a certain position corresponding to the start
//   of the transaction, thereby discarding all the results that had been
//   accumulated for the previous attempt to run the transaction in question.
//
// These steps are all orchestrated through clientComm.lockCommunication() and
// rewindCapability{}.
