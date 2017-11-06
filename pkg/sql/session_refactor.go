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

// A SQL Session is in charge of executing queries received on a given client
// connection. The Session implements a state machine (mostly defined by the
// Postgres/pgwire session semantics). The state machine is supposed to run
// asynchronously wrt the client connection: it receives input statements
// through a statementBuffer and produces results through a clientComm
// interface. The session maintains a cursor over the statementBuffer and
// executes statements / produces results for one statement at a time.
//
// The Session has two main responsibilities: to dispatch queries to the
// execution engine(s) and relay their results to the clientComm and to
// implement the state machine maintaining the various aspects of a session's
// state. The state machine implementation is further divided into two aspects:
// maintaining the transaction status of the session (outside of a txn, inside a
// txn, in an aborted txn, in a txn awaiting client restart, etc.) and
// maintaining the cursor position indicating the next statement for the buffer
// to be executed.
//
// The cursor normally advances one statement at a time, but it can also skip
// some statements and it can sometimes be rewound when performing automatic
// retries. Rewinding can only be done if results for the rewound statements
// have not actually been sent to the client; see below.
//
//     +--------------------+
//     |stmtBuf             |                       +-----------------+
//     |                    |                       |Session          |
//     |                    |                       |                 |
//     | batches of stmts   |   statements are read |                 |
//     | +-+-+ +-+-+ +-+-+  +----------------------->     +--------+  |
//     | | | | | | | | | |  |                       |     |txnState|  |
// +---> +-+-+ +++-+ +-+-+  |                       |     +--------+  |
// |   |        ^           |                       |                 |
// |   |        |   +-------------------------------+                 |
// |   |        +   v       | cursor is advanced    |                 |
// |   |       cursor       |           +           |                 |
// |   +--------------------+           |           +------------+----+
// |                                    |                        |
// |                                    |                        |
// +-------------+                      |                        |
//               +-------+              |                        |
//               | parser|              |results are produced    |
//               +-------+              |                        |
//               |                      |            +-----------v------+
//               |                      |            |execution engine  |
//               |                      |            |(local and DistSQL|
//       +-------+------+               |            +------------------+
//       | pgwire conn  |               |
//       |              |               |
//       |   +----------+               |
//       |   |clientComm<---------------+
//       |   +----------+
//       |              |
//       |              |
//       |              |
//       +--------------+
//
//
// The Session is disconnected from client communication (i.e. generally network
// communication - pgwire); the module doing client communication is responsible
// for pushing statements into the buffer and for providing an implementation of
// the clientConn interface (and thus consume results and send them to the
// client). The Session does not control when results are sent to the client,
// but still it does have something to do with that; this is because of the fact
// that the desire to do automatic retries is broken the moment results for the
// transaction in question are sent to the client. The communication module has
// full freedom in sending results whenever it sees fit; however the Session
// influences communication in the following ways:
// a) The Session calls clientComm.flushAll(), informing the implementer that
// all the previous results can be sent at will.
// b) When deciding whether an automatic retry can be performed for a
// transaction, the Session needs to 1) query the communication status to check
// that no results for the txn have been delivered to the client and, if this
// check passes, 2) lock the communication so that no further results are
// delivered to the client and, eventually, 3) rewind the clientComm to a
// certain position corresponding to the start of the transaction, thereby
// discarding all the results that had been accumulated for the previous attempt
// to run the transaction in question. These steps are all orchestrated through
// clientComm.lockCommunication() and rewindCapability{}.
