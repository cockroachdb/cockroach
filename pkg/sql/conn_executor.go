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

// WIP(andrei)
// func execPrepare(stmt PrepareStmt) {
//
// 	// The unnamed prepared statement can be freely overwritten.
// 	if name != "" {
//  	 if c.session.PreparedStatements.Exists(name) {
//    	 return c.sendError(pgerror.NewErrorf(pgerror.CodeDuplicatePreparedStatementError, "prepared statement %q already exists", name))
//   	}
// 	}
//
//   // Convert the inferred SQL types back to an array of pgwire Oids.
//   inTypes := make([]oid.Oid, 0, len(stmt.TypeHints))
//   if len(stmt.TypeHints) > maxPreparedStatementArgs {
//     return c.stmtBuf.Push(
//       SendError{
//         Err: pgerror.NewErrorf(
//           pgerror.CodeProtocolViolationError,
//           "more than %d arguments to prepared statement: %d",
//           maxPreparedStatementArgs, len(stmt.TypeHints)),
//       })
//   }
//   for k, t := range stmt.TypeHints {
//     i, err := strconv.Atoi(k)
//     if err != nil || i < 1 {
//       return c.sendError(pgerror.NewErrorf(pgerror.CodeUndefinedParameterError, "invalid placeholder name: $%s", k))
//     }
//     // Placeholder names are 1-indexed; the arrays in the protocol are
//     // 0-indexed.
//     i--
//     // Grow inTypes to be at least as large as i. Prepopulate all
//     // slots with the hints provided, if any.
//     for j := len(inTypes); j <= i; j++ {
//       inTypes = append(inTypes, 0)
//       if j < len(inTypeHints) {
//         inTypes[j] = inTypeHints[j]
//       }
//     }
//     // OID to Datum is not a 1-1 mapping (for example, int4 and int8
//     // both map to TypeInt), so we need to maintain the types sent by
//     // the client.
//     if inTypes[i] != 0 {
//       continue
//     }
//     inTypes[i] = t.Oid()
//   }
//   for i, t := range inTypes {
//     if t == 0 {
//       return c.sendError(pgerror.NewErrorf(pgerror.CodeIndeterminateDatatypeError, "could not determine data type of placeholder $%d", i+1))
//     }
//   }
//   // Attach pgwire-specific metadata to the PreparedStatement.
//   stmt.ProtocolMeta = preparedStatementMeta{inTypes: inTypes}
//   c.writeBuf.initMsg(pgwirebase.ServerMsgParseComplete)
//   return c.writeBuf.finishMsg(c.wr)
// }
//
// func execDescribe() {
// switch typ {
// case pgwirebase.PrepareStatement:
//   stmt, ok := c.session.PreparedStatements.Get(name)
//   if !ok {
//     return c.stmtBuf.Push(
//       SendError{
//         Err: pgerror.NewErrorf(
//           pgerror.CodeInvalidSQLStatementNameError,
//           "unknown prepared statement %q", name),
//       })
//   }
//
//   stmtMeta := stmt.ProtocolMeta.(preparedStatementMeta)
//   c.writeBuf.initMsg(pgwirebase.ServerMsgParameterDescription)
//   c.writeBuf.putInt16(int16(len(stmtMeta.inTypes)))
//   for _, t := range stmtMeta.inTypes {
//     c.writeBuf.putInt32(int32(t))
//   }
//   if err := c.writeBuf.finishMsg(c.wr); err != nil {
//     return err
//   }
//
//   if stmtHasNoData(stmt.Statement) {
//     return c.sendNoData(c.wr)
//   }
//   return c.sendRowDescription(ctx, stmt.Columns, nil, c.wr)
// case pgwirebase.PreparePortal:
//   portal, ok := c.session.PreparedPortals.Get(name)
//   if !ok {
//     return c.sendError(pgerror.NewErrorf(pgerror.CodeInvalidCursorNameError, "unknown portal %q", name))
//   }
//
//   portalMeta := portal.ProtocolMeta.(preparedPortalMeta)
//
//   if stmtHasNoData(portal.Stmt.Statement) {
//     return c.sendNoData(c.wr)
//   }
//   return c.sendRowDescription(ctx, portal.Stmt.Columns, portalMeta.outFormats, c.wr)
// default:
//   return errors.Errorf("unknown describe type: %s", typ)
// }
//
// }
//
// func execDescribe() {
//   switch typ {
//   case pgwirebase.PrepareStatement:
//     c.session.PreparedStatements.Delete(ctx, name)
//   case pgwirebase.PreparePortal:
//     c.session.PreparedPortals.Delete(ctx, name)
//   default:
//     return errors.Errorf("unknown close type: %s", typ)
//   }
//   c.writeBuf.initMsg(pgwirebase.ServerMsgCloseComplete)
//   return c.writeBuf.finishMsg(c.wr)
// }
//
// func execBind() {
// // The unnamed portal can be freely overwritten.
// if portalName != "" {
//   if c.session.PreparedPortals.Exists(portalName) {
//     return c.sendError(pgerror.NewErrorf(pgerror.CodeDuplicateCursorError, "portal %q already exists", portalName))
//   }
// }
// stmt, ok := c.session.PreparedStatements.Get(statementName)
// if !ok {
//   return c.sendError(pgerror.NewErrorf(pgerror.CodeInvalidSQLStatementNameError, "unknown prepared statement %q", statementName))
// }
//
//
// if numValues != numQArgs {
//   return c.stmtBuf.Push(
//     sql.SendError{
//       Err: pgwirebase.NewProtocolViolationErrorf(
//         "expected %d arguments, got %d", numQArgs, numValues),
//     })
// }
//
// return c.sendError(pgerror.NewErrorf(pgerror.CodeProtocolViolationError, "wrong number of format codes specified: %d for %d arguments", numQArgFormatCodes, numQArgs))
// return c.sendError(pgerror.NewErrorf(pgerror.CodeProtocolViolationError, "expected 0, 1, or %d for number of format codes, got %d", numColumns, numColumnFormatCodes))
//
// // Create the new PreparedPortal in the connection's Session.
// portal, err := c.session.PreparedPortals.New(ctx, portalName, stmt, qargs)
// if err != nil {
//   return err
// }
//
// if log.V(2) {
//   log.Infof(ctx, "portal: %q for %q, args %q, formats %q", portalName, stmt.Statement, qargs, columnFormatCodes)
// }
//
// c.writeBuf.initMsg(pgwirebase.ServerMsgBindComplete)
// return c.writeBuf.finishMsg(c.wr)
// }
//
// func execExecPortal() {
// portal, ok := c.session.PreparedPortals.Get(portalName)
// if !ok {
//   return c.sendError(pgerror.NewErrorf(pgerror.CodeInvalidCursorNameError, "unknown portal %q", portalName))
// }
// stmt := portal.Stmt
// portalMeta := portal.ProtocolMeta.(preparedPortalMeta)
// pinfo := &tree.PlaceholderInfo{
//   TypeHints: stmt.TypeHints,
//   Types:     stmt.Types,
//   Values:    portal.Qargs,
// }
//
// tracing.AnnotateTrace()
// c.streamingState.reset(portalMeta.outFormats, false [> sendDescription */, int(limit))
// c.session.ResultsWriter = c
// err = c.executor.ExecutePreparedStatement(c.session, stmt, pinfo)
// if err != nil {
//   if err := c.setError(err); err != nil {
//     return err
//   }
// }
// return c.done()
// }
