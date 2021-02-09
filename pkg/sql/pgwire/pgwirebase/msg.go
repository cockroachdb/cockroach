// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwirebase

import "math"

//ClientMessageType represents a client pgwire message.
//go:generate stringer -type=ClientMessageType
type ClientMessageType byte

//ServerMessageType represents a server pgwire message.
//go:generate stringer -type=ServerMessageType
type ServerMessageType byte

// http://www.postgresql.org/docs/9.4/static/protocol-message-formats.html
const (
	ClientMsgBind        ClientMessageType = 'B'
	ClientMsgClose       ClientMessageType = 'C'
	ClientMsgCopyData    ClientMessageType = 'd'
	ClientMsgCopyDone    ClientMessageType = 'c'
	ClientMsgCopyFail    ClientMessageType = 'f'
	ClientMsgDescribe    ClientMessageType = 'D'
	ClientMsgExecute     ClientMessageType = 'E'
	ClientMsgFlush       ClientMessageType = 'H'
	ClientMsgParse       ClientMessageType = 'P'
	ClientMsgPassword    ClientMessageType = 'p'
	ClientMsgSimpleQuery ClientMessageType = 'Q'
	ClientMsgSync        ClientMessageType = 'S'
	ClientMsgTerminate   ClientMessageType = 'X'

	ServerMsgAuth                 ServerMessageType = 'R'
	ServerMsgBackendKeyData       ServerMessageType = 'K'
	ServerMsgBindComplete         ServerMessageType = '2'
	ServerMsgCommandComplete      ServerMessageType = 'C'
	ServerMsgCloseComplete        ServerMessageType = '3'
	ServerMsgCopyInResponse       ServerMessageType = 'G'
	ServerMsgDataRow              ServerMessageType = 'D'
	ServerMsgEmptyQuery           ServerMessageType = 'I'
	ServerMsgErrorResponse        ServerMessageType = 'E'
	ServerMsgNoticeResponse       ServerMessageType = 'N'
	ServerMsgNoData               ServerMessageType = 'n'
	ServerMsgParameterDescription ServerMessageType = 't'
	ServerMsgParameterStatus      ServerMessageType = 'S'
	ServerMsgParseComplete        ServerMessageType = '1'
	ServerMsgPortalSuspended      ServerMessageType = 's'
	ServerMsgReady                ServerMessageType = 'Z'
	ServerMsgRowDescription       ServerMessageType = 'T'
)

// ServerErrFieldType represents the error fields.
//go:generate stringer -type=ServerErrFieldType
type ServerErrFieldType byte

// http://www.postgresql.org/docs/current/static/protocol-error-fields.html
const (
	ServerErrFieldSeverity       ServerErrFieldType = 'S'
	ServerErrFieldSQLState       ServerErrFieldType = 'C'
	ServerErrFieldMsgPrimary     ServerErrFieldType = 'M'
	ServerErrFieldDetail         ServerErrFieldType = 'D'
	ServerErrFieldHint           ServerErrFieldType = 'H'
	ServerErrFieldSrcFile        ServerErrFieldType = 'F'
	ServerErrFieldSrcLine        ServerErrFieldType = 'L'
	ServerErrFieldSrcFunction    ServerErrFieldType = 'R'
	ServerErrFieldConstraintName ServerErrFieldType = 'n'
)

// PrepareType represents a subtype for prepare messages.
//go:generate stringer -type=PrepareType
type PrepareType byte

const (
	// PrepareStatement represents a prepared statement.
	PrepareStatement PrepareType = 'S'
	// PreparePortal represents a portal.
	PreparePortal PrepareType = 'P'
)

// MaxPreparedStatementArgs is the maximum number of arguments a prepared
// statement can have when prepared via the Postgres wire protocol. This is not
// documented by Postgres, but is a consequence of the fact that a 16-bit
// integer in the wire format is used to indicate the number of values to bind
// during prepared statement execution.
const MaxPreparedStatementArgs = math.MaxUint16
