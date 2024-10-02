// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

// Package sqlfsm contains the definitions for the state labels of the
// conn executor FSM. These are in a separate package to ensure the
// SQL shell only uses a minimal dependency graph.
package sqlfsm

// Constants for the String() representation of the session states. Shared with
// the CLI code which needs to recognize them.
const (
	NoTxnStateStr         = "NoTxn"
	OpenStateStr          = "Open"
	AbortedStateStr       = "Aborted"
	CommitWaitStateStr    = "CommitWait"
	InternalErrorStateStr = "InternalError"
)
