// Copyright 2015 The Cockroach Authors.
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
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)
// Author: Andrei Matei (andreimatei1@gmail.com)

package parser

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

// IsolationLevel holds the isolation level for a transaction.
type IsolationLevel int

// IsolationLevel values
const (
	UnspecifiedIsolation IsolationLevel = iota
	SnapshotIsolation
	SerializableIsolation
)

var isolationLevelNames = [...]string{
	UnspecifiedIsolation:  "UNSPECIFIED",
	SnapshotIsolation:     "SNAPSHOT",
	SerializableIsolation: "SERIALIZABLE",
}

func (i IsolationLevel) String() string {
	if i < 0 || i > IsolationLevel(len(isolationLevelNames)-1) {
		return fmt.Sprintf("IsolationLevel(%d)", i)
	}
	return isolationLevelNames[i]
}

// UserPriority holds the user priority for a transaction.
type UserPriority int

// UserPriority values
const (
	UnspecifiedUserPriority UserPriority = iota
	Low
	Normal
	High
)

var userPriorityNames = [...]string{
	UnspecifiedUserPriority: "UNSPECIFIED",
	Low:    "LOW",
	Normal: "NORMAL",
	High:   "HIGH",
}

func (up UserPriority) String() string {
	if up < 0 || up > UserPriority(len(userPriorityNames)-1) {
		return fmt.Sprintf("UserPriority(%d)", up)
	}
	return userPriorityNames[up]
}

// ReadWriteMode holds the read write mode for a transaction.
type ReadWriteMode int

// ReadWriteMode values
const (
	UnspecifiedReadWriteMode ReadWriteMode = iota
	ReadOnly
	ReadWrite
)

var readWriteModeNames = [...]string{
	UnspecifiedReadWriteMode: "UNSPECIFIED",
	ReadOnly:                 "ONLY",
	ReadWrite:                "WRITE",
}

func (ro ReadWriteMode) String() string {
	if ro < 0 || ro > ReadWriteMode(len(readWriteModeNames)-1) {
		return fmt.Sprintf("ReadWriteMode(%d)", ro)
	}
	return readWriteModeNames[ro]
}

// TransactionModes holds the transaction modes for a transaction.
type TransactionModes struct {
	Isolation     IsolationLevel
	UserPriority  UserPriority
	ReadWriteMode ReadWriteMode
}

// Format implements the NodeFormatter interface.
func (node *TransactionModes) Format(buf *bytes.Buffer, f FmtFlags) {
	var sep string
	if node.Isolation != UnspecifiedIsolation {
		fmt.Fprintf(buf, " ISOLATION LEVEL %s", node.Isolation)
		sep = ","
	}
	if node.UserPriority != UnspecifiedUserPriority {
		fmt.Fprintf(buf, "%s PRIORITY %s", sep, node.UserPriority)
		sep = ","
	}
	if node.ReadWriteMode != UnspecifiedReadWriteMode {
		fmt.Fprintf(buf, "%s READ %s", sep, node.ReadWriteMode)
	}
}

func (node *TransactionModes) merge(other TransactionModes) error {
	if other.Isolation != UnspecifiedIsolation {
		if node.Isolation != UnspecifiedIsolation {
			return errors.New("isolation level specified multiple times")
		}
		node.Isolation = other.Isolation
	}
	if other.UserPriority != UnspecifiedUserPriority {
		if node.UserPriority != UnspecifiedUserPriority {
			return errors.New("user priority specified multiple times")
		}
		node.UserPriority = other.UserPriority
	}
	if other.ReadWriteMode != UnspecifiedReadWriteMode {
		if node.ReadWriteMode != UnspecifiedReadWriteMode {
			return errors.New("read mode specified multiple times")
		}
		node.ReadWriteMode = other.ReadWriteMode
	}
	return nil
}

// BeginTransaction represents a BEGIN statement
type BeginTransaction struct {
	Modes TransactionModes
}

// Format implements the NodeFormatter interface.
func (node *BeginTransaction) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("BEGIN TRANSACTION")
	node.Modes.Format(buf, f)
}

// CommitTransaction represents a COMMIT statement.
type CommitTransaction struct{}

// Format implements the NodeFormatter interface.
func (node *CommitTransaction) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("COMMIT TRANSACTION")
}

// RollbackTransaction represents a ROLLBACK statement.
type RollbackTransaction struct{}

// Format implements the NodeFormatter interface.
func (node *RollbackTransaction) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ROLLBACK TRANSACTION")
}

// RestartSavepointName is the only savepoint name that we accept, modulo
// capitalization.
const RestartSavepointName string = "COCKROACH_RESTART"

// ValidateRestartCheckpoint checks that a checkpoint name is our magic restart
// value.
// We accept everything with the desired prefix because at least the C++ libpqxx
// appends sequence numbers to the savepoint name specified by the user.
func ValidateRestartCheckpoint(savepoint string) error {
	if !strings.HasPrefix(strings.ToUpper(savepoint), RestartSavepointName) {
		return fmt.Errorf("SAVEPOINT not supported except for %s", RestartSavepointName)
	}
	return nil
}

// Savepoint represents a SAVEPOINT <name> statement.
type Savepoint struct {
	Name string
}

// Format implements the NodeFormatter interface.
func (node *Savepoint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SAVEPOINT ")
	buf.WriteString(node.Name)
}

// ReleaseSavepoint represents a RELEASE SAVEPOINT <name> statement.
type ReleaseSavepoint struct {
	Savepoint string
}

// Format implements the NodeFormatter interface.
func (node *ReleaseSavepoint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("RELEASE SAVEPOINT ")
	buf.WriteString(node.Savepoint)
}

// RollbackToSavepoint represents a ROLLBACK TO SAVEPOINT <name> statement.
type RollbackToSavepoint struct {
	Savepoint string
}

// Format implements the NodeFormatter interface.
func (node *RollbackToSavepoint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("ROLLBACK TRANSACTION TO SAVEPOINT ")
	buf.WriteString(node.Savepoint)
}
