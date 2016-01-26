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

package parser

import (
	"bytes"
	"fmt"
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
	UnspecifiedIsolation: "UNSPECIFIED",
	Low:                  "LOW",
	Normal:               "NORMAL",
	High:                 "HIGH",
}

func (up UserPriority) String() string {
	if up < 0 || up > UserPriority(len(userPriorityNames)-1) {
		return fmt.Sprintf("UserPriority(%d)", up)
	}
	return userPriorityNames[up]
}

// BeginTransaction represents a BEGIN statement
type BeginTransaction struct {
	Isolation    IsolationLevel
	UserPriority UserPriority
}

func (node *BeginTransaction) String() string {
	var buf bytes.Buffer
	var sep string
	buf.WriteString("BEGIN TRANSACTION")
	if node.Isolation != UnspecifiedIsolation {
		fmt.Fprintf(&buf, " ISOLATION LEVEL %s", node.Isolation)
		sep = ","
	}
	if node.UserPriority != UnspecifiedUserPriority {
		fmt.Fprintf(&buf, "%s PRIORITY %s", sep, node.UserPriority)
	}
	return buf.String()
}

// CommitTransaction represents a COMMIT statement.
type CommitTransaction struct{}

func (node *CommitTransaction) String() string {
	return "COMMIT TRANSACTION"
}

// RollbackTransaction represents a ROLLBACK statement.
type RollbackTransaction struct{}

func (node *RollbackTransaction) String() string {
	return "ROLLBACK TRANSACTION"
}
