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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"bytes"
	"fmt"
)

// Grant represents a GRANT statement.
type Grant struct {
	Privileges PrivilegeList
	Targets    TargetList
	Grantees   NameList
}

// TargetType represents the type of target.
type TargetType int

func (tt TargetType) String() string {
	return targetNames[tt]
}

// PrivilegeType represents the type of privilege.
type PrivilegeType int

func (pt PrivilegeType) String() string {
	return privilegeNames[pt]
}

// PrivilegeList represents a list of privileges.
type PrivilegeList []PrivilegeType

func (pl PrivilegeList) String() string {
	return pl.Join(", ")
}

// Join returns the list of privilege names joined using `sep`.
// The default stringer uses `, `, but we want to be able to
// get a tight representation of the string.
func (pl PrivilegeList) Join(sep string) string {
	var buf bytes.Buffer
	for i, n := range pl {
		if i > 0 {
			_, _ = buf.WriteString(sep)
		}
		buf.WriteString(n.String())
	}
	return buf.String()
}

// Enums for target and privilege types.
const (
	TargetDatabase TargetType = iota

	PrivilegeAll PrivilegeType = iota
	PrivilegeRead
	PrivilegeWrite
)

var (
	targetNames = [...]string{
		TargetDatabase: "DATABASE",
	}

	privilegeNames = [...]string{
		PrivilegeAll:   "ALL",
		PrivilegeRead:  "READ",
		PrivilegeWrite: "WRITE",
	}
)

// TargetList represents a list of targets.
// Only one field may be non-nil.
type TargetList struct {
	Databases NameList
	Tables    QualifiedNames
}

func (tl TargetList) String() string {
	if tl.Databases != nil {
		return fmt.Sprintf("DATABASE %s", tl.Databases)
	}
	return fmt.Sprintf("%s", tl.Tables)
}

func (node *Grant) String() string {
	return fmt.Sprintf("GRANT %s ON %s TO %v",
		node.Privileges,
		node.Targets,
		node.Grantees)
}
