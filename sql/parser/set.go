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
// Author: Peter Mattis (peter@cockroachlabs.com)

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

// Set represents a SET statement.
type Set struct {
	Name   *QualifiedName
	Values Exprs
}

func (node *Set) String() string {
	if node.Values == nil {
		return fmt.Sprintf("SET %s = DEFAULT", node.Name)
	}
	return fmt.Sprintf("SET %s = %v", node.Name, node.Values)
}

// SetTransaction represents a SET TRANSACTION statement.
type SetTransaction struct {
	Isolation    IsolationLevel
	UserPriority UserPriority
}

func (node *SetTransaction) String() string {
	var buf bytes.Buffer
	var sep string
	buf.WriteString("SET TRANSACTION")
	if node.Isolation != UnspecifiedIsolation {
		fmt.Fprintf(&buf, " ISOLATION LEVEL %s", node.Isolation)
		sep = ","
	}
	if node.UserPriority != UnspecifiedUserPriority {
		fmt.Fprintf(&buf, "%s PRIORITY %s", sep, node.UserPriority)
	}
	return buf.String()
}

// SetTimeZone represents a SET TIME ZONE statement.
type SetTimeZone struct {
	Value Expr
}

func (node *SetTimeZone) String() string {
	prefix := "SET TIME ZONE"
	switch v := node.Value.(type) {
	case DInterval:
		return fmt.Sprintf("%s INTERVAL '%s'", prefix, v)

	case DString:
		if zone := string(v); zone == "DEFAULT" || zone == "LOCAL" {
			return fmt.Sprintf("%s %s", prefix, zone)
		}
	}
	return fmt.Sprintf("%s %s", prefix, node.Value)
}

// SetDefaultIsolation represents a SET SESSION CHARACTERISTICS AS TRANSACTION statement.
type SetDefaultIsolation struct {
	Isolation IsolationLevel
}

func (node *SetDefaultIsolation) String() string {
	var buf bytes.Buffer
	buf.WriteString("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL")
	if node.Isolation != UnspecifiedIsolation {
		fmt.Fprintf(&buf, " %s", node.Isolation)
	}
	return buf.String()
}
