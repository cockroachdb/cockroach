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

import "bytes"

// Set represents a SET or RESET statement.
type Set struct {
	Name    VarName
	Values  Exprs
	SetMode SetMode
}

// SetMode is an enum of the various set modes.
type SetMode int

const (
	// SetModeAssign represents a SET statement.
	SetModeAssign SetMode = iota

	// SetModeReset represents a RESET statement.
	SetModeReset

	// SetModeClusterSetting represents a SET CLUSTER SETTING statement.
	SetModeClusterSetting
)

// Format implements the NodeFormatter interface.
func (node *Set) Format(buf *bytes.Buffer, f FmtFlags) {
	switch node.SetMode {
	case SetModeAssign, SetModeClusterSetting:
		buf.WriteString("SET ")
		if node.SetMode == SetModeClusterSetting {
			buf.WriteString("CLUSTER SETTING ")
		}
		if node.Name != nil {
			FormatNode(buf, f, node.Name)
			buf.WriteString(" = ")
		}
		if node.Values == nil {
			buf.WriteString("DEFAULT")
		} else {
			FormatNode(buf, f, node.Values)
		}
	case SetModeReset:
		buf.WriteString("RESET ")
		FormatNode(buf, f, node.Name)
	}
}

// SetTransaction represents a SET TRANSACTION statement.
type SetTransaction struct {
	Isolation    IsolationLevel
	UserPriority UserPriority
}

// Format implements the NodeFormatter interface.
func (node *SetTransaction) Format(buf *bytes.Buffer, f FmtFlags) {
	var sep string
	buf.WriteString("SET TRANSACTION")
	if node.Isolation != UnspecifiedIsolation {
		buf.WriteString(" ISOLATION LEVEL ")
		buf.WriteString(node.Isolation.String())
		sep = ","
	}
	if node.UserPriority != UnspecifiedUserPriority {
		buf.WriteString(sep)
		buf.WriteString(" PRIORITY ")
		buf.WriteString(node.UserPriority.String())
	}
}

// SetTimeZone represents a SET TIME ZONE statement.
type SetTimeZone struct {
	Value Expr
}

// Format implements the NodeFormatter interface.
func (node *SetTimeZone) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SET TIME ZONE ")
	switch node.Value.(type) {
	case *DInterval:
		FormatNode(buf, f, node.Value)
	default:
		var s string
		switch v := node.Value.(type) {
		case *DString:
			s = string(*v)
		case *StrVal:
			s = v.s
		}
		if s == "DEFAULT" || s == "LOCAL" {
			buf.WriteString(s)
		} else {
			FormatNode(buf, f, node.Value)
		}
	}
}

// SetDefaultIsolation represents a SET SESSION CHARACTERISTICS AS TRANSACTION statement.
type SetDefaultIsolation struct {
	Isolation IsolationLevel
}

// Format implements the NodeFormatter interface.
func (node *SetDefaultIsolation) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL")
	if node.Isolation != UnspecifiedIsolation {
		buf.WriteByte(' ')
		buf.WriteString(node.Isolation.String())
	}
}
