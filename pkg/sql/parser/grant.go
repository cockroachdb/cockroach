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
// Author: Marc Berhault (marc@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// Grant represents a GRANT statement.
type Grant struct {
	Privileges privilege.List
	Targets    TargetList
	Grantees   NameList
}

// TargetList represents a list of targets.
// Only one field may be non-nil.
type TargetList struct {
	Databases NameList
	Tables    TablePatterns
}

// Format implements the NodeFormatter interface.
func (tl TargetList) Format(buf *bytes.Buffer, f FmtFlags) {
	if tl.Databases != nil {
		buf.WriteString("DATABASE ")
		FormatNode(buf, f, tl.Databases)
	} else {
		FormatNode(buf, f, tl.Tables)
	}
}

// Format implements the NodeFormatter interface.
func (node *Grant) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("GRANT ")
	node.Privileges.Format(buf)
	buf.WriteString(" ON ")
	FormatNode(buf, f, node.Targets)
	buf.WriteString(" TO ")
	FormatNode(buf, f, node.Grantees)
}
