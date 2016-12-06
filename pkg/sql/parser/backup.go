// Copyright 2016 The Cockroach Authors.
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
// Author: Daniel Harrison (daniel.harrison@gmail.com

package parser

import "bytes"

// Backup represents a BACKUP statement.
type Backup struct {
	Database        Name
	To              *StrVal
	IncrementalFrom *StrVal
}

var _ Statement = &Backup{}

// Format implements the NodeFormatter interface.
func (node *Backup) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("BACKUP DATABASE ")
	FormatNode(buf, f, node.Database)
	buf.WriteString(" TO ")
	FormatNode(buf, f, node.To)
	if node.IncrementalFrom != nil {
		buf.WriteString(" INCREMENTAL FROM ")
		FormatNode(buf, f, node.IncrementalFrom)
	}
}

// Restore represents a RESTORE statement.
type Restore struct {
	Database Name
	From     *StrVal
}

var _ Statement = &Restore{}

// Format implements the NodeFormatter interface.
func (node *Restore) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("RESTORE DATABASE ")
	FormatNode(buf, f, node.Database)
	buf.WriteString(" FROM ")
	FormatNode(buf, f, node.From)
}
