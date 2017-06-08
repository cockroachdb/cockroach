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
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package parser

import "bytes"

// PauseJob represents a PAUSE JOB statement.
type PauseJob struct {
	ID Expr
}

// Format implements the NodeFormatter interface.
func (node *PauseJob) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("PAUSE JOB ")
	FormatNode(buf, f, node.ID)
}

// ResumeJob represents a RESUME JOB statement.
type ResumeJob struct {
	ID Expr
}

// Format implements the NodeFormatter interface.
func (node *ResumeJob) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("RESUME JOB ")
	FormatNode(buf, f, node.ID)
}

// CancelJob represents a CANCEL JOB statement.
type CancelJob struct {
	ID Expr
}

// Format implements the NodeFormatter interface.
func (node *CancelJob) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("CANCEL JOB ")
	FormatNode(buf, f, node.ID)
}

// CancelQuery represents a CANCEL QUERY statement.
type CancelQuery struct {
	ID Expr
}

// Format implements the NodeFormatter interface.
func (node *CancelQuery) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("CANCEL QUERY ")
	FormatNode(buf, f, node.ID)
}
