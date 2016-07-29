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

// Insert represents an INSERT statement.
type Insert struct {
	Table      TableExpr
	Columns    QualifiedNames
	Rows       *Select
	OnConflict *OnConflict
	Returning  ReturningExprs
}

// Format implements the NodeFormatter interface.
func (node *Insert) Format(buf *bytes.Buffer, f FmtFlags) {
	if node.OnConflict.IsUpsertAlias() {
		buf.WriteString("UPSERT")
	} else {
		buf.WriteString("INSERT")
	}
	buf.WriteString(" INTO ")
	FormatNode(buf, f, node.Table)
	if node.Columns != nil {
		buf.WriteByte('(')
		FormatNode(buf, f, node.Columns)
		buf.WriteByte(')')
	}
	if node.DefaultValues() {
		buf.WriteString(" DEFAULT VALUES")
	} else {
		buf.WriteByte(' ')
		FormatNode(buf, f, node.Rows)
	}
	if node.OnConflict != nil && !node.OnConflict.IsUpsertAlias() {
		buf.WriteString(" ON CONFLICT")
		if len(node.OnConflict.Columns) > 0 {
			buf.WriteString(" (")
			FormatNode(buf, f, node.OnConflict.Columns)
			buf.WriteString(")")
		}
		if node.OnConflict.DoNothing {
			buf.WriteString(" DO NOTHING")
		} else {
			buf.WriteString(" DO UPDATE SET ")
			FormatNode(buf, f, node.OnConflict.Exprs)
			if node.OnConflict.Where != nil {
				FormatNode(buf, f, node.OnConflict.Where)
			}
		}
	}
	FormatNode(buf, f, node.Returning)
}

// DefaultValues returns true iff only default values are being inserted.
func (node *Insert) DefaultValues() bool {
	return node.Rows.Select == nil
}

// OnConflict represents an `ON CONFLICT (columns) DO UPDATE SET exprs WHERE
// where` clause.
//
// The zero value for OnConflict is used to signal the UPSERT short form, which
// uses the primary key for as the conflict index and the values being inserted
// for Exprs.
type OnConflict struct {
	Columns   NameList
	Exprs     UpdateExprs
	Where     *Where
	DoNothing bool
}

// IsUpsertAlias returns true if the UPSERT syntactic sugar was used.
func (oc *OnConflict) IsUpsertAlias() bool {
	return oc != nil && oc.Columns == nil && oc.Exprs == nil && oc.Where == nil && !oc.DoNothing
}
