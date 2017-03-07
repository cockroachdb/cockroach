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

import (
	"bytes"

	"github.com/pkg/errors"
)

// Backup represents a BACKUP statement.
type Backup struct {
	Targets         TargetList
	To              string
	IncrementalFrom []string
	AsOf            AsOfClause
	Options         KVOptions
}

var _ Statement = &Backup{}

// Format implements the NodeFormatter interface.
func (node *Backup) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("BACKUP ")
	FormatNode(buf, f, node.Targets)
	buf.WriteString(" TO ")
	encodeSQLStringWithFlags(buf, node.To, f)
	if node.AsOf.Expr != nil {
		buf.WriteString(" ")
		FormatNode(buf, f, node.AsOf)
	}
	if node.IncrementalFrom != nil {
		buf.WriteString(" INCREMENTAL FROM ")
		for i, from := range node.IncrementalFrom {
			if i > 0 {
				buf.WriteString(", ")
			}
			encodeSQLStringWithFlags(buf, from, f)
		}
	}
	if node.Options != nil {
		buf.WriteString(" WITH OPTIONS (")
		FormatNode(buf, f, node.Options)
		buf.WriteString(")")
	}
}

// Restore represents a RESTORE statement.
type Restore struct {
	Targets TargetList
	From    []string
	AsOf    AsOfClause
	Options KVOptions
}

var _ Statement = &Restore{}

// Format implements the NodeFormatter interface.
func (node *Restore) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("RESTORE ")
	FormatNode(buf, f, node.Targets)
	buf.WriteString(" FROM ")
	for i, from := range node.From {
		if i > 0 {
			buf.WriteString(", ")
		}
		encodeSQLStringWithFlags(buf, from, f)
	}
	if node.AsOf.Expr != nil {
		buf.WriteString(" ")
		FormatNode(buf, f, node.AsOf)
	}
	if node.Options != nil {
		buf.WriteString(" WITH OPTIONS (")
		FormatNode(buf, f, node.Options)
		buf.WriteString(")")
	}
}

// KVOption is a key-value option.
// It has a `read` state that should be marked when accessed.
type KVOption struct {
	Key   string
	Value string
	read  bool
}

// KVOptions is a list of KVOptions.
type KVOptions []KVOption

// Get returns value for requested key and if it was found or not.
// Returns an error if more than one value for key is found.
func (o KVOptions) Get(key string) (string, bool, error) {
	found := false
	var val string
	for i := range o {
		if key == o[i].Key {
			if found {
				return "", false, errors.Errorf("duplicate value for parameter %q", key)
			}
			found = true
			val = o[i].Value
			o[i].read = true
		}
	}
	return val, found, nil
}

func (o KVOptions) CheckAllRead() error {
	for _, k := range o {
		if !k.read {
			return errors.Errorf("option %q unused", k.Key)
		}
	}
	return nil
}

// Format implements the NodeFormatter interface.
func (o KVOptions) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, n := range o {
		if i > 0 {
			buf.WriteString(", ")
		}
		encodeSQLStringWithFlags(buf, n.Key, f)
		if len(n.Value) != 0 {
			buf.WriteString(`=`)
			encodeSQLStringWithFlags(buf, n.Value, f)
		}
	}
}
