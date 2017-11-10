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

package parser

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
)

// This file contains column type definitions that don't fit
// one of the existing categories.
// When you add more types to this file, and you observe that multiple
// types form a group, consider splitting that group into its own
// file.

// UUIDColType represents a UUID type.
type UUIDColType struct{}

// Format implements the ColTypeFormatter interface.
func (node *UUIDColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString("UUID")
}

// IPAddrColType represents an INET or CIDR type.
type IPAddrColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *IPAddrColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// JSONColType represents the JSON column type.
type JSONColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *JSONColType) Format(buf *bytes.Buffer, _ lex.EncodeFlags) {
	buf.WriteString(node.Name)
}

// OidColType represents an OID type, which is the type of system object
// identifiers. There are several different OID types: the raw OID type, which
// can be any integer, and the reg* types, each of which corresponds to the
// particular system table that contains the system object identified by the
// OID itself.
//
// See https://www.postgresql.org/docs/9.6/static/datatype-oid.html.
type OidColType struct {
	Name string
}

// Format implements the ColTypeFormatter interface.
func (node *OidColType) Format(buf *bytes.Buffer, f lex.EncodeFlags) {
	buf.WriteString(node.Name)
}
