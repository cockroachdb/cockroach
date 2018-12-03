// Copyright 2018 The Cockroach Authors.
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

// Package hba implements an hba.conf parser.
package hba

// conf.rl is a ragel v6.10 file containing a parser for pg_hba.conf
// files. "make" should be executed in this directory when conf.rl is
// changed. Since it is changed so rarely it is not hooked up to the top-level
// Makefile since that would require ragel being a dev dependency, which is
// an annoying burden since it's written in C and we can't auto install it
// on all systems.

import (
	"fmt"
	"strings"
)

// Conf is a parsed configuration.
type Conf struct {
	Entries []Entry
}

func (c Conf) String() string {
	var sb strings.Builder
	for _, e := range c.Entries {
		fmt.Fprintf(&sb, "%s\n", e)
	}
	return sb.String()
}

// Entry is a single line of a configuration.
type Entry struct {
	Type     string
	Database []String
	User     []String
	// Address is either a String or *net.IPNet.
	Address interface{}
	Method  string
	Options [][2]string
}

func (h Entry) String() string {
	var sb strings.Builder
	sb.WriteString("host ")
	comma := ""
	for _, s := range h.Database {
		sb.WriteString(comma)
		sb.WriteString(s.String())
		comma = ","
	}
	sb.WriteByte(' ')
	comma = ""
	for _, s := range h.User {
		sb.WriteString(comma)
		sb.WriteString(s.String())
		comma = ","
	}
	fmt.Fprintf(&sb, " %s %s", h.Address, h.Method)
	for _, opt := range h.Options {
		fmt.Fprintf(&sb, " %s=%s", opt[0], opt[1])
	}
	return sb.String()
}

// String is a possibly quoted string.
type String struct {
	Value  string
	Quoted bool
}

func (s String) String() string {
	if s.Quoted {
		return fmt.Sprintf(`"%s"`, s.Value)
	}
	return s.Value
}

// IsSpecial returns whether s is the non-quoted string v.
func (s String) IsSpecial(v string) bool {
	return !s.Quoted && s.Value == v
}
