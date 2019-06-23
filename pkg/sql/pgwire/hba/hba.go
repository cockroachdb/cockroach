// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// GetOption returns the value of option name if there is exactly one
// occurrence of name in the options list, otherwise the empty string.
func (h Entry) GetOption(name string) string {
	var val string
	for _, opt := range h.Options {
		if opt[0] == name {
			// If there is more than one entry, return empty string.
			if val != "" {
				return ""
			}
			val = opt[1]
		}
	}
	return val
}

// GetOptions returns all values of option name.
func (h Entry) GetOptions(name string) []string {
	var val []string
	for _, opt := range h.Options {
		if opt[0] == name {
			val = append(val, opt[1])
		}
	}
	return val
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
