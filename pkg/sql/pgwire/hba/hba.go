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
	"net"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/olekukonko/tablewriter"
)

// Conf is a parsed configuration.
type Conf struct {
	Entries []Entry
}

// Entry is a single line of a configuration.
type Entry struct {
	// ConnType is the connection type to match.
	ConnType ConnType
	// Database is the list of databases to match. An empty list means
	// "match any database".
	Database []String
	// User is the list of users to match. An empty list means "match
	// any user".
	User []String
	// Address is either AnyAddr, *net.IPNet or (unsupported) String for a hostname.
	Address interface{}
	Method  String
	// MethodFn is populated during name resolution of Method.
	MethodFn     interface{}
	Options      [][2]string
	OptionQuotes []bool
	// Input is the original configuration line in the HBA configuration string.
	// This is used for auditing purposes.
	Input string
	// Generated is true if the entry was expanded from another. All the
	// generated entries share the same value for Input.
	Generated bool
}

// ConnType represents the type of connection matched by a rule.
type ConnType int

const (
	// ConnLocal matches unix socket connections.
	ConnLocal ConnType = 1 << iota
	// ConnHostNoSSL matches TCP connections without SSL/TLS.
	ConnHostNoSSL
	// ConnHostSSL matches TCP connections with SSL/TLS.
	ConnHostSSL

	// ConnHostAny matches TCP connections with or without SSL/TLS.
	ConnHostAny = ConnHostNoSSL | ConnHostSSL

	// ConnAny matches any connection type. Used when registering auth
	// methods.
	ConnAny = ConnHostAny | ConnLocal
)

// String implements the fmt.Stringer interface.
func (t ConnType) String() string {
	switch t {
	case ConnLocal:
		return "local"
	case ConnHostNoSSL:
		return "hostnossl"
	case ConnHostSSL:
		return "hostssl"
	case ConnHostAny:
		return "host"
	default:
		panic(errors.Newf("unimplemented conn type: %v", int(t)))
	}
}

// String implements the fmt.Stringer interface.
func (c Conf) String() string {
	if len(c.Entries) == 0 {
		return "# (empty configuration)\n"
	}
	var sb strings.Builder
	sb.WriteString("# Original configuration:\n")
	for _, e := range c.Entries {
		if e.Generated {
			continue
		}
		fmt.Fprintf(&sb, "# %s\n", e.Input)
	}
	sb.WriteString("#\n# Interpreted configuration:\n")

	table := tablewriter.NewWriter(&sb)
	table.SetAutoWrapText(false)
	table.SetReflowDuringAutoWrap(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(false)
	table.SetNoWhiteSpace(true)
	table.SetTrimWhiteSpaceAtEOL(true)
	table.SetTablePadding(" ")

	row := []string{"# TYPE", "DATABASE", "USER", "ADDRESS", "METHOD", "OPTIONS"}
	table.Append(row)
	for _, e := range c.Entries {
		row[0] = e.ConnType.String()
		row[1] = e.DatabaseString()
		row[2] = e.UserString()
		row[3] = e.AddressString()
		row[4] = e.Method.String()
		row[5] = e.OptionsString()
		table.Append(row)
	}
	table.Render()
	return sb.String()
}

// AnyAddr represents "any address" and is used when parsing "all" for
// the "Address" field.
type AnyAddr struct{}

// String implements the fmt.Stringer interface.
func (AnyAddr) String() string { return "all" }

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

// Equivalent returns true iff the entry is equivalent to another,
// excluding the original syntax.
func (h Entry) Equivalent(other Entry) bool {
	h.Input = ""
	other.Input = ""
	return reflect.DeepEqual(h, other)
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

// ConnTypeMatches returns true iff the provided actual client connection
// type matches the connection type specified in the rule.
func (h Entry) ConnTypeMatches(clientConn ConnType) bool {
	switch clientConn {
	case ConnLocal:
		return h.ConnType == ConnLocal
	case ConnHostSSL:
		// A SSL connection matches both "hostssl" and "host".
		return h.ConnType&ConnHostSSL != 0
	case ConnHostNoSSL:
		// A non-SSL connection matches both "hostnossl" and "host".
		return h.ConnType&ConnHostNoSSL != 0
	default:
		panic("unimplemented")
	}
}

// ConnMatches returns true iff the provided client connection
// type and address matches the entry spec.
func (h Entry) ConnMatches(clientConn ConnType, ip net.IP) (bool, error) {
	if !h.ConnTypeMatches(clientConn) {
		return false, nil
	}
	if clientConn != ConnLocal {
		return h.AddressMatches(ip)
	}
	return true, nil
}

// UserMatches returns true iff the provided username matches the an
// entry in the User list or if the user list is empty (the entry
// matches all).
//
// The provided username must be normalized already.
// The function assumes the entry was normalized to contain only
// one user and its username normalized. See ParseAndNormalize().
func (h Entry) UserMatches(userName security.SQLUsername) bool {
	if h.User == nil {
		return true
	}
	for _, u := range h.User {
		if u.Value == userName.Normalized() {
			return true
		}
	}
	return false
}

// AddressMatches returns true iff the provided address matches the
// entry. The function assumes the entry was normalized already.
// See ParseAndNormalize.
func (h Entry) AddressMatches(addr net.IP) (bool, error) {
	switch a := h.Address.(type) {
	case AnyAddr:
		return true, nil
	case *net.IPNet:
		return a.Contains(addr), nil
	default:
		// This is where name-based validation can occur later.
		return false, errors.Newf("unknown address type: %T", addr)
	}
}

// DatabaseString returns a string that describes the database field.
func (h Entry) DatabaseString() string {
	if h.Database == nil {
		return "all"
	}
	var sb strings.Builder
	comma := ""
	for _, s := range h.Database {
		sb.WriteString(comma)
		sb.WriteString(s.String())
		comma = ","
	}
	return sb.String()
}

// UserString returns a string that describes the username field.
func (h Entry) UserString() string {
	if h.User == nil {
		return "all"
	}
	var sb strings.Builder
	comma := ""
	for _, s := range h.User {
		sb.WriteString(comma)
		sb.WriteString(s.String())
		comma = ","
	}
	return sb.String()
}

// AddressString returns a string that describes the address field.
func (h Entry) AddressString() string {
	if h.Address == nil {
		// This is possible for conn type "local".
		return ""
	}
	return fmt.Sprintf("%s", h.Address)
}

// OptionsString returns a string that describes the option field.
func (h Entry) OptionsString() string {
	var sb strings.Builder
	sp := ""
	for i, opt := range h.Options {
		sb.WriteString(sp)
		sb.WriteString(String{Value: opt[0] + "=" + opt[1], Quoted: h.OptionQuotes[i]}.String())
		sp = " "
	}
	return sb.String()
}

// String implements the fmt.Stringer interface.
func (h Entry) String() string {
	return Conf{Entries: []Entry{h}}.String()
}

// String is a possibly quoted string.
type String struct {
	Value  string
	Quoted bool
}

// String implements the fmt.Stringer interface.
func (s String) String() string {
	if s.Quoted {
		return `"` + s.Value + `"`
	}
	return s.Value
}

// Empty returns true iff s is the unquoted empty string.
func (s String) Empty() bool { return s.IsKeyword("") }

// IsKeyword returns whether s is the non-quoted string v.
func (s String) IsKeyword(v string) bool {
	return !s.Quoted && s.Value == v
}

// ParseAndNormalize parses the HBA configuration from the provided
// string and performs two tasks:
//
// - it unicode-normalizes the usernames. Since usernames are
//   initialized during pgwire session initialization, this
//   ensures that string comparisons can be used to match usernames.
//
// - it ensures there is one entry per username. This simplifies
//   the code in the authentication logic.
//
func ParseAndNormalize(val string) (*Conf, error) {
	conf, err := Parse(val)
	if err != nil {
		return nil, err
	}

	entries := conf.Entries[:0]
	entriesCopied := false
outer:
	for i := range conf.Entries {
		entry := conf.Entries[i]

		// The database field is not supported yet in CockroachDB.
		entry.Database = nil

		// Normalize the 'all' keyword into AnyAddr.
		if addr, ok := entry.Address.(String); ok && addr.IsKeyword("all") {
			entry.Address = AnyAddr{}
		}

		// If we're observing an "any" entry, just keep that and move
		// along.
		for _, iu := range entry.User {
			if iu.IsKeyword("all") {
				entry.User = nil
				entries = append(entries, entry)
				continue outer
			}
		}

		// If we're about to change the size of the slice, first copy the
		// result entries.
		if len(entry.User) != 1 && !entriesCopied {
			entries = append([]Entry(nil), conf.Entries[:len(entries)]...)
			entriesCopied = true
		}
		// Expand and normalize the usernames.
		allUsers := entry.User
		for userIdx, iu := range allUsers {
			entry.User = allUsers[userIdx : userIdx+1]
			entry.User[0].Value = tree.Name(iu.Value).Normalize()
			if userIdx > 0 {
				entry.Generated = true
			}
			entries = append(entries, entry)
		}
	}
	conf.Entries = entries
	return conf, nil
}
