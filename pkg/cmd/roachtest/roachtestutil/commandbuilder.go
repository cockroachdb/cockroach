// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestutil

import (
	"fmt"
	"sort"
	"strings"
)

// Command wraps a command to be run in a cluster. It allows users to
// manipulate a command without having to perform string-based
// operations.
type Command struct {
	Binary    string
	Arguments []string
	Flags     map[string]*string
	UseEquals bool
}

// NewCommand builds a command. The format parameter can take
// `fmt.Print` verbs.
//
// Examples:
//
//	NewCommand("./cockroach version")
//	NewCommand("%s version", binaryPath)
func NewCommand(format string, args ...interface{}) *Command {
	cmd := fmt.Sprintf(format, args...)
	parts := strings.Fields(cmd)
	return &Command{
		Binary:    parts[0],
		Arguments: parts[1:],
		Flags:     make(map[string]*string),
	}
}

func (c *Command) WithEqualsSyntax() *Command {
	c.UseEquals = true
	return c
}

func (c *Command) Arg(format string, args ...interface{}) *Command {
	c.Arguments = append(c.Arguments, fmt.Sprintf(format, args...))
	return c
}

func (c *Command) HasFlag(name string) bool {
	_, ok := c.Flags[name]
	return ok
}

func (c *Command) Flag(name string, val interface{}) *Command {
	c.Flags[name] = stringP(fmt.Sprint(val))
	return c
}

// MaybeFlag is a thin wrapper around Flag for the caller's
// convenience. The flag is added only if the `condition` parameter is
// true.
func (c *Command) MaybeFlag(condition bool, name string, val interface{}) *Command {
	if condition {
		return c.Flag(name, val)
	}

	return c
}

// Option adds a flag that doesn't have an associated value
func (c *Command) Option(name string) *Command {
	c.Flags[name] = nil
	return c
}

func (c *Command) MaybeOption(condition bool, name string) *Command {
	if condition {
		return c.Option(name)
	}

	return c
}

// ITEFlag (if-then-else flag) adds a flag where the value depends on
// the `condition` parameter. `trueVal` is used if `condition` is
// true; `falseVal` is used otherwise.
func (c *Command) ITEFlag(condition bool, name string, trueVal, falseVal interface{}) *Command {
	if condition {
		return c.Flag(name, trueVal)
	}

	return c.Flag(name, falseVal)
}

// String returns a canonical string representation of the command
// which can be passed to `cluster.Run`.
func (c *Command) String() string {
	flags := make([]string, 0, len(c.Flags))
	names := make([]string, 0, len(c.Flags))
	for name := range c.Flags {
		names = append(names, name)
	}
	sort.Strings(names)

	flagJoinSymbol := " "
	if c.UseEquals {
		flagJoinSymbol = "="
	}

	for _, name := range names {
		val := c.Flags[name]
		prefix := "-"
		if len(name) > 1 {
			prefix = "--"
		}

		prefixedName := prefix + name
		parts := []string{prefixedName}
		if val != nil {
			parts = append(parts, *val)
		}
		flags = append(flags, strings.Join(parts, flagJoinSymbol))
	}

	cmd := append(
		[]string{c.Binary},
		append(c.Arguments, flags...)...,
	)

	return strings.Join(cmd, " ")
}

func stringP(s string) *string {
	return &s
}
