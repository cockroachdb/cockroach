// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachtestflags

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/pflag"
)

type cmdID int

const (
	listCmdID cmdID = iota
	runCmdID
	numCmdIDs
)

type manager struct {
	flags [numCmdIDs]map[interface{}]*flagData
}

type flagData struct {
	FlagInfo
	// Flag sets to which the flag has been added.
	flagSets []*pflag.FlagSet
}

// RegisterFlag defines a flag for a type of command.
//
// It is not allowed to register the same value pointer to the same command
// multiple times.
func (m *manager) RegisterFlag(cmd cmdID, valPtr interface{}, info FlagInfo) {
	if _, ok := m.flags[cmd][valPtr]; ok {
		panic("flag value registered twice")
	}
	if m.flags[cmd] == nil {
		m.flags[cmd] = make(map[interface{}]*flagData)
	}
	m.flags[cmd][valPtr] = &flagData{
		FlagInfo: info,
	}
}

// AddFlagsToCommand adds all flags registered to cmd to the flag set for a
// command.
func (m *manager) AddFlagsToCommand(cmd cmdID, cmdFlags *pflag.FlagSet) {
	for p, f := range m.flags[cmd] {
		usage := cleanupString(f.Usage)
		switch p := p.(type) {
		case *bool:
			cmdFlags.BoolVarP(p, f.Name, f.Shorthand, *p, usage)
		case *int:
			cmdFlags.IntVarP(p, f.Name, f.Shorthand, *p, usage)
		case *int64:
			cmdFlags.Int64VarP(p, f.Name, f.Shorthand, *p, usage)
		case *uint64:
			cmdFlags.Uint64VarP(p, f.Name, f.Shorthand, *p, usage)
		case *float64:
			cmdFlags.Float64VarP(p, f.Name, f.Shorthand, *p, usage)
		case *time.Duration:
			cmdFlags.DurationVarP(p, f.Name, f.Shorthand, *p, usage)
		case *string:
			cmdFlags.StringVarP(p, f.Name, f.Shorthand, *p, usage)
		case *map[string]string:
			cmdFlags.StringToStringVarP(p, f.Name, f.Shorthand, *p, usage)
		default:
			panic(fmt.Sprintf("unsupported pointer type %T", p))
		}
		if f.Deprecated != "" {
			if err := cmdFlags.MarkDeprecated(f.Name, cleanupString(f.Deprecated)); err != nil {
				// An error here is not possible, as we just defined this flag above.
				panic(err)
			}
		}
		f.flagSets = append(f.flagSets, cmdFlags)
	}
}

// Changed returns true if a flag associated with the given value was passed.
func (m *manager) Changed(valPtr interface{}) bool {
	// We don't know which command we're running, but we'll only run one per
	// program invocation; so check all of them.
	for cmd := cmdID(0); cmd < numCmdIDs; cmd++ {
		if f, ok := m.flags[cmd][valPtr]; ok {
			for _, flagSet := range f.flagSets {
				if flagSet.Changed(f.Name) {
					return true
				}
			}
		}
	}
	return false
}

// cleanupString converts a multi-line string into a single-line string,
// removing all extra whitespace at the beginning and end of lines.
func cleanupString(s string) string {
	l := strings.Split(s, "\n")
	for i := range l {
		l[i] = strings.TrimSpace(l[i])
	}
	// Remove leading and trailing empty lines.
	for len(l) > 0 && l[0] == "" {
		l = l[1:]
	}
	for len(l) > 0 && l[len(l)-1] == "" {
		l = l[:len(l)-1]
	}
	return strings.Join(l, " ")
}
