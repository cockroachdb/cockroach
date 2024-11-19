// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachtestflags

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
)

type cmdID int

const (
	listCmdID cmdID = iota
	runCmdID
	runOpsCmdID
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
		case *spec.Cloud:
			cmdFlags.VarP(&cloudValue{val: p}, f.Name, f.Shorthand, usage)
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

// Changed returns non-nil FlagInfo iff a flag associated with the given value was passed.
func (m *manager) Changed(valPtr interface{}) *FlagInfo {
	// We don't know which command we're running, but we'll only run one per
	// program invocation; so check all of them.
	for cmd := cmdID(0); cmd < numCmdIDs; cmd++ {
		if f, ok := m.flags[cmd][valPtr]; ok {
			for _, flagSet := range f.flagSets {
				if flagSet.Changed(f.Name) {
					return &f.FlagInfo
				}
			}
		}
	}
	return nil
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

type cloudValue struct {
	val *spec.Cloud
}

var _ pflag.Value = (*cloudValue)(nil)

func (cv *cloudValue) String() string {
	return cv.val.String()
}

func (cv *cloudValue) Type() string {
	return "string"
}

func (cv *cloudValue) Set(str string) error {
	if strings.ToLower(str) == "all" {
		*cv.val = spec.AnyCloud
		return nil
	}
	val, ok := spec.TryCloudFromString(str)
	if !ok {
		return errors.Errorf("invalid cloud %q", str)
	}
	*cv.val = val
	return nil
}
