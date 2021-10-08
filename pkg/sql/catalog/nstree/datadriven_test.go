// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nstree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/datadriven"
)

const notFound = "not found"

type argType int

const (
	argParentID = 1 << iota
	argParentSchemaID
	argID
	argName
	argStopAfter
)

type args struct {
	set                          argType
	parentID, parentSchemaID, id descpb.ID
	name                         string
	stopAfter                    int
}

func parseArgs(t *testing.T, d *datadriven.TestData, required, allowed argType) args {
	allowed = allowed | required // all required are allowed
	var a args
	for at, p := range argParser {
		if ok := d.HasArg(p.key); ok {
			if allowed&at == 0 {
				d.Fatalf(t, "%s: illegal argument %s", d.Cmd, p.key)
			}
			p.sf(t, d, p.key, &a)
			a.set |= at
		} else if required&at != 0 {
			d.Fatalf(t, "%s: missing required argument %s", d.Cmd, p.key)
		}
	}
	for _, a := range d.CmdArgs {
		if _, ok := argKeys[a.Key]; !ok {
			d.Fatalf(t, "%s: unknown argument %s", d.Cmd, a.Key)
		}
	}
	return a
}

type setFunc func(t *testing.T, d *datadriven.TestData, key string, a *args) bool

var (
	setDescIDFunc = func(f func(a *args) *descpb.ID) setFunc {
		return func(t *testing.T, d *datadriven.TestData, key string, a *args) bool {
			t.Helper()
			var id int
			d.ScanArgs(t, key, &id)
			*f(a) = descpb.ID(id)
			return true
		}
	}
	setStringFunc = func(f func(a *args) *string) setFunc {
		return func(t *testing.T, d *datadriven.TestData, key string, a *args) bool {
			t.Helper()
			d.ScanArgs(t, key, f(a))
			return true
		}
	}
	setIntFunc = func(f func(a *args) *int) setFunc {
		return func(t *testing.T, d *datadriven.TestData, key string, a *args) bool {
			t.Helper()
			d.ScanArgs(t, key, f(a))
			return true
		}
	}
	argParser = map[argType]struct {
		key string
		sf  setFunc
	}{
		argParentID: {
			"parent-id",
			setDescIDFunc(func(a *args) *descpb.ID { return &a.parentID }),
		},
		argParentSchemaID: {
			"parent-schema-id",
			setDescIDFunc(func(a *args) *descpb.ID { return &a.parentSchemaID }),
		},
		argID: {
			"id",
			setDescIDFunc(func(a *args) *descpb.ID { return &a.id }),
		},
		argName: {
			"name",
			setStringFunc(func(a *args) *string { return &a.name }),
		},
		argStopAfter: {
			"stop-after",
			setIntFunc(func(a *args) *int { return &a.stopAfter }),
		},
	}
	argKeys = func() map[string]struct{} {
		m := make(map[string]struct{}, len(argParser))
		for _, p := range argParser {
			m[p.key] = struct{}{}
		}
		return m
	}()
)
