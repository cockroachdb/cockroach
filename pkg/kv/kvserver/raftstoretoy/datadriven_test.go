// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v2"
)

// TestRaftStoreToy is the main entry point for our datadriven tests.
// It demonstrates the core functionality of the Raft storage design through
// simple, educational examples.
func TestRaftStoreToy(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		env := Env{
			logEng: &llLogEngine{enc: DefaultLogEncoding{}, e: NewLowLevelEngine()},
			smEng:  nil,
		}

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			env.Handle(t, d)
			return env.Output()
		})
	})
}

type Env struct {
	logEng LogEngine
	smEng  SMEngine

	out strings.Builder
}

func (e *Env) logf(format string, args ...interface{}) {
	e.out.WriteString(strings.TrimSpace(fmt.Sprintf(format+"\n", args...)))
}

func (e *Env) Output() string {
	return e.out.String()
}

func (e *Env) Handle(t *testing.T, d *datadriven.TestData) {
	e.out.Reset()
	defer func() {
		if e.out.Len() == 0 {
			e.logf("ok")
		}
	}()

	switch d.Cmd {
	case "create":
		var rangeID int64
		var replID int64
		d.ScanArgs(t, "range", &rangeID)
		d.ScanArgs(t, "repl", &replID)
		id, wix, err := e.Create(Create{
			RangeID:   roachpb.RangeID(rangeID),
			ReplicaID: roachpb.ReplicaID(replID),
		})
		if err != nil {
			e.logf("%s", err)
			return
		}
		e.logf("%s: %s", wix, id)
	default:
		t.Fatalf("unknown command: %s", d.Cmd)
	}
}

func y(obj any) string {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	if err := enc.Encode(obj); err != nil {
		return err.Error()
	}
	return buf.String()
}

func (e *Env) Create(op Create) (FullLogID, WAGIndex, error) {
	return e.logEng.Create(context.Background(), op)
}
