// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgtest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/jackc/pgx/pgproto3"
)

// Walk walks path for datadriven files and calls RunTest on them.
func Walk(t *testing.T, path, addr, user string) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		RunTest(t, path, addr, user)
	})
}

// RunTest executes PGTest commands, connecting to the database specified by
// addr and user. Supported commands:
//
// "send": Sends messages to a server. Takes a newline-delimited list of
// pgproto3.FrontendMessage types. Can fill in values by adding a space then
// a JSON object. No output.
//
// "until": Receives all messages from a server until messages of the given
// types have been seen. Converts them to JSON one per line as output. Takes
// a newline-delimited list of pgproto3.BackendMessage types. An ignore option
// can be used to specify types to ignore. ErrorResponse messages are
// immediately returned as errors unless they are the expected type, in which
// case they will marshal to an empty ErrorResponse message since our error
// detail specifics differ from Postgres.
//
// "receive": Like "until", but only output matching messages instead of all
// messages.
func RunTest(t *testing.T, path, addr, user string) {
	p, err := NewPGTest(context.Background(), addr, user)
	if err != nil {
		t.Fatal(err)
	}
	datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
		switch d.Cmd {
		case "send":
			for _, line := range strings.Split(d.Input, "\n") {
				sp := strings.SplitN(line, " ", 2)
				msg := toMessage(sp[0])
				if len(sp) == 2 {
					if err := json.Unmarshal([]byte(sp[1]), msg); err != nil {
						t.Fatal(err)
					}
				}
				if err := p.Send(msg.(pgproto3.FrontendMessage)); err != nil {
					t.Fatalf("%s: send %s: %v", d.Pos, line, err)
				}
			}
			return ""
		case "receive":
			until := parseMessages(d.Input)
			msgs, err := p.Receive(until...)
			if err != nil {
				t.Fatalf("%s: %+v", d.Pos, err)
			}
			return msgsToJSONWithIgnore(msgs, d)
		case "until":
			until := parseMessages(d.Input)
			msgs, err := p.Until(until...)
			if err != nil {
				t.Fatalf("%s: %+v", d.Pos, err)
			}
			return msgsToJSONWithIgnore(msgs, d)
		default:
			t.Fatalf("unknown command %s", d.Cmd)
			return ""
		}
	})
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

}

func parseMessages(s string) []pgproto3.BackendMessage {
	var msgs []pgproto3.BackendMessage
	for _, typ := range strings.Split(s, "\n") {
		msgs = append(msgs, toMessage(typ).(pgproto3.BackendMessage))
	}
	return msgs
}

func msgsToJSONWithIgnore(msgs []pgproto3.BackendMessage, args *datadriven.TestData) string {
	ignore := map[string]bool{}
	for _, arg := range args.CmdArgs {
		if arg.Key == "ignore" {
			for _, typ := range arg.Vals {
				ignore[fmt.Sprintf("*pgproto3.%s", typ)] = true
			}
		}
	}
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, msg := range msgs {
		if ignore[fmt.Sprintf("%T", msg)] {
			continue
		}
		if reflect.TypeOf(msg) == typErrorResponse {
			if err := enc.Encode(struct {
				Type string
			}{
				Type: "ErrorResponse",
			}); err != nil {
				panic(err)
			}
		} else if err := enc.Encode(msg); err != nil {
			panic(err)
		}
	}
	return sb.String()
}

func toMessage(typ string) interface{} {
	switch typ {
	case "Bind":
		return &pgproto3.Bind{}
	case "CommandComplete":
		return &pgproto3.CommandComplete{}
	case "DataRow":
		return &pgproto3.DataRow{}
	case "ErrorResponse":
		return &pgproto3.ErrorResponse{}
	case "Execute":
		return &pgproto3.Execute{}
	case "Parse":
		return &pgproto3.Parse{}
	case "Query":
		return &pgproto3.Query{}
	case "ReadyForQuery":
		return &pgproto3.ReadyForQuery{}
	case "Sync":
		return &pgproto3.Sync{}
	default:
		panic(fmt.Errorf("unknown type %s", typ))
	}
}
