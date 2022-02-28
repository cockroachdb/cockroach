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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/datadriven"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/require"
)

// WalkWithRunningServer walks path for datadriven files and calls RunTest on them.
// It is used when an existing server is desired for each test.
func WalkWithRunningServer(t *testing.T, path, addr, user string) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		RunTest(t, path, addr, user)
	})
}

// WalkWithNewServer walks path for datadriven files and calls RunTest on them,
// but creates a new server for each test file.
func WalkWithNewServer(
	t *testing.T, path string, newServer func() (addr, user string, cleanup func()),
) {
	datadriven.Walk(t, path, func(t *testing.T, path string) {
		addr, user, cleanup := newServer()
		defer cleanup()
		RunTest(t, path, addr, user)
	})
}

// RunTest executes PGTest commands, connecting to the database specified by
// addr and user. Supported commands:
//
// "let": Run a query that returns a single row with a single column, and
// save it in the variable named in the command argument. This variable can
// be used in future "send" commands and is replaced by simple string
// substitution.
//
// "send": Sends messages to a server. Takes a newline-delimited list of
// pgproto3.FrontendMessage types. Can fill in values by adding a space then
// a JSON object. No output. Messages with a []byte type (like CopyData) should
// not base64 encode the data, instead use Go-escaped strings. If the input
// data actually has binary input or characters not allowed in JSON strings
// (like '\n'), then a base64-encoded value should be used as the `BinaryData`
// field.
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
//
// If the argument crdb_only is given and the server is non-crdb (e.g.
// postgres), then the exchange is skipped. With noncrdb_only, the inverse
// happens.
func RunTest(t *testing.T, path, addr, user string) {
	p, err := NewPGTest(context.Background(), addr, user)

	if err != nil {
		t.Fatal(err)
	}
	vars := make(map[string]string)
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "only":
			if d.HasArg("crdb") && !p.isCockroachDB {
				skip.IgnoreLint(t, "only crdb")
			}
			if d.HasArg("noncrdb") && p.isCockroachDB {
				skip.IgnoreLint(t, "only non-crdb")
			}
			return d.Expected

		case "let":
			require.Len(t, d.CmdArgs, 1, "only one argument permitted for let")
			require.Truef(t, strings.HasPrefix(d.CmdArgs[0].Key, "$"), "let argument must begin with '$'")
			lines := strings.Split(d.Input, "\n")
			require.Len(t, lines, 1, "only one input command permitted for let")
			require.Truef(t, strings.HasPrefix(lines[0], "Query "), "let must use a Query command")
			if err := p.SendOneLine(lines[0]); err != nil {
				t.Fatalf("%s: send %s: %v", d.Pos, lines[0], err)
			}
			msgs, err := p.Receive(hasKeepErrMsg(d), &pgproto3.DataRow{}, &pgproto3.ReadyForQuery{})
			if err != nil {
				t.Fatalf("%s: %+v", d.Pos, err)
			}
			sawData := false
			for _, msg := range msgs {
				if dataRow, ok := msg.(*pgproto3.DataRow); ok {
					require.False(t, sawData, "let Query must return only one row")
					require.Len(t, dataRow.Values, 1, "let Query must return only one column")
					sawData = true
					for _, arg := range d.CmdArgs {
						vars[arg.Key] = string(dataRow.Values[0])
					}
				}
			}
			return ""
		case "send":
			if (d.HasArg("crdb_only") && !p.isCockroachDB) ||
				(d.HasArg("noncrdb_only") && p.isCockroachDB) {
				return d.Expected
			}
			for _, line := range strings.Split(d.Input, "\n") {
				for k, v := range vars {
					line = strings.ReplaceAll(line, k, v)
				}
				if err := p.SendOneLine(line); err != nil {
					t.Fatalf("%s: send %s: %v", d.Pos, line, err)
				}
			}
			return ""
		case "receive":
			if (d.HasArg("crdb_only") && !p.isCockroachDB) ||
				(d.HasArg("noncrdb_only") && p.isCockroachDB) {
				return d.Expected
			}
			until := ParseMessages(d.Input)
			msgs, err := p.Receive(hasKeepErrMsg(d), until...)
			if err != nil {
				t.Fatalf("%s: %+v", d.Pos, err)
			}
			return MsgsToJSONWithIgnore(msgs, d)
		case "until":
			if (d.HasArg("crdb_only") && !p.isCockroachDB) ||
				(d.HasArg("noncrdb_only") && p.isCockroachDB) {
				return d.Expected
			}
			until := ParseMessages(d.Input)
			msgs, err := p.Until(hasKeepErrMsg(d), until...)
			if err != nil {
				t.Fatalf("%s: %+v", d.Pos, err)
			}
			return MsgsToJSONWithIgnore(msgs, d)
		default:
			t.Fatalf("unknown command %s", d.Cmd)
			return ""
		}
	})
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

// ParseMessages parses a string containing multiple pgproto3 messages separated
// by the newline symbol. See testdata for examples ("until" or "receive"
// commands).
func ParseMessages(s string) []pgproto3.BackendMessage {
	var msgs []pgproto3.BackendMessage
	for _, typ := range strings.Split(s, "\n") {
		msgs = append(msgs, toMessage(typ).(pgproto3.BackendMessage))
	}
	return msgs
}

func hasKeepErrMsg(d *datadriven.TestData) bool {
	for _, arg := range d.CmdArgs {
		if arg.Key == "keepErrMessage" {
			return true
		}
	}
	return false
}

// MsgsToJSONWithIgnore converts the pgproto3 messages to JSON format. The
// second argument can specify how to adjust the messages (e.g. to make them
// more deterministic) if needed, see testdata for examples.
func MsgsToJSONWithIgnore(msgs []pgproto3.BackendMessage, args *datadriven.TestData) string {
	ignore := map[string]bool{}
	errs := map[string]string{}
	for _, arg := range args.CmdArgs {
		switch arg.Key {
		case "keepErrMessage":
		case "crdb_only":
		case "noncrdb_only":
		case "ignore_table_oids":
			for _, msg := range msgs {
				if m, ok := msg.(*pgproto3.RowDescription); ok {
					for i := range m.Fields {
						m.Fields[i].TableOID = 0
					}
				}
			}
		case "ignore_type_oids":
			for _, msg := range msgs {
				if m, ok := msg.(*pgproto3.RowDescription); ok {
					for i := range m.Fields {
						m.Fields[i].DataTypeOID = 0
					}
				}
			}
		case "ignore_data_type_sizes":
			for _, msg := range msgs {
				if m, ok := msg.(*pgproto3.RowDescription); ok {
					for i := range m.Fields {
						m.Fields[i].DataTypeSize = 0
					}
				}
			}
		case "ignore_constraint_name":
			for _, msg := range msgs {
				if m, ok := msg.(*pgproto3.ErrorResponse); ok {
					m.ConstraintName = ""
				}
			}
		case "ignore":
			for _, typ := range arg.Vals {
				ignore[fmt.Sprintf("*pgproto3.%s", typ)] = true
			}
		case "mapError":
			errs[arg.Vals[0]] = arg.Vals[1]
		default:
			panic(fmt.Errorf("unknown argument: %v", arg))
		}
	}
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, msg := range msgs {
		if ignore[fmt.Sprintf("%T", msg)] {
			continue
		}
		if errmsg, ok := msg.(*pgproto3.ErrorResponse); ok {
			code := errmsg.Code
			if v, ok := errs[code]; ok {
				code = v
			}
			if err := enc.Encode(struct {
				Type           string
				Code           string
				Message        string `json:",omitempty"`
				ConstraintName string `json:",omitempty"`
			}{
				Type:           "ErrorResponse",
				Code:           code,
				Message:        errmsg.Message,
				ConstraintName: errmsg.ConstraintName,
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
	case "Close":
		return &pgproto3.Close{}
	case "CommandComplete":
		return &pgproto3.CommandComplete{}
	case "CopyData":
		return &pgproto3.CopyData{}
	case "CopyDone":
		return &pgproto3.CopyDone{}
	case "CopyInResponse":
		return &pgproto3.CopyInResponse{}
	case "DataRow":
		return &pgproto3.DataRow{}
	case "Describe":
		return &pgproto3.Describe{}
	case "ErrorResponse":
		return &pgproto3.ErrorResponse{}
	case "Execute":
		return &pgproto3.Execute{}
	case "Parse":
		return &pgproto3.Parse{}
	case "PortalSuspended":
		return &pgproto3.PortalSuspended{}
	case "Query":
		return &pgproto3.Query{}
	case "ReadyForQuery":
		return &pgproto3.ReadyForQuery{}
	case "Sync":
		return &pgproto3.Sync{}
	case "ParameterStatus":
		return &pgproto3.ParameterStatus{}
	case "BindComplete":
		return &pgproto3.BindComplete{}
	case "ParseComplete":
		return &pgproto3.ParseComplete{}
	case "RowDescription":
		return &pgproto3.RowDescription{}
	default:
		panic(fmt.Errorf("unknown type %q", typ))
	}
}
