// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgtest

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

// PGTest can be used to send and receive arbitrary pgwire messages on
// Postgres-compatible servers.
type PGTest struct {
	fe            *pgproto3.Frontend
	conn          net.Conn
	isCockroachDB bool
}

// NewPGTest connects to a Postgres server at addr with username user.
func NewPGTest(ctx context.Context, addr, user string) (*PGTest, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "dial")
	}
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()
	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)
	if err := fe.Send(&pgproto3.StartupMessage{
		ProtocolVersion: 196608, // Version 3.0
		Parameters: map[string]string{
			"user": user,
		},
	}); err != nil {
		return nil, errors.Wrap(err, "startup")
	}
	if msg, err := fe.Receive(); err != nil {
		return nil, errors.Wrap(err, "receive")
	} else if _, ok := msg.(*pgproto3.AuthenticationOk); !ok {
		return nil, errors.Errorf("unexpected: %#v", msg)
	}
	p := &PGTest{
		fe:   fe,
		conn: conn,
	}
	msgs, err := p.Until(false /* keepErrMsg */, &pgproto3.ReadyForQuery{})
	foundCrdb := false
	var backendKeyData *pgproto3.BackendKeyData
	for _, msg := range msgs {
		if s, ok := msg.(*pgproto3.ParameterStatus); ok && s.Name == "crdb_version" {
			foundCrdb = true
		}
		if d, ok := msg.(*pgproto3.BackendKeyData); ok {
			backendKeyData = d
		}
	}
	if backendKeyData == nil {
		return nil, errors.Errorf("did not receive BackendKeyData")
	}
	if err := checkPGBackendPID(p, backendKeyData); err != nil {
		return nil, err
	}

	p.isCockroachDB = foundCrdb
	success = err == nil
	return p, err
}

func checkPGBackendPID(p *PGTest, backendKeyData *pgproto3.BackendKeyData) error {
	if err := p.fe.Send(&pgproto3.Query{
		String: "SELECT pg_backend_pid();",
	}); err != nil {
		return errors.Wrap(err, "fetching pg_backend_pid")
	}
	msgs, err := p.Until(false /* keepErrMsg */, &pgproto3.ReadyForQuery{})
	if err != nil {
		return errors.Wrap(err, "fetching pg_backend_pid")
	}
	matched := false
	for _, msg := range msgs {
		if d, ok := msg.(*pgproto3.DataRow); ok {
			pid, err := strconv.Atoi(string(d.Values[0]))
			if err != nil {
				return errors.Wrap(err, "parsing pg_backend_pid")
			}
			if uint32(pid) != backendKeyData.ProcessID {
				return errors.Errorf("wrong pg_backend_pid; wanted %d, got %d", backendKeyData.ProcessID, pid)
			}
			matched = true
		}
	}
	if !matched {
		return errors.Errorf("could not retrieve pg_backend_pid")
	}
	return nil
}

// Close sends a Terminate message and closes the connection.
func (p *PGTest) Close() error {
	defer p.conn.Close()
	return p.fe.Send(&pgproto3.Terminate{})
}

// SendOneLine sends a single msg to the server represented as a single string
// in the format `<msg type> <msg body in JSON>`. See testdata for examples.
func (p *PGTest) SendOneLine(line string) error {
	sp := strings.SplitN(line, " ", 2)
	msg := toMessage(sp[0])
	if len(sp) == 2 {
		msgBytes := []byte(sp[1])
		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			var data struct {
				Data       string
				BinaryData []byte
			}
			if err := json.Unmarshal(msgBytes, &data); err != nil {
				return err
			}
			if data.BinaryData != nil {
				msg.Data = data.BinaryData
			} else {
				msg.Data = []byte(data.Data)
			}
		default:
			if err := json.Unmarshal(msgBytes, msg); err != nil {
				return err
			}
		}
	}
	return p.Send(msg.(pgproto3.FrontendMessage))
}

// Send sends msg to the server.
func (p *PGTest) Send(msg pgproto3.FrontendMessage) error {
	if testing.Verbose() {
		fmt.Printf("SEND %T: %+[1]v\n", msg)
	}
	return p.fe.Send(msg)
}

// Receive reads messages until messages of the given types have been found
// in the specified order (with any number of messages in between). It returns
// matched messages.
func (p *PGTest) Receive(
	keepErrMsg bool, typs ...pgproto3.BackendMessage,
) ([]pgproto3.BackendMessage, error) {
	var matched []pgproto3.BackendMessage
	for len(typs) > 0 {
		msgs, err := p.Until(keepErrMsg, typs[0])
		if err != nil {
			return nil, err
		}
		matched = append(matched, msgs[len(msgs)-1])
		typs = typs[1:]
	}
	return matched, nil
}

// Until is like Receive except all messages are returned instead of only
// matched messages.
func (p *PGTest) Until(
	keepErrMsg bool, typs ...pgproto3.BackendMessage,
) ([]pgproto3.BackendMessage, error) {
	var msgs []pgproto3.BackendMessage
	for len(typs) > 0 {
		typ := reflect.TypeOf(typs[0])

		// Receive messages and make copies of them.
		recv, err := p.fe.Receive()
		if err != nil {
			return nil, errors.Wrap(err, "receive")
		}
		if testing.Verbose() {
			fmt.Printf("RECV %T: %+[1]v\n", recv)
		}
		if errmsg, ok := recv.(*pgproto3.ErrorResponse); ok {
			if typ != typErrorResponse {
				return nil, errors.Errorf("waiting for %T, got %#v", typs[0], errmsg)
			}
			var message, detail, hint string
			if keepErrMsg {
				message = errmsg.Message
				detail = errmsg.Detail
				hint = errmsg.Hint
			}
			// ErrorResponse doesn't encode/decode correctly, so
			// manually append it here.
			msgs = append(msgs, &pgproto3.ErrorResponse{
				Code:           errmsg.Code,
				Message:        message,
				ConstraintName: errmsg.ConstraintName,
				Detail:         detail,
				Hint:           hint,
			})
			typs = typs[1:]
			continue
		}
		// If we saw a ready message but weren't waiting for one, we
		// might wait forever so bail.
		if msg, ok := recv.(*pgproto3.ReadyForQuery); ok && typ != typReadyForQuery {
			return nil, errors.Errorf("waiting for %T, got %#v", typs[0], msg)
		}
		if typ == reflect.TypeOf(recv) {
			typs = typs[1:]
		}

		// recv is a pointer to some union'd interface. The next call
		// to p.fe.Receive with the same message type will overwrite
		// the previous message. We thus need to copy recv into some
		// new variable. In the past we have used the BackendMessage's
		// Encode/Decode methods, but those are sometimes
		// broken. Instead, go through gob.
		var buf bytes.Buffer
		rv := reflect.ValueOf(recv).Elem()
		x := reflect.New(rv.Type())
		if err := gob.NewEncoder(&buf).EncodeValue(rv); err != nil {
			return nil, err
		}
		if err := gob.NewDecoder(&buf).DecodeValue(x); err != nil {
			return nil, err
		}
		msg := x.Interface().(pgproto3.BackendMessage)
		if notice, ok := msg.(*pgproto3.NoticeResponse); ok {
			// The line number can change frequently, so to reduce churn, we always
			// ignore it.
			notice.Line = 0
			msgs = append(msgs, notice)
			continue
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

var (
	typErrorResponse = reflect.TypeOf(&pgproto3.ErrorResponse{})
	typReadyForQuery = reflect.TypeOf(&pgproto3.ReadyForQuery{})
)
