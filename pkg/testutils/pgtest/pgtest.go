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
	"net"
	"reflect"

	"github.com/jackc/pgx/pgproto3"
	"github.com/pkg/errors"
)

// PGTest can be used to send and receive arbitrary pgwire messages on
// Postgres-compatible servers.
type PGTest struct {
	fe   *pgproto3.Frontend
	conn net.Conn
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
	fe, err := pgproto3.NewFrontend(conn, conn)
	if err != nil {
		return nil, errors.Wrap(err, "new frontend")
	}
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
	} else if auth, ok := msg.(*pgproto3.Authentication); !ok || auth.Type != 0 {
		return nil, errors.Errorf("unexpected: %#v", msg)
	}
	p := &PGTest{
		fe:   fe,
		conn: conn,
	}
	_, err = p.Until(&pgproto3.ReadyForQuery{})
	success = err == nil
	return p, err
}

// Close sends a Terminate message and closes the connection.
func (p *PGTest) Close() error {
	defer p.conn.Close()
	return p.fe.Send(&pgproto3.Terminate{})
}

// Send sends msg to the serrver.
func (p *PGTest) Send(msg pgproto3.FrontendMessage) error {
	return p.fe.Send(msg)
}

// Receive reads messages until messages of the given types have been found
// in the specified order (with any number of messages in between). It returns
// matched messages.
func (p *PGTest) Receive(typs ...pgproto3.BackendMessage) ([]pgproto3.BackendMessage, error) {
	var matched []pgproto3.BackendMessage
	for len(typs) > 0 {
		msgs, err := p.Until(typs[0])
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
func (p *PGTest) Until(typs ...pgproto3.BackendMessage) ([]pgproto3.BackendMessage, error) {
	var msgs []pgproto3.BackendMessage
	for len(typs) > 0 {
		typ := reflect.TypeOf(typs[0])

		// Receive messages and make copies of them.
		recv, err := p.fe.Receive()
		if err != nil {
			return nil, errors.Wrap(err, "receive")
		}
		if errmsg, ok := recv.(*pgproto3.ErrorResponse); ok && typ != typErrorResponse {
			return nil, errors.Errorf("waiting for %T, got %#v", typs[0], errmsg)
		}
		// If we saw a ready message but weren't waiting for one, we
		// might wait forever so bail.
		if msg, ok := recv.(*pgproto3.ReadyForQuery); ok && typ != typReadyForQuery {
			return nil, errors.Errorf("waiting for %T, got %#v", typs[0], msg)
		}
		data := recv.Encode(nil)
		// Trim off message type and length.
		data = data[5:]

		x := reflect.New(reflect.ValueOf(recv).Elem().Type())
		msg := x.Interface().(pgproto3.BackendMessage)
		if err := msg.Decode(data); err != nil {
			return nil, errors.Wrap(err, "decode")
		}
		msgs = append(msgs, msg)
		if typ == reflect.TypeOf(msg) {
			typs = typs[1:]
		}
	}
	return msgs, nil
}

var (
	typErrorResponse = reflect.TypeOf(&pgproto3.ErrorResponse{})
	typReadyForQuery = reflect.TypeOf(&pgproto3.ReadyForQuery{})
)
