// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package pgconnect provides a way to get byte encodings from a simple query.
package pgconnect

import (
	"context"
	"net"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgproto3/v2"
)

// Connect connects to the postgres-compatible server at addr with specified
// user. input must specify a SELECT query (including the "SELECT") that
// returns one row and one column. code is the format code. The resulting
// row bytes are returned.
func Connect(
	ctx context.Context, input, addr, user string, code pgwirebase.FormatCode,
) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "dail")
	}
	defer conn.Close()

	fe := pgproto3.NewFrontend(pgproto3.NewChunkReader(conn), conn)

	send := make(chan pgproto3.FrontendMessage)
	recv := make(chan pgproto3.BackendMessage)
	var res []byte
	// Use go routines to divide up work in order to improve debugging. These
	// aren't strictly necessary, but they make it easy to print when messages
	// are received.
	g := ctxgroup.WithContext(ctx)
	// The send chan sends messages to the server.
	g.GoCtx(func(ctx context.Context) error {
		defer close(send)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msg := <-send:
				err := fe.Send(msg)
				if err != nil {
					return errors.Wrap(err, "send")
				}
			}
		}
	})
	// The recv go routine receives messages from the server and puts them on
	// the recv chan. It makes a copy of them when it does since the next message
	// received will otherwise use the same pointer.
	g.GoCtx(func(ctx context.Context) error {
		defer close(recv)
		for {
			msg, err := fe.Receive()
			if err != nil {
				return errors.Wrap(err, "receive")
			}

			// Make a deep copy since the receiver uses a pointer.
			x := reflect.ValueOf(msg)
			starX := x.Elem()
			y := reflect.New(starX.Type())
			starY := y.Elem()
			starY.Set(starX)
			dup := y.Interface().(pgproto3.BackendMessage)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case recv <- dup:
			}
		}
	})
	// The main go routine executing the logic.
	g.GoCtx(func(ctx context.Context) error {
		send <- &pgproto3.StartupMessage{
			ProtocolVersion: 196608, // Version 3.0
			Parameters: map[string]string{
				"user": user,
			},
		}
		{
			r := <-recv
			if _, ok := r.(*pgproto3.AuthenticationOk); !ok {
				return errors.Errorf("unexpected: %#v\n", r)
			}
		}
	WaitConnLoop:
		for {
			msg := <-recv
			switch msg.(type) {
			case *pgproto3.ReadyForQuery:
				break WaitConnLoop
			}
		}
		send <- &pgproto3.Parse{
			Query: input,
		}
		send <- &pgproto3.Describe{
			ObjectType: 'S',
		}
		send <- &pgproto3.Sync{}
		r := <-recv
		if _, ok := r.(*pgproto3.ParseComplete); !ok {
			return errors.Errorf("unexpected: %#v", r)
		}
		send <- &pgproto3.Bind{
			ResultFormatCodes: []int16{int16(code)},
		}
		send <- &pgproto3.Execute{}
		send <- &pgproto3.Sync{}
	WaitExecuteLoop:
		for {
			msg := <-recv
			switch msg := msg.(type) {
			case *pgproto3.DataRow:
				if res != nil {
					return errors.New("already saw a row")
				}
				if len(msg.Values) != 1 {
					return errors.Errorf("unexpected: %#v\n", msg)
				}
				res = msg.Values[0]
			case *pgproto3.CommandComplete,
				*pgproto3.EmptyQueryResponse,
				*pgproto3.ErrorResponse:
				break WaitExecuteLoop
			}
		}
		// Stop the other go routines.
		cancel()
		return nil
	})
	err = g.Wait()
	// If res is set, we don't care about any errors.
	if res != nil {
		return res, nil
	}
	if err == nil {
		return nil, errors.New("unexpected")
	}
	return nil, err
}
