// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package pgconnect provides a way to get byte encodings from a simple query.
package pgconnect

import (
	"context"
	"net"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/jackc/pgx/pgproto3"
	"github.com/pkg/errors"
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

	fe, err := pgproto3.NewFrontend(conn, conn)
	if err != nil {
		return nil, errors.Wrap(err, "new frontend")
	}

	send := make(chan pgproto3.FrontendMessage)
	recv := make(chan pgproto3.BackendMessage)
	var res []byte
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer close(send)
		for {
			select {
			case <-g.Done:
				return g.Err()
			case msg := <-send:
				err := fe.Send(msg)
				if err != nil {
					return errors.Wrap(err, "send")
				}
			}
		}
	})
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
			case <-g.Done:
				return g.Err()
			case recv <- dup:
			}
		}
	})
	g.GoCtx(func(ctx context.Context) error {
		send <- &pgproto3.StartupMessage{
			ProtocolVersion: 196608,
			Parameters: map[string]string{
				"user": user,
			},
		}
		{
			r := <-recv
			if msg, ok := r.(*pgproto3.Authentication); !ok || msg.Type != 0 {
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
