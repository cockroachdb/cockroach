// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package codec

import (
	"errors"
	"net"
	"net/rpc"
	"testing"

	// can not import xxx.pb with rpc stub here,
	// because it will cause import cycle.
	msg "github.com/cockroachdb/cockroach/rpc/codec/message.pb"
	"github.com/cockroachdb/cockroach/util/log"
)

type Arith int

func (t *Arith) Add(args *msg.ArithRequest, reply *msg.ArithResponse) error {
	reply.C = args.GetA() + args.GetB()
	log.Infof("Arith.Add(%v, %v): %v", args.GetA(), args.GetB(), reply.GetC())
	return nil
}

func (t *Arith) Mul(args *msg.ArithRequest, reply *msg.ArithResponse) error {
	reply.C = args.GetA() * args.GetB()
	return nil
}

func (t *Arith) Div(args *msg.ArithRequest, reply *msg.ArithResponse) error {
	if args.GetB() == 0 {
		return errors.New("divide by zero")
	}
	reply.C = args.GetA() / args.GetB()
	return nil
}

func (t *Arith) Error(args *msg.ArithRequest, reply *msg.ArithResponse) error {
	return errors.New("ArithError")
}

type Echo int

func (t *Echo) Echo(args *msg.EchoRequest, reply *msg.EchoResponse) error {
	reply.Msg = args.Msg
	return nil
}

func TestAll(t *testing.T) {
	srvAddr, err := listenAndServeArithAndEchoService("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("could not start server")
	}
	conn, err := net.Dial(srvAddr.Network(), srvAddr.String())
	if err != nil {
		t.Fatalf("could not dial client to %s: %s", srvAddr, err)
	}
	client := rpc.NewClientWithCodec(NewClientCodec(conn))
	defer client.Close()

	testArithClient(t, client)
	testEchoClient(t, client)

	testArithClientAsync(t, client)
	testEchoClientAsync(t, client)
}

func listenAndServeArithAndEchoService(network, addr string) (net.Addr, error) {
	clients, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}
	srv := rpc.NewServer()
	if err := srv.RegisterName("ArithService", new(Arith)); err != nil {
		return nil, err
	}
	if err := srv.RegisterName("EchoService", new(Echo)); err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := clients.Accept()
			if err != nil {
				log.Infof("clients.Accept(): %v\n", err)
				continue
			}
			go srv.ServeCodec(NewServerCodec(conn))
		}
	}()
	return clients.Addr(), nil
}

func testArithClient(t *testing.T, client *rpc.Client) {
	var args msg.ArithRequest
	var reply msg.ArithResponse
	var err error

	// Add
	args.A = 1
	args.B = 2
	if err = client.Call("ArithService.Add", &args, &reply); err != nil {
		t.Fatalf(`arith.Add: %v`, err)
	}
	if reply.GetC() != 3 {
		t.Fatalf(`arith.Add: expected = %d, got = %d`, 3, reply.GetC())
	}

	// Mul
	args.A = 2
	args.B = 3
	if err = client.Call("ArithService.Mul", &args, &reply); err != nil {
		t.Fatalf(`arith.Mul: %v`, err)
	}
	if reply.GetC() != 6 {
		t.Fatalf(`arith.Mul: expected = %d, got = %d`, 6, reply.GetC())
	}

	// Div
	args.A = 13
	args.B = 5
	if err = client.Call("ArithService.Div", &args, &reply); err != nil {
		t.Fatalf(`arith.Div: %v`, err)
	}
	if reply.GetC() != 2 {
		t.Fatalf(`arith.Div: expected = %d, got = %d`, 2, reply.GetC())
	}

	// Div zero
	args.A = 1
	args.B = 0
	if err = client.Call("ArithService.Div", &args, &reply); err.Error() != "divide by zero" {
		t.Fatalf(`arith.Error: expected = "%s", got = "%s"`, "divide by zero", err.Error())
	}

	// Error
	args.A = 1
	args.B = 2
	if err = client.Call("ArithService.Error", &args, &reply); err.Error() != "ArithError" {
		t.Fatalf(`arith.Error: expected = "%s", got = "%s"`, "ArithError", err.Error())
	}
}

func testArithClientAsync(t *testing.T, client *rpc.Client) {
	done := make(chan *rpc.Call, 16)
	callInfoList := []struct {
		method string
		args   *msg.ArithRequest
		reply  *msg.ArithResponse
		err    error
	}{
		{
			"ArithService.Add",
			&msg.ArithRequest{A: 1, B: 2},
			&msg.ArithResponse{C: 3},
			nil,
		},
		{
			"ArithService.Mul",
			&msg.ArithRequest{A: 2, B: 3},
			&msg.ArithResponse{C: 6},
			nil,
		},
		{
			"ArithService.Div",
			&msg.ArithRequest{A: 13, B: 5},
			&msg.ArithResponse{C: 2},
			nil,
		},
		{
			"ArithService.Div",
			&msg.ArithRequest{A: 1, B: 0},
			&msg.ArithResponse{},
			errors.New("divide by zero"),
		},
		{
			"ArithService.Error",
			&msg.ArithRequest{A: 1, B: 2},
			&msg.ArithResponse{},
			errors.New("ArithError"),
		},
	}

	// GoCall list
	calls := make([]*rpc.Call, len(callInfoList))
	for i := 0; i < len(calls); i++ {
		calls[i] = client.Go(callInfoList[i].method,
			callInfoList[i].args, callInfoList[i].reply,
			done,
		)
	}
	for i := 0; i < len(calls); i++ {
		<-calls[i].Done
	}

	// check result
	for i := 0; i < len(calls); i++ {
		if callInfoList[i].err != nil {
			if calls[i].Error.Error() != callInfoList[i].err.Error() {
				t.Fatalf(`%s: expected %v, Got = %v`,
					callInfoList[i].method,
					callInfoList[i].err,
					calls[i].Error,
				)
			}
			continue
		}

		got := calls[i].Reply.(*msg.ArithResponse).GetC()
		expected := callInfoList[i].reply.GetC()
		if got != expected {
			t.Fatalf(`%d: expected %v, Got = %v`,
				callInfoList[i].method, got, expected,
			)
		}
	}
}

func testEchoClient(t *testing.T, client *rpc.Client) {
	var args msg.EchoRequest
	var reply msg.EchoResponse
	var err error

	// EchoService.Echo
	args.Msg = "Hello, Protobuf-RPC"
	if err = client.Call("EchoService.Echo", &args, &reply); err != nil {
		t.Fatalf(`EchoService.Echo: %v`, err)
	}
	if reply.GetMsg() != args.GetMsg() {
		t.Fatalf(`EchoService.Echo: expected = "%s", got = "%s"`, args.GetMsg(), reply.GetMsg())
	}
}

func testEchoClientAsync(t *testing.T, client *rpc.Client) {
	// EchoService.Echo
	args := &msg.EchoRequest{Msg: "Hello, Protobuf-RPC"}
	reply := &msg.EchoResponse{}
	echoCall := client.Go("EchoService.Echo", args, reply, nil)

	// EchoService.Echo reply
	echoCall = <-echoCall.Done
	if echoCall.Error != nil {
		t.Fatalf(`EchoService.Echo: %v`, echoCall.Error)
	}
	if echoCall.Reply.(*msg.EchoResponse).GetMsg() != args.GetMsg() {
		t.Fatalf(`EchoService.Echo: expected = "%s", got = "%s"`,
			args.GetMsg(),
			echoCall.Reply.(*msg.EchoResponse).GetMsg(),
		)
	}
}
