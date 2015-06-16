// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package codec

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/rpc"
	"testing"

	// can not import xxx.pb with rpc stub here,
	// because it will cause import cycle.

	msg "github.com/cockroachdb/cockroach/rpc/codec/message.pb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/gogo/protobuf/proto"
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
			t.Fatalf(`%s: expected %v, Got = %v`,
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

func randString(n int) string {
	var randLetters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789#$")
	return string(bytes.Repeat(randLetters, n/len(randLetters)))
}

func listenAndServeEchoService(network, addr string,
	serveConn func(srv *rpc.Server, conn io.ReadWriteCloser)) (net.Listener, error) {
	l, err := net.Listen(network, addr)
	if err != nil {
		fmt.Printf("failed to listen on %s: %s\n", addr, err)
		return nil, err
	}
	srv := rpc.NewServer()
	if err := srv.RegisterName("EchoService", new(Echo)); err != nil {
		return nil, err
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Infof("accept: %v\n", err)
				break
			}
			serveConn(srv, conn)
		}
	}()

	if *onlyEchoServer {
		select {}
	}
	return l, nil
}

func benchmarkEcho(b *testing.B, size int, newClient func() *rpc.Client) {
	echoMsg := randString(size)

	b.SetBytes(2 * int64(len(echoMsg)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		client := newClient()
		defer client.Close()

		for pb.Next() {
			args := &msg.EchoRequest{Msg: echoMsg}
			reply := &msg.EchoResponse{}
			if err := client.Call("EchoService.Echo", args, reply); err != nil {
				b.Fatalf(`EchoService.Echo: %v`, err)
			}
		}
	})

	b.StopTimer()
}

var echoAddr = flag.String("echo-addr", "127.0.0.1:0",
	"host:port to bind for the echo server used in benchmarks")
var startEchoServer = flag.Bool("start-echo-server", true,
	"start the echo server; false to connect to an already running server")
var onlyEchoServer = flag.Bool("only-echo-server", false,
	"only run the echo server; looping forever")

// To run these benchmarks between machines, on machine 1 start the
// echo server:
//
//   go test -run= -bench=BenchmarkEchoGobRPC -echoAddr :9999 -only-echo-server
//
// On machine 2:
//
//   go test -run= -bench=BenchmarkEchoGobRPC -echoAddr <machine-1-ip>:9999 -start-echo-server=false

func benchmarkEchoGobRPC(b *testing.B, size int) {
	var addr string
	if *startEchoServer {
		l, err := listenAndServeEchoService("tcp", *echoAddr,
			func(srv *rpc.Server, conn io.ReadWriteCloser) {
				go srv.ServeConn(conn)
			})
		if err != nil {
			b.Fatal("could not start server")
		}
		defer l.Close()
		addr = l.Addr().String()
	} else {
		addr = *echoAddr
	}

	benchmarkEcho(b, size, func() *rpc.Client {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatalf("could not dial client to %s: %s", addr, err)
		}
		return rpc.NewClient(conn)
	})
}

func BenchmarkEchoGobRPC1K(b *testing.B) {
	benchmarkEchoGobRPC(b, 1<<10)
}

func BenchmarkEchoGobRPC64K(b *testing.B) {
	benchmarkEchoGobRPC(b, 64<<10)
}

func benchmarkEchoProtoRPC(b *testing.B, size int) {
	var addr string
	if *startEchoServer {
		l, err := listenAndServeEchoService("tcp", *echoAddr,
			func(srv *rpc.Server, conn io.ReadWriteCloser) {
				go srv.ServeCodec(NewServerCodec(conn))
			})
		if err != nil {
			b.Fatal("could not start server")
		}
		defer l.Close()
		addr = l.Addr().String()
	} else {
		addr = *echoAddr
	}

	benchmarkEcho(b, size, func() *rpc.Client {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatalf("could not dial client to %s: %s", addr, err)
		}
		return rpc.NewClientWithCodec(NewClientCodec(conn))
	})
}

func BenchmarkEchoProtoRPC1K(b *testing.B) {
	benchmarkEchoProtoRPC(b, 1<<10)
}

func BenchmarkEchoProtoRPC64K(b *testing.B) {
	benchmarkEchoProtoRPC(b, 64<<10)
}

func benchmarkEchoProtoHTTP(b *testing.B, size int) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()

	go func() {
		if err := http.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqBody, err := ioutil.ReadAll(r.Body)
			defer r.Body.Close()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			req := &msg.EchoRequest{}
			if err := proto.Unmarshal(reqBody, req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			resp := &msg.EchoResponse{Msg: req.Msg}
			body, err := proto.Marshal(resp)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set(util.ContentTypeHeader, util.ProtoContentType)
			w.Write(body)
		})); err != nil {
			b.Fatal(err)
		}
	}()

	echoMsg := randString(size)
	url := fmt.Sprintf("http://%s", l.Addr())

	b.SetBytes(2 * int64(len(echoMsg)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			args := &msg.EchoRequest{Msg: echoMsg}
			body, err := proto.Marshal(args)
			if err != nil {
				b.Fatal(err)
			}
			resp, err := http.Post(url, util.ProtoContentType, bytes.NewReader(body))
			if err != nil {
				b.Fatalf("%s: %v", url, err)
			}
			defer resp.Body.Close()

			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				b.Fatal(err)
			}
			reply := &msg.EchoResponse{}
			if err := proto.Unmarshal(body, reply); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.StopTimer()
}

func BenchmarkEchoProtoHTTP1K(b *testing.B) {
	benchmarkEchoProtoHTTP(b, 1<<10)
}

func BenchmarkEchoProtoHTTP64K(b *testing.B) {
	benchmarkEchoProtoHTTP(b, 64<<10)
}
