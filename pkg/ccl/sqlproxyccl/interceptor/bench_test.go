// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package interceptor_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/jackc/chunkreader/v2"
	"github.com/jackc/pgproto3/v2"
)

type connCopyType func(dst io.ReadWriter, src io.ReadWriter) (int64, error)

// BenchmarkConnectionCopy is used to benchmark various approaches that can be
// used to copy packets from the client to the server, and vice versa.
//
// go version go1.17.3 linux/amd64
// ---
// goos: linux
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/ccl/sqlproxyccl/interceptor
// cpu: AMD Ryzen 9 5950X 16-Core Processor
// BenchmarkConnectionCopy/msgCount=10000/msgLen=170/io.Copy-32 	     	      385	   3538410 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=170/io.CopyN-32         	      15	  75036404 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=170/pgproto3-32         	      26	  42251150 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=170/chunkreader-32      	      18	  65000047 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=170/interceptor-32      	      33	  39125341 ns/op
//
// BenchmarkConnectionCopy/msgCount=500/msgLen=320/io.Copy-32            	    3204	    406888 ns/op
// BenchmarkConnectionCopy/msgCount=500/msgLen=320/io.CopyN-32           	     410	   3123298 ns/op
// BenchmarkConnectionCopy/msgCount=500/msgLen=320/pgproto3-32           	     577	   2387535 ns/op
// BenchmarkConnectionCopy/msgCount=500/msgLen=320/chunkreader-32        	     469	   3565173 ns/op
// BenchmarkConnectionCopy/msgCount=500/msgLen=320/interceptor-32        	     652	   2015079 ns/op
//
// BenchmarkConnectionCopy/msgCount=9000/msgLen=2900/io.Copy-32          	      49	  23330567 ns/op
// BenchmarkConnectionCopy/msgCount=9000/msgLen=2900/io.CopyN-32         	      18	  72003323 ns/op
// BenchmarkConnectionCopy/msgCount=9000/msgLen=2900/pgproto3-32         	      15	  82500818 ns/op
// BenchmarkConnectionCopy/msgCount=9000/msgLen=2900/chunkreader-32      	      18	  79832494 ns/op
// BenchmarkConnectionCopy/msgCount=9000/msgLen=2900/interceptor-32      	      20	  54727023 ns/op
//
// BenchmarkConnectionCopy/msgCount=5000/msgLen=30000/io.Copy-32         	      12	  98640876 ns/op
// BenchmarkConnectionCopy/msgCount=5000/msgLen=30000/io.CopyN-32        	      10	 110690053 ns/op
// BenchmarkConnectionCopy/msgCount=5000/msgLen=30000/pgproto3-32        	       6	 177894915 ns/op
// BenchmarkConnectionCopy/msgCount=5000/msgLen=30000/chunkreader-32     	       9	 129588686 ns/op
// BenchmarkConnectionCopy/msgCount=5000/msgLen=30000/interceptor-32     	       9	 112610362 ns/op
//
// BenchmarkConnectionCopy/msgCount=10000/msgLen=10/io.Copy-32           	     591	   2274817 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=10/io.CopyN-32          	      25	  47465099 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=10/pgproto3-32          	      58	  23077900 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=10/chunkreader-32       	      38	  31459201 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=10/interceptor-32       	      64	  17616468 ns/op
//
// BenchmarkConnectionCopy/msgCount=10000/msgLen=15/io.Copy-32           	     531	   2336896 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=15/io.CopyN-32          	      30	  45135200 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=15/pgproto3-32          	      51	  22100293 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=15/chunkreader-32       	      49	  28931167 ns/op
// BenchmarkConnectionCopy/msgCount=10000/msgLen=15/interceptor-32       	      66	  15189020 ns/op
func BenchmarkConnectionCopy(b *testing.B) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	runSql := func() (net.Listener, error) {
		ln, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			return nil, err
		}
		err = stopper.RunAsyncTask(ctx, "sql-quiesce", func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			if err := ln.Close(); err != nil && !grpcutil.IsClosedConnection(err) {
				log.Fatalf(ctx, "closing sql listener: %s", err)
			}
		})
		if err != nil {
			return nil, err
		}
		server := newTestBmServer(ctx, stopper, sqlServerHandler)
		go func() {
			if err := server.serve(ctx, ln); err != nil {
				panic(err)
			}
		}()
		return ln, nil
	}

	runProxy := func(
		sqlAddr string,
		connCopyClientFn, connCopyServerFn connCopyType,
	) (net.Listener, error) {
		ln, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			return nil, err
		}
		err = stopper.RunAsyncTask(ctx, "proxy-quiesce", func(ctx context.Context) {
			<-stopper.ShouldQuiesce()
			if err := ln.Close(); err != nil && !grpcutil.IsClosedConnection(err) {
				log.Fatalf(ctx, "closing proxy listener: %s", err)
			}
		})
		if err != nil {
			return nil, err
		}
		server := newTestBmServer(
			ctx,
			stopper,
			makeProxyServerHandler(sqlAddr, connCopyClientFn, connCopyServerFn),
		)
		go func() {
			if err := server.serve(ctx, ln); err != nil {
				panic(err)
			}
		}()
		return ln, nil
	}

	sqlLn, err := runSql()
	if err != nil {
		b.Fatal(err)
	}

	// Define a list of implementations.
	impls := []struct {
		name           string
		connCopyClient connCopyType
		connCopyServer connCopyType
	}{
		{"io.Copy", connCopyWithIOCopy, connCopyWithIOCopy},
		{"io.CopyN", connCopyWithIOCopyN, connCopyWithIOCopyN},
		{"pgproto3", connCopyWithPGProto3_Client, connCopyWithPGProto3_Server},
		{"chunkreader", connCopyWithChunkReader, connCopyWithChunkReader},
		{"interceptor", connCopyWithInterceptor_Client, connCopyWithInterceptor_Server},
	}

	// These configuration values were determined by measuring the average
	// message size for pgwire messages sent by both client and server for
	// various workloads. The measurements were made over a period of 30s.
	for _, bm := range []struct {
		msgCount int
		msgLen   int
	}{
		{10000, 170},  // tpch - sent by server
		{500, 320},    // tpch - sent by client
		{9000, 2900},  // tpcc - sent by server
		{5000, 30000}, // tpcc - sent by client
		// Note: for kv specifically, there were > 1.8M messages over a period
		// of 30s, so we just sampled the first 10K.
		{10000, 10}, // kv - sent by server
		{10000, 15}, // kv - sent by client
	} {
		b.Run(fmt.Sprintf("msgCount=%d/msgLen=%d", bm.msgCount, bm.msgLen), func(b *testing.B) {
			// Add 5 bytes to include the last ClientMsgTerminate message.
			totalBytes := int64(bm.msgCount*bm.msgLen + 5)

			for _, impl := range impls {
				b.Run(impl.name, func(b *testing.B) {
					proxyLn, err := runProxy(
						sqlLn.Addr().String(),
						impl.connCopyClient,
						impl.connCopyServer,
					)
					if err != nil {
						b.Fatal(err)
					}
					defer func() { _ = proxyLn.Close() }()

					conn, err := net.Dial("tcp", proxyLn.Addr().String())
					if err != nil {
						panic(err)
					}
					defer func() { _ = conn.Close() }()

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						src, err := generateSrc(bm.msgCount, bm.msgLen)
						if err != nil {
							b.Fatal(err)
						}
						// Mimic client sending packets.
						if _, err := io.CopyN(conn, src, int64(src.Len())); err != nil {
							b.Fatal(err)
						}
						res, err := readBytesWritten(conn)
						if err != nil {
							b.Fatal(err)
						}
						if res != totalBytes {
							b.Fatalf("sent %d bytes, received %d", totalBytes, res)
						}
					}
				})
			}
		})
	}

	// Terminate process. We have to do a force kill here because all the
	// readers are blocked waiting for packets, and we didn't want to make the
	// connection a context-aware one.
	os.Exit(1)
}

// randStringN generates a random string with length n.
func randStringN(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	// Convert from slice to string directly to avoid allocations and copying.
	return *((*string)(unsafe.Pointer(&b)))
}

// generateSrc generates a src buffer with size = msgLen * msgCount. Note that
// msgLen requires a minimum length of 6 since we use pgproto3.Query under the
// hood.
func generateSrc(msgCount int, msgLen int) (*bytes.Buffer, error) {
	if msgLen < 6 {
		return nil, errors.AssertionFailedf("invalid msgLen: less than 6")
	}

	str := randStringN(msgLen - 6)
	src := new(bytes.Buffer)

	// All Query message types have at least 6 bytes.
	//     <type:1><len:4><NUL:1>
	for i := 0; i < msgCount; i++ {
		msg := &pgproto3.Query{String: str}
		if _, err := src.Write(msg.Encode(nil)); err != nil {
			return nil, err
		}
	}

	var result [5]byte // 1 + 4
	result[0] = byte(pgwirebase.ClientMsgTerminate)
	binary.BigEndian.PutUint32(result[1:5], uint32(4))
	if _, err := src.Write(result[:]); err != nil {
		return nil, err
	}

	// Assert that the total number of bytes is correct.
	expected := msgCount*msgLen + 5
	if src.Len() != expected {
		return nil, errors.Newf("bytesCount mismatch: generated=%d, expected=%d",
			src.Len(), expected)
	}

	return src, nil
}

// connCopyWithIOCopy represents the old approach of forwarding pgwire messages,
// without the connection migration work. As the name suggests, this uses
// io.Copy under the hood. For the TCP case, we take the fast-path, so there
// won't be allocations and copying:
// https://cs.opensource.google/go/go/+/refs/tags/go1.17.6:src/io/io.go;l=402-410;drc=refs%2Ftags%2Fgo1.17.6
//
// This approach does not allow us to keep track of pgwire message boundaries.
func connCopyWithIOCopy(dst io.ReadWriter, src io.ReadWriter) (int64, error) {
	return io.Copy(dst, src)
}

// connCopyWithIOCopyN uses io.CopyN to forward the message body to dst. This
// uses a small buffer of 5 bytes under the hood to read the message headers.
//
// This approach incurs two Write calls to the server. One for the header, and
// the other through io.CopyN for the body. Small messages can potentially be
// a problem since we'll end up with more syscalls.
func connCopyWithIOCopyN(dst io.ReadWriter, src io.ReadWriter) (int64, error) {
	var tmp [5]byte
	var written int64
	for {
		// Read/forward message header.
		nread, err := io.ReadFull(src, tmp[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		if nread != 5 {
			return 0, errors.New("could not read length")
		}
		n, err := dst.Write(tmp[:])
		if err != nil {
			return 0, err
		}
		written += int64(n)

		// Forward message body.
		size := int(binary.BigEndian.Uint32(tmp[1:])) - 4
		if size > 0 {
			// NOTE: io.CopyN(dst, src, n) = io.Copy(dst, LimitReader(src, n))
			m, err := io.CopyN(dst, src, int64(size))
			if err != nil {
				return 0, err
			}
			written += int64(m)
		}
	}
	return written, nil
}

// bufferSize represents the size of the internal buffer. This has different
// meanings depending on how it is used:
// - for pgproto3's chunkreaders, this would indicate the minimum buffer size
// - for interceptors, this would indicate a fixed buffer size
const bufferSize = 2 << 12 // 4K

// connCopyWithPGProto3_Client uses pgproto3's chunkreader and backend to
// receive and forward messages. This is used for the connection between
// client and proxy.
//
// This uses pgproto3's Receive under the hood, which also parses the message.
// The chunkreader also allocates without any limits (i.e. if a message has a
// body of 10MB, it will allocate 10MB to fit that message for parsing).
func connCopyWithPGProto3_Client(dst io.ReadWriter, src io.ReadWriter) (int64, error) {
	var written int64
	cr, err := chunkreader.NewConfig(src, chunkreader.Config{
		MinBufLen: bufferSize,
	})
	if err != nil {
		return 0, err
	}
	client := pgproto3.NewBackend(cr, src)
	for {
		msg, err := client.Receive()
		if err != nil {
			if err.Error() == "unexpected EOF" {
				return written, nil
			}
			return 0, err
		}
		n, err := dst.Write(msg.Encode(nil))
		if err != nil {
			return 0, err
		}
		written += int64(n)
	}
	return written, nil
}

// connCopyWithPGProto3_Server is similar to connCopyWithPGProto3_Client,
// but is used for the connection between proxy and server.
//
// See comments in connCopyWithPGProto3_Client for more information.
func connCopyWithPGProto3_Server(dst io.ReadWriter, src io.ReadWriter) (int64, error) {
	var written int64
	cr, err := chunkreader.NewConfig(src, chunkreader.Config{
		MinBufLen: bufferSize,
	})
	if err != nil {
		return 0, err
	}
	client := pgproto3.NewFrontend(cr, src)
	for {
		msg, err := client.Receive()
		if err != nil {
			if err.Error() == "unexpected EOF" {
				return written, nil
			}
			return 0, err
		}
		n, err := dst.Write(msg.Encode(nil))
		if err != nil {
			return 0, err
		}
		written += int64(n)
	}
	return written, nil
}

// connCopyWithChunkReader uses pgproto3's chunkreader to read the bytes. Data
// returned by Next is owned by the caller, and chunkreader allocates whenever
// the requested bytes does not fit in its internal buffer. There is no limit
// to how much chunkreader can allocate.
//
// This approach isn't like the previous pgproto3 approaches, which parses each
// time a message is read.
func connCopyWithChunkReader(dst io.ReadWriter, src io.ReadWriter) (int64, error) {
	var written int64
	r, err := chunkreader.NewConfig(src, chunkreader.Config{
		MinBufLen: bufferSize,
	})
	if err != nil {
		return 0, err
	}
	for {
		// Read/forward message header.
		header, err := r.Next(5)
		if err != nil {
			if err == io.EOF {
				break
			}
			return 0, err
		}
		n, err := dst.Write(header)
		if err != nil {
			return 0, err
		}
		written += int64(n)

		// Read/forward message body.
		size := int(binary.BigEndian.Uint32(header[1:])) - 4
		if size > 0 {
			msgBody, err := r.Next(size)
			if err != nil {
				return 0, err
			}
			m, err := dst.Write(msgBody)
			if err != nil {
				return 0, err
			}
			written += int64(m)
		}
	}
	return written, nil
}

// connCopyWithInterceptor_Client uses our custom implementation of the PG
// interceptors, and is used for the connection between client and proxy.
//
// This approach does not allocate during forwarding. If the message does not
// fit into the buffer size, this will call io.CopyN under the hood.
func connCopyWithInterceptor_Client(dst io.ReadWriter, src io.ReadWriter) (int64, error) {
	var written int64
	client, err := interceptor.NewBackendInterceptor(dst, src, bufferSize)
	if err != nil {
		return 0, err
	}
	for {
		n, err := client.ForwardMsg()
		if err != nil {
			return 0, err
		}
		written += int64(n)
	}
	return written, nil
}

// connCopyWithInterceptor_Server is similar to connCopyWithInterceptor_Client,
// but is used for the connection between proxy and server.
//
// See connCopyWithInterceptor_Client for more information.
func connCopyWithInterceptor_Server(dst io.ReadWriter, src io.ReadWriter) (int64, error) {
	var written int64
	proxy, err := interceptor.NewFrontendInterceptor(dst, src, bufferSize)
	if err != nil {
		return 0, err
	}
	for {
		n, err := proxy.ForwardMsg()
		if err != nil {
			return 0, err
		}
		written += int64(n)
	}
	return written, nil
}

// makeProxyServerHandler returns a handler used for the dummy proxy server.
// The returned handler forwards all pgwire messages to the server using the
// connCopy functions that were passed in. This terminates whenever there's an
// error on either end of the copying.
func makeProxyServerHandler(
	sqlServerAddr string, connCopyClientFn connCopyType, connCopyServerFn connCopyType,
) func(context.Context, net.Conn) error {
	return func(ctx context.Context, conn net.Conn) error {
		crdbConn, err := net.DialTimeout("tcp", sqlServerAddr, 5*time.Second)
		if err != nil {
			return err
		}
		defer crdbConn.Close()

		errCh := make(chan error, 2)
		go func() {
			if _, err := connCopyClientFn(crdbConn, conn); err != nil {
				errCh <- err
			}
		}()
		go func() {
			if _, err := connCopyServerFn(conn, crdbConn); err != nil {
				errCh <- err
			}
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		}
	}
}

// readBytesWritten reads the last dummy message sent by the SQL server, and
// extracts the number of bytes written to the SQL server.
func readBytesWritten(rd io.Reader) (int64, error) {
	var tmp [13]byte
	if _, err := io.ReadFull(rd, tmp[:]); err != nil {
		return 0, err
	}
	if tmp[0] != byte(pgwirebase.ServerMsgBackendKeyData) {
		return 0, errors.New("ServerMsgBackendKeyData was not received")
	}
	if binary.BigEndian.Uint32(tmp[1:5]) != 12 {
		return 0, errors.New("invalid length")
	}
	return int64(binary.BigEndian.Uint64(tmp[5:])), nil
}

// sqlServerHandler is a handler used for the dummy sql server. It discards all
// the pgwire messages received, and upon receiving the ClientMsgTerminate
// message, a ServerMsgBackendKeyData message will be returned with the total
// number of bytes received.
//
// Client -> Server:
//    ClientMsgSimpleQuery, ..., ClientMsgSimpleQuery, ClientMsgTerminate
//
// Server -> Client:
//    ServerMsgBackendKeyData (once terminate has been received)
//
func sqlServerHandler(ctx context.Context, conn net.Conn) error {
	var tmp [5]byte
	var written int64

	// Use a buffered reader.
	rd := bufio.NewReader(conn)
	for {
		// Read message header.
		nread, err := io.ReadFull(rd, tmp[:])
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if nread != 5 {
			return errors.New("could not read length")
		}
		written += int64(nread)

		// Read body.
		switch tmp[0] {
		case byte(pgwirebase.ClientMsgSimpleQuery):
			// Read message body.
			size := int(binary.BigEndian.Uint32(tmp[1:]))
			size -= 4
			nread, err := io.CopyN(io.Discard, rd, int64(size))
			if err != nil {
				return err
			}
			written += int64(nread)
		case byte(pgwirebase.ClientMsgTerminate):
			// This message acts like a flush on the number of written bytes.
			// The format is as follows:
			// - 1 byte (byte): type
			// - 4 bytes (uint32): length (always 12)
			// - 8 bytes (uint64): number of bytes received by server
			var result [13]byte // 1 + 4 + 8
			result[0] = byte(pgwirebase.ServerMsgBackendKeyData)
			binary.BigEndian.PutUint32(result[1:5], uint32(12))
			binary.BigEndian.PutUint64(result[5:], uint64(written))
			if _, err := conn.Write(result[:]); err != nil {
				return err
			}
			written = 0
		default:
			panic(errors.Newf("unsupported message type: %v", tmp[0]))
		}
	}
}

// testBmServer is a TCP server used for benchmarking ConnCopy. Both the test
// proxy and sql servers use this construct. It may also expose an HTTP server
// for health checks.
type testBmServer struct {
	stopper     *stop.Stopper
	connHandler func(context.Context, net.Conn) error
	mux         *http.ServeMux
	verboseLogs bool
}

func newTestBmServer(
	ctx context.Context, stopper *stop.Stopper, handler func(context.Context, net.Conn) error,
) *testBmServer {
	s := &testBmServer{
		stopper:     stopper,
		connHandler: handler,
		mux:         http.NewServeMux(),
	}
	s.mux.HandleFunc("/_status/healthz/", s.handleHealth)
	return s
}

// handleHealth is a healthz handler that tells that the server is up. It does
// not indicate whether the server is ready to accept requests.
func (s *testBmServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

// serve serves the listener ln that was passed to it. All incoming connections
// are handled by the connHandler.
func (s *testBmServer) serve(ctx context.Context, ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Suppress all "use of closed network connection" errors.
			return nil
		}

		if s.verboseLogs {
			log.Infof(ctx, "new connection: %s", conn.RemoteAddr())
		}

		if err := s.stopper.RunAsyncTask(ctx, "conn-serve", func(ctx context.Context) {
			defer conn.Close()

			ctx = logtags.AddTag(ctx, "client", conn.RemoteAddr())
			if err := s.connHandler(ctx, conn); err != nil && err != io.EOF {
				log.Infof(ctx, "connection error: %v", err)
			}
		}); err != nil {
			return err
		}
	}
}
