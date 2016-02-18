// Copyright 2016 The Cockroach Authors.
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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package util

import (
	"fmt"
	"io"
	"net"
	"testing"

	"golang.org/x/net/http2"
)

func TestReplayableConn(t *testing.T) {
	for _, tc := range []struct {
		input          string
		expectedOutput string
	}{
		{"\x12\x34", "\x12\x34"},
		{"GET /index.html HTTP1.1\r\n", "GET /index.html HTTP1.1\r\n"},
		{"PRI x HTTP/2.0\r\n\r\nSM\r\n\r\n", "PRI x HTTP/2.0\r\n\r\nSM\r\n\r\n"},
		{http2.ClientPreface, "PRI * HTTP/2.0" + hostHeader + "\r\n\r\nSM\r\n\r\n"},
	} {
		c1, c2 := net.Pipe()
		errCh := make(chan error)
		go func() {
			inputSlice := []byte(tc.input)
			// Write these one at a time, so that many Read calls are needed.
			for i := range inputSlice {
				writeSlice := inputSlice[i : i+1]
				if n, err := c1.Write(writeSlice); err != nil {
					errCh <- err
				} else if n != len(writeSlice) {
					errCh <- fmt.Errorf("expected to write %d bytes, but wrote %d bytes", len(writeSlice), n)
				}
			}
			errCh <- c1.Close()
			close(errCh)
		}()

		rc := newReplayableConn(c2)

		var output []byte
		var readBuf [1]byte
		readAll := func() {
			for output = output[:0]; ; {
				if n, err := rc.Read(readBuf[:]); err != nil {
					if err == io.EOF {
						break
					}
					t.Fatal(err)
				} else {
					output = append(output, readBuf[:n]...)
				}
			}
		}
		readAll()

		for err := range errCh {
			if err != nil {
				t.Fatal(err)
			}
		}
		if string(output) != tc.expectedOutput {
			t.Errorf("expected output:\n%q\nactual output:\n%q", tc.expectedOutput, output)
		}

		rc.replay()
		readAll()

		expectedOutput := tc.input
		if tc.expectedOutput == tc.input {
			// If the expected output equals the input, then the test case is not
			// an HTTP2 request, and it is expected that replay will not be called.
			// If it is called anyway, only the minimum number of Read calls needed
			// to determine that the test case is not an HTTP2 request will have
			// been made while buffering, up to a maximum of
			// len(http2ClientPrefaceFirstLineSlice) bytes - this is why the
			// replayed output might be truncated.
			i := 0
			for ; i < len(http2ClientPrefaceFirstLineSlice) && http2.ClientPreface[:i] == tc.input[:i]; i++ {
			}
			expectedOutput = expectedOutput[:i]
		}
		if string(output) != expectedOutput {
			t.Errorf("expected output:\n%q\nactual output:\n%q", tc.input, output)
		}
	}
}
