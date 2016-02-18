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
	const chunkSize = 2

	for _, tc := range []struct {
		input          string
		expectedOutput string
	}{
		{"\x12\x34", "\x12\x34"},
		{"GET /index.html HTTP1.1\r\n", "GET /index.html HTTP1.1\r\n"},
		{"PRI x HTTP/2.0\r\n\r\nSM\r\n\r\n", "PRI x HTTP/2.0\r\n\r\nSM\r\n\r\n"},
		{http2.ClientPreface, http2.ClientPreface[:headerInsertionIndex] + hostHeader + http2.ClientPreface[headerInsertionIndex:]},
	} {
		c1, c2 := net.Pipe()
		errCh := make(chan error, 100) // prevent deadlocks
		go func() {
			inputSlice := []byte(tc.input)
			// Write the input chunkSize bytes at a time, so that many Read calls are
			// needed.
			for i := 0; i < len(inputSlice); i += chunkSize {
				j := i + chunkSize
				if j > len(inputSlice) {
					j = len(inputSlice)
				}
				writeSlice := inputSlice[i:j]
				if n, err := c1.Write(writeSlice); err != nil {
					errCh <- err
				} else if n != len(writeSlice) {
					errCh <- fmt.Errorf("%q: expected to write %d bytes, but wrote %d bytes", tc.input, len(writeSlice), n)
				}
			}
			errCh <- c1.Close()
			close(errCh)
		}()

		rc := newReplayableConn(c2)

		// This construction reads from rc chunkSize bytes at a time to ensure that
		// rc.Read makes no unwise assumptions about the capacity of the passed
		// slice.
		numReads := 1 // declared outside readAll because we only check it once.
		readAll := func() []byte {
			var output []byte
			var readBuf [chunkSize]byte

			for ; ; numReads++ {
				n, err := rc.Read(readBuf[:])
				output = append(output, readBuf[:n]...)
				if err != nil {
					if err == io.EOF {
						break
					}
					t.Fatal(err)
				}
			}

			return output
		}

		if output := readAll(); string(output) != tc.expectedOutput {
			t.Errorf("%q: expected output:\n%q\nactual output:\n%q", tc.input, tc.expectedOutput, output)
		} else if e := ((len(output) + chunkSize - 1) / chunkSize) + 1; numReads != e {
			t.Errorf("%q: expected read to complete in %d calls, but took %d", tc.input, e, numReads)
		}

		for err := range errCh {
			if err != nil {
				t.Fatal(err)
			}
		}

		rc.replay()

		var expectedOutput string
		// If the expected output equals the input, then the test case is not an
		// HTTP2 request, the buffer has been released, so no playback occurs after
		// the call to replay.
		if tc.expectedOutput != tc.input {
			expectedOutput = tc.input
		}
		if output := readAll(); string(output) != expectedOutput {
			t.Errorf("%q: expected output:\n%q\nactual output:\n%q", tc.input, expectedOutput, output)
		}
	}
}
