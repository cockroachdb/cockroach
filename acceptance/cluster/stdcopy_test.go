// Copyright 2012-2016 Docker, Inc.
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
// Source: github.com/docker/docker/pkg/stdcopy

package cluster

// TODO(tamird): all of this can be removed if a dependency on docker/docker
// is deemed acceptable or https://github.com/docker/engine-api/issues/204
// closes.

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

func TestNewStdWriter(t *testing.T) {
	writer := NewStdWriter(ioutil.Discard, Stdout)
	if writer == nil {
		t.Fatalf("NewStdWriter with an invalid StdType should not return nil.")
	}
}

func TestWriteWithUnitializedStdWriter(t *testing.T) {
	writer := stdWriter{
		Writer: nil,
		prefix: byte(Stdout),
	}
	n, err := writer.Write([]byte("Something here"))
	if n != 0 || err == nil {
		t.Fatalf("Should fail when given an uncomplete or uninitialized StdWriter")
	}
}

func TestWriteWithNilBytes(t *testing.T) {
	writer := NewStdWriter(ioutil.Discard, Stdout)
	n, err := writer.Write(nil)
	if err != nil {
		t.Fatalf("Shouldn't have fail when given no data")
	}
	if n > 0 {
		t.Fatalf("Write should have written 0 byte, but has written %d", n)
	}
}

func TestWrite(t *testing.T) {
	writer := NewStdWriter(ioutil.Discard, Stdout)
	data := []byte("Test StdWrite.Write")
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Error while writing with StdWrite")
	}
	if n != len(data) {
		t.Fatalf("Write should have written %d byte but wrote %d.", len(data), n)
	}
}

type errWriter struct {
	n   int
	err error
}

func (f *errWriter) Write(buf []byte) (int, error) {
	return f.n, f.err
}

func TestWriteWithWriterError(t *testing.T) {
	expectedError := errors.New("expected")
	expectedReturnedBytes := 10
	writer := NewStdWriter(&errWriter{
		n:   stdWriterPrefixLen + expectedReturnedBytes,
		err: expectedError}, Stdout)
	data := []byte("This won't get written, sigh")
	n, err := writer.Write(data)
	if err != expectedError {
		t.Fatalf("Didn't get expected error.")
	}
	if n != expectedReturnedBytes {
		t.Fatalf("Didn't get expected written bytes %d, got %d.",
			expectedReturnedBytes, n)
	}
}

func TestWriteDoesNotReturnNegativeWrittenBytes(t *testing.T) {
	writer := NewStdWriter(&errWriter{n: -1}, Stdout)
	data := []byte("This won't get written, sigh")
	actual, _ := writer.Write(data)
	if actual != 0 {
		t.Fatalf("Expected returned written bytes equal to 0, got %d", actual)
	}
}

func getSrcBuffer(stdoutBytes, stderrBytes []byte) (buffer *bytes.Buffer, err error) {
	buffer = new(bytes.Buffer)
	dstOut := NewStdWriter(buffer, Stdout)
	_, err = dstOut.Write(stdoutBytes)
	if err != nil {
		return
	}
	dstErr := NewStdWriter(buffer, Stderr)
	_, err = dstErr.Write(stderrBytes)
	return
}

func TestStdCopyWriteAndRead(t *testing.T) {
	stdoutBytes := []byte(strings.Repeat("o", startingBufLen))
	stderrBytes := []byte(strings.Repeat("e", startingBufLen))
	buffer, err := getSrcBuffer(stdoutBytes, stderrBytes)
	if err != nil {
		t.Fatal(err)
	}
	written, err := StdCopy(ioutil.Discard, ioutil.Discard, buffer)
	if err != nil {
		t.Fatal(err)
	}
	expectedTotalWritten := len(stdoutBytes) + len(stderrBytes)
	if written != int64(expectedTotalWritten) {
		t.Fatalf("Expected to have total of %d bytes written, got %d", expectedTotalWritten, written)
	}
}

type customReader struct {
	n            int
	err          error
	totalCalls   int
	correctCalls int
	src          *bytes.Buffer
}

func (f *customReader) Read(buf []byte) (int, error) {
	f.totalCalls++
	if f.totalCalls <= f.correctCalls {
		return f.src.Read(buf)
	}
	return f.n, f.err
}

func TestStdCopyReturnsErrorReadingHeader(t *testing.T) {
	expectedError := errors.New("error")
	reader := &customReader{
		err: expectedError}
	written, err := StdCopy(ioutil.Discard, ioutil.Discard, reader)
	if written != 0 {
		t.Fatalf("Expected 0 bytes read, got %d", written)
	}
	if err != expectedError {
		t.Fatalf("Didn't get expected error")
	}
}

func TestStdCopyReturnsErrorReadingFrame(t *testing.T) {
	expectedError := errors.New("error")
	stdoutBytes := []byte(strings.Repeat("o", startingBufLen))
	stderrBytes := []byte(strings.Repeat("e", startingBufLen))
	buffer, err := getSrcBuffer(stdoutBytes, stderrBytes)
	if err != nil {
		t.Fatal(err)
	}
	reader := &customReader{
		correctCalls: 1,
		n:            stdWriterPrefixLen + 1,
		err:          expectedError,
		src:          buffer}
	written, err := StdCopy(ioutil.Discard, ioutil.Discard, reader)
	if written != 0 {
		t.Fatalf("Expected 0 bytes read, got %d", written)
	}
	if err != expectedError {
		t.Fatalf("Didn't get expected error")
	}
}

func TestStdCopyDetectsCorruptedFrame(t *testing.T) {
	stdoutBytes := []byte(strings.Repeat("o", startingBufLen))
	stderrBytes := []byte(strings.Repeat("e", startingBufLen))
	buffer, err := getSrcBuffer(stdoutBytes, stderrBytes)
	if err != nil {
		t.Fatal(err)
	}
	reader := &customReader{
		correctCalls: 1,
		n:            stdWriterPrefixLen + 1,
		err:          io.EOF,
		src:          buffer}
	written, err := StdCopy(ioutil.Discard, ioutil.Discard, reader)
	if written != startingBufLen {
		t.Fatalf("Expected %d bytes read, got %d", startingBufLen, written)
	}
	if err != nil {
		t.Fatal("Didn't get nil error")
	}
}

func TestStdCopyWithInvalidInputHeader(t *testing.T) {
	dstOut := NewStdWriter(ioutil.Discard, Stdout)
	dstErr := NewStdWriter(ioutil.Discard, Stderr)
	src := strings.NewReader("Invalid input")
	_, err := StdCopy(dstOut, dstErr, src)
	if err == nil {
		t.Fatal("StdCopy with invalid input header should fail.")
	}
}

func TestStdCopyWithCorruptedPrefix(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03}
	src := bytes.NewReader(data)
	written, err := StdCopy(nil, nil, src)
	if err != nil {
		t.Fatalf("StdCopy should not return an error with corrupted prefix.")
	}
	if written != 0 {
		t.Fatalf("StdCopy should have written 0, but has written %d", written)
	}
}

func TestStdCopyReturnsWriteErrors(t *testing.T) {
	stdoutBytes := []byte(strings.Repeat("o", startingBufLen))
	stderrBytes := []byte(strings.Repeat("e", startingBufLen))
	buffer, err := getSrcBuffer(stdoutBytes, stderrBytes)
	if err != nil {
		t.Fatal(err)
	}
	expectedError := errors.New("expected")

	dstOut := &errWriter{err: expectedError}

	written, err := StdCopy(dstOut, ioutil.Discard, buffer)
	if written != 0 {
		t.Fatalf("StdCopy should have written 0, but has written %d", written)
	}
	if err != expectedError {
		t.Fatalf("Didn't get expected error, got %v", err)
	}
}

func TestStdCopyDetectsNotFullyWrittenFrames(t *testing.T) {
	stdoutBytes := []byte(strings.Repeat("o", startingBufLen))
	stderrBytes := []byte(strings.Repeat("e", startingBufLen))
	buffer, err := getSrcBuffer(stdoutBytes, stderrBytes)
	if err != nil {
		t.Fatal(err)
	}
	dstOut := &errWriter{n: startingBufLen - 10}

	written, err := StdCopy(dstOut, ioutil.Discard, buffer)
	if written != 0 {
		t.Fatalf("StdCopy should have return 0 written bytes, but returned %d", written)
	}
	if err != io.ErrShortWrite {
		t.Fatalf("Didn't get expected io.ErrShortWrite error")
	}
}

func BenchmarkWrite(b *testing.B) {
	w := NewStdWriter(ioutil.Discard, Stdout)
	data := []byte("Test line for testing stdwriter performance\n")
	data = bytes.Repeat(data, 100)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Write(data); err != nil {
			b.Fatal(err)
		}
	}
}
