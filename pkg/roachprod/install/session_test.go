// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"context"
	"io"
	"sync/atomic"
)

type MockSession struct {
	OnCombinedOutput func(context.Context) ([]byte, error)
	OnRun            func(context.Context) error
	OnStart          func(context.Context) error
	OnWait           func() error

	Stdout io.ReadWriteCloser
	Stderr io.ReadWriteCloser
}

type MockSessionOptions struct {
	ChannelSize int
}

// dataChannel emulates blocking reads and writes with a channel.
type dataChannel struct {
	dataChan chan []byte
	closed   atomic.Bool
}

// MockSession implements the session interface.
var _ session = &MockSession{}

func (m MockSession) CombinedOutput(ctx context.Context) ([]byte, error) {
	if m.OnCombinedOutput != nil {
		return m.OnCombinedOutput(ctx)
	}
	return nil, nil
}

func (m MockSession) Run(ctx context.Context) error {
	if m.OnRun != nil {
		return m.OnRun(ctx)
	}
	return nil
}

func (m MockSession) Start() error {
	if m.OnStart != nil {
		return m.OnStart(context.Background())
	}
	return nil
}

func (m MockSession) RequestPty() error {
	return nil
}

func (m MockSession) Wait() error {
	if m.OnWait != nil {
		return m.OnWait()
	}
	return nil
}

func (m MockSession) Close() {
	_ = m.Stdout.Close()
	_ = m.Stderr.Close()
}

func (m MockSession) SetStdin(_ io.Reader) {
	panic("not implemented")
}

func (m MockSession) SetStdout(_ io.Writer) {
	panic("not implemented")
}

func (m MockSession) SetStderr(_ io.Writer) {
	panic("not implemented")
}

func (m MockSession) StdinPipe() (io.WriteCloser, error) {
	panic("not implemented")
}

func (m MockSession) StdoutPipe() (io.Reader, error) {
	return m.Stdout, nil
}

func (m MockSession) StderrPipe() (io.Reader, error) {
	return m.Stderr, nil
}

// NewMockSession creates a new mock session. The mock session can be used to
// simulate the Stdout and Stderr of a session. Stdout and Stderr are backed by
// channels, that are unbuffered by default.
func NewMockSession(opts MockSessionOptions) *MockSession {
	return &MockSession{
		Stdout: newDataChannel(opts.ChannelSize),
		Stderr: newDataChannel(opts.ChannelSize),
	}
}

// DefaultMockSessionOptions returns the default options for a mock session.
func DefaultMockSessionOptions() MockSessionOptions {
	return MockSessionOptions{}
}

func newDataChannel(channelSize int) io.ReadWriteCloser {
	return &dataChannel{dataChan: make(chan []byte, channelSize)}
}

// Close is a no-op if the channel is already closed. This allows tests or the
// implementation to close the channel without panicking.
func (b *dataChannel) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	close(b.dataChan)
	return nil
}

func (b *dataChannel) Write(p []byte) (n int, err error) {
	b.dataChan <- p
	return len(p), nil
}

func (b *dataChannel) Read(p []byte) (n int, err error) {
	data := <-b.dataChan
	if data == nil {
		return 0, io.EOF
	}
	copiedLen := copy(p, data)
	if copiedLen < len(data) {
		b.dataChan <- data[copiedLen:]
	}
	return copiedLen, nil
}
