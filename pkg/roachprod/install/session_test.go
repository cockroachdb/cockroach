package install

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type mockSession struct {
	OnCombinedOutput func(context.Context) ([]byte, error)
	OnRun            func(context.Context) error
	OnStart          func(context.Context) error
	OnWait           func() error

	StdoutBuffer io.ReadWriteCloser
	StderrBuffer io.ReadWriteCloser
}

// CloseableBuffer is a bytes.Buffer that can be closed.
// Only io.Reader and io.Writer methods are protected by a mutex.
type CloseableBuffer struct {
	*bytes.Buffer
	mu     sync.Mutex
	closed atomic.Bool
}

var _ session = &mockSession{}

func (m mockSession) CombinedOutput(ctx context.Context) ([]byte, error) {
	if m.OnCombinedOutput != nil {
		return m.OnCombinedOutput(ctx)
	}
	return nil, nil
}

func (m mockSession) Run(ctx context.Context) error {
	if m.OnRun != nil {
		return m.OnRun(ctx)
	}
	return nil
}

func (m mockSession) Start() error {
	if m.OnStart != nil {
		return m.OnStart(context.Background())
	}
	return nil
}

func (m mockSession) RequestPty() error {
	return nil
}

func (m mockSession) Wait() error {
	if m.OnWait != nil {
		return m.OnWait()
	}
	return nil
}

func (m mockSession) Close() {

}

func (m mockSession) SetStdin(_ io.Reader) {
	//TODO implement me
	panic("implement me")
}

func (m mockSession) SetStdout(_ io.Writer) {
	//TODO implement me
	panic("implement me")
}

func (m mockSession) SetStderr(_ io.Writer) {
	//TODO implement me
	panic("implement me")
}

func (m mockSession) StdinPipe() (io.WriteCloser, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockSession) StdoutPipe() (io.Reader, error) {
	return m.StdoutBuffer, nil
}

func (m mockSession) StderrPipe() (io.Reader, error) {
	return m.StderrBuffer, nil
}

func newMockSession() *mockSession {
	return &mockSession{
		StdoutBuffer: newCloseableBuffer(),
		StderrBuffer: newCloseableBuffer(),
	}
}

func newCloseableBuffer() *CloseableBuffer {
	return &CloseableBuffer{Buffer: &bytes.Buffer{}}
}

func (b *CloseableBuffer) Close() error {
	b.closed.Store(true)
	return nil
}

func (b *CloseableBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *CloseableBuffer) Read(p []byte) (n int, err error) {
	// Simulate a blocking read until the buffer is closed.
	for !b.closed.Load() {
		if func() bool {
			b.mu.Lock()
			defer b.mu.Unlock()
			return b.Len() != 0
		}() {
			break
		}
		// Yield a bit to avoid busy-waiting.
		time.Sleep(10 * time.Millisecond)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Read(p)
}
