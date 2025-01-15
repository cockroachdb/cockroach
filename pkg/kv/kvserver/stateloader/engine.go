package stateloader

import "github.com/cockroachdb/cockroach/pkg/storage"

type StateEngine storage.Engine

type StateReader struct {
	storage.Reader
}

func MakeStateReader(r storage.Reader) StateReader {
	return StateReader{Reader: r}
}

type StateReadWriter struct {
	storage.ReadWriter
}

func MakeStateRW(rw storage.ReadWriter) StateReadWriter {
	return StateReadWriter{ReadWriter: rw}
}

func (rw StateReadWriter) Reader() StateReader {
	return StateReader{Reader: rw.ReadWriter}
}
