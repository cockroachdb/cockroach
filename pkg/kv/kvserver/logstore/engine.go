package logstore

import "github.com/cockroachdb/cockroach/pkg/storage"

type LogEngine storage.Engine

type LogReader struct {
	storage.Reader
}

func MakeLogReader(r storage.Reader) LogReader {
	return LogReader{Reader: r}
}

type LogWriter struct {
	storage.Writer
}

func MakeLogWriter(w storage.Writer) LogWriter {
	return LogWriter{Writer: w}
}

type LogReadWriter struct {
	storage.ReadWriter
}

func MakeLogRW(rw storage.ReadWriter) LogReadWriter {
	return LogReadWriter{ReadWriter: rw}
}

func (rw LogReadWriter) Writer() LogWriter {
	return LogWriter{Writer: rw.ReadWriter}
}

func (rw LogReadWriter) Reader() LogReader {
	return LogReader{Reader: rw.ReadWriter}
}
