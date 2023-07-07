// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package copy

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgwirebase"
	"github.com/cockroachdb/errors"
)

// Reader is an io.Reader that reads the COPY protocol from the underlying pgwire
// buffer but removes the protocol bits and just returns the raw text. There's
// no buffering, there's a buffer below it on top of the raw connection and
// any buffering required above it (for line reading etc) is either
// implemented in COPY or in the CSV reader.
type Reader struct {
	// We don't use this buffer but we use some of its helper methods.
	readBuf pgwirebase.ReadBuffer
	// pgr is the underlying pgwire reader we're reading from
	pgr pgwirebase.BufferedReader
	// Scratch space for Drain
	scratch   [128]byte
	remainder int
	done      bool
}

var _ io.Reader = &Reader{}

// NewReader creates and initializes a copy.Reader.
func NewReader(pgr pgwirebase.BufferedReader, sv *settings.Values) *Reader {
	c := &Reader{}
	c.pgr = pgr
	c.readBuf.SetOption(pgwirebase.ReadBufferOptionWithClusterSettings(sv))
	return c
}

// Read implements the io.Reader interface.
func (c *Reader) Read(p []byte) (int, error) {
	// The CSV reader can eat an EOF and will come back for another read in some
	// case's. Keep giving it EOF's in that case.
	if c.done {
		return 0, io.EOF
	}
	// If we had a short read, finish it.
	if c.remainder == 0 {
		// Go to pgwire to get next segment.
		size, err := c.readTypedMessage()
		if err != nil {
			return 0, err
		}
		c.remainder = size
	}
	// We never want to overread from the wire, we might read past COPY data
	// segments so limit p to remainder bytes.
	if c.remainder < len(p) {
		p = p[:c.remainder]
	}
	n, err := c.pgr.Read(p)
	if err != nil {
		return 0, err
	}
	c.remainder -= n
	return n, nil
}

func (c *Reader) readTypedMessage() (size int, err error) {
	b, err := c.pgr.ReadByte()
	if err != nil {
		return 0, err
	}
	typ := pgwirebase.ClientMessageType(b)
	_, size, err = c.readBuf.ReadUntypedMsgSize(c.pgr)
	if err != nil {
		if pgwirebase.IsMessageTooBigError(err) && typ == pgwirebase.ClientMsgCopyData {
			// Slurp the remaining bytes.
			_, slurpErr := c.readBuf.SlurpBytes(c.pgr, pgwirebase.GetMessageTooBigSize(err))
			if slurpErr != nil {
				return 0, errors.CombineErrors(err, errors.Wrapf(slurpErr, "error slurping remaining bytes in COPY"))
			}

			// As per the pgwire spec, we must continue reading until we encounter
			// CopyDone or CopyFail. We don't support COPY in the extended
			// protocol, so we don't need to look for Sync messages. See
			// https://www.postgresql.org/docs/13/protocol-flow.html#PROTOCOL-COPY
			for {
				typ, _, slurpErr = c.readBuf.ReadTypedMsg(c.pgr)
				if typ == pgwirebase.ClientMsgCopyDone || typ == pgwirebase.ClientMsgCopyFail {
					break
				}
				if slurpErr != nil && !pgwirebase.IsMessageTooBigError(slurpErr) {
					return 0, errors.CombineErrors(err, errors.Wrapf(slurpErr, "error slurping remaining bytes in COPY"))
				}

				_, slurpErr = c.readBuf.SlurpBytes(c.pgr, pgwirebase.GetMessageTooBigSize(slurpErr))
				if slurpErr != nil {
					return 0, errors.CombineErrors(err, errors.Wrapf(slurpErr, "error slurping remaining bytes in COPY"))
				}
			}
		}
		return 0, err
	}
	switch typ {
	case pgwirebase.ClientMsgCopyData:
		// Just return size.
	case pgwirebase.ClientMsgCopyDone:
		c.done = true
		return 0, io.EOF
	case pgwirebase.ClientMsgCopyFail:
		msg := make([]byte, size)
		if _, err := io.ReadFull(c.pgr, msg); err != nil {
			return 0, err
		}
		return 0, pgerror.Newf(pgcode.QueryCanceled, "COPY from stdin failed: %s", string(msg))
	case pgwirebase.ClientMsgFlush, pgwirebase.ClientMsgSync:
		// Spec says to "ignore Flush and Sync messages received during copy-in mode".
	default:
		// In order to gracefully handle bogus protocol, ie back to back copies, we have to
		// slurp these bytes.
		msg := make([]byte, size)
		if _, err := io.ReadFull(c.pgr, msg); err != nil {
			return 0, err
		}
		return 0, pgwirebase.NewUnrecognizedMsgTypeErr(typ)
	}
	return size, nil
}

// Drain will discard any bytes we haven't read yet.
func (c *Reader) Drain() error {
	for c.remainder > 0 {
		n, err := c.pgr.Read(c.scratch[:])
		if err != nil {
			return err
		}
		c.remainder -= n
	}
	return nil
}
