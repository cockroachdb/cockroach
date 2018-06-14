// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

type postgreStream struct {
	r     io.Reader
	b     []byte
	queue []tree.Statement
	max   int
}

const (
	defaultPgStreamInit = 1024 * 64
	defaultPgStreamMax  = 1024 * 1024 * 10
)

// newPostgreStream returns a struct that can stream statements from an io.Reader by parsing chunks of statements at a time. initialCap is the initial buffer size. It will grow up to maxCap. maxCap must be larger than the expected largest statement.
func newPostgreStream(r io.Reader, initialCap, maxCap int) *postgreStream {
	return &postgreStream{
		r:   r,
		b:   make([]byte, 0, initialCap),
		max: maxCap,
	}
}

func (p *postgreStream) Next() (tree.Statement, error) {
	if len(p.queue) == 0 {
		sl, err := p.read()
		if err != nil {
			return nil, err
		}
		p.queue = sl
	}
	// len(p.queue) is guaranteed to be > 0 here because EOF would have been
	// returned earlier if nothing was read.
	s := p.queue[0]
	p.queue = p.queue[1:]
	return s, nil
}

func (p *postgreStream) read() (tree.StatementList, error) {
	for {
		// First attempt to read the possibly empty p.b.
		bs := string(p.b)
		s := parser.MakeScanner(bs)
		pos := s.Until(';')
		// Find the last semicolon.
		for pos != 0 {
			npos := s.Until(';')
			if npos != 0 {
				pos = npos
			} else {
				break
			}
		}
		// We found something. Shift over the unused p.b to its beginning using the
		// same underlying location and parse what we found.
		if pos != 0 {
			n := copy(p.b, p.b[pos:])
			p.b = p.b[:n]
			return parser.Parse(bs[:pos])
		}

		// We didn't find a semicolon. Need to read more into p.b. See if p.b already
		// has more cap space.
		start := len(p.b)
		sz := cap(p.b)
		// If len(b) == cap(b) then we need a bigger slice.
		if len(p.b) == cap(p.b) {
			sz = cap(p.b) * 2
			if sz > p.max {
				sz = p.max
			}
			if sz < cap(p.b) {
				sz = cap(p.b)
			}
		}
		p.b = append(p.b, make([]byte, sz-start)...)

		// Read in data after whatever was already there.
		n, err := io.ReadFull(p.r, p.b[start:])
		if err == io.ErrUnexpectedEOF {
			p.b = p.b[:start+n]
		} else if err != nil {
			return nil, err
		} else if n == 0 {
			return nil, errors.Errorf("buffer too small: %d", sz)
		}
	}
}
