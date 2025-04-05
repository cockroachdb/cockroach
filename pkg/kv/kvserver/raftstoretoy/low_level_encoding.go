// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import (
	"bytes"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

type DefaultLogEncoding struct {
	registry map[LogKeyKind]func() LogEncodable
}

var _ Encoding = &DefaultLogEncoding{}

func (e DefaultLogEncoding) Register(kind LogKeyKind, new func() LogEncodable) {
	if e.registry == nil {
		e.registry = make(map[LogKeyKind]func() LogEncodable)
	}
	e.registry[kind] = new
}

func (DefaultLogEncoding) Encode(buf []byte, e LogEncodable) ([]byte, error) {
	buf = append(buf, byte(e.LogKeyKind()))
	switch obj := e.(type) {
	case *LogIdent:
		// TODO(tbg): make all `e` protos and just use proto encoding.
		enc, err := ye(obj)
		if err != nil {
			return nil, err
		}
		buf = append(buf, enc...)
	// case *Destroy:
	default:
		return nil, errors.Errorf("unknown LogEncodable: %T", e)
	}
	return buf, nil
}

func (e DefaultLogEncoding) Decode(b []byte, dstOrNil LogEncodable) (LogEncodable, error) {
	if len(b) == 0 {
		return nil, errors.New("too short")
	}
	kind := LogKeyKind(b[0])
	if kind >= LKKInvalid {
		return nil, errors.Errorf("unknown LogKeyKind: %v", b[0])
	}
	if dstOrNil == nil {
		dstOrNil = e.registry[kind]()
	}
	if err := yd(b[1:], dstOrNil); err != nil {
		return nil, err
	}
	return dstOrNil, nil
}

func ye(obj any) ([]byte, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func yd(in []byte, dst any) error {
	return yaml.NewDecoder(bytes.NewReader(in)).Decode(dst)
}
