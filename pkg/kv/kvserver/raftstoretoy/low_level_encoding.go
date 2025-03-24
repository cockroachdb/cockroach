// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import "github.com/cockroachdb/errors"

type DefaultLogEncoding struct{}

func (DefaultLogEncoding) Encode(buf []byte, e LogEncodable) ([]byte, error) {
	buf = append(buf, byte(e.LogKeyKind()))
	switch obj := e.(type) {
	case *LogIdent:
		// TODO(tbg): make all `e` protos and just use proto encoding.
		buf = append(buf, MakeKey(obj.RangeID, obj.ReplicaID, obj.LogID).Encode()...)
	// case *Destroy:
	default:
		return nil, errors.Errorf("unknown LogEncodable: %T", e)
	}
	return buf, nil
}

func (DefaultLogEncoding) Decode(b []byte) (LogEncodable, error) {
	if len(b) == 0 {
		return nil, errors.New("too short")
	}
	switch LogKeyKind(b[0]) {
	default:
		return nil, errors.Errorf("unknown LogKeyKind: %v", b[0])
	}
}
