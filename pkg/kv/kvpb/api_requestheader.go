// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvpb

import "github.com/cockroachdb/cockroach/pkg/util/buildutil"

func (r *RequestHeader) pure() RequestHeaderPure {
	return RequestHeaderPure{
		Key:      r.Key,
		EndKey:   r.EndKey,
		Sequence: r.Sequence,
	}
}

func (r *RequestHeader) crdbTest() RequestHeaderCrdbTest {
	return RequestHeaderCrdbTest(*r)
}

// Size implements protoutil.Message.
func (r *RequestHeader) Size() int {
	if buildutil.CrdbTestBuild && r.KVNemesisSeq.Get() != 0 {
		rt := r.crdbTest() //gcassert:noescape
		return rt.Size()
	}
	p := r.pure() //gcassert:noescape
	return p.Size()
}

// Marshal implements protoutil.Message.
func (r *RequestHeader) Marshal() ([]byte, error) {
	if buildutil.CrdbTestBuild && r.KVNemesisSeq.Get() != 0 {
		rt := r.crdbTest()
		return rt.Marshal()
	}
	p := r.pure()
	return p.Marshal()
}

// MarshalTo implements protoutil.Message.
func (r *RequestHeader) MarshalTo(buf []byte) (int, error) {
	if buildutil.CrdbTestBuild && r.KVNemesisSeq.Get() != 0 {
		rt := r.crdbTest() //gcassert:noescape
		return rt.MarshalTo(buf)
	}
	p := r.pure() //gcassert:noescape
	return p.MarshalTo(buf)
}

// MarshalToSizedBuffer implements protoutil.Message.
func (r *RequestHeader) MarshalToSizedBuffer(buf []byte) (int, error) {
	if buildutil.CrdbTestBuild && r.KVNemesisSeq.Get() != 0 {
		rt := r.crdbTest() //gcassert:noescape
		return rt.MarshalToSizedBuffer(buf)
	}
	p := r.pure() //gcassert:noescape
	return p.MarshalToSizedBuffer(buf)
}
