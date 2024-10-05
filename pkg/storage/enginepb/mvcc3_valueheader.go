// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

import "github.com/cockroachdb/cockroach/pkg/util/buildutil"

// IsEmpty returns true if the header is empty.
// gcassert:inline
func (h MVCCValueHeader) IsEmpty() bool {
	return h == MVCCValueHeader{}
}

func (h *MVCCValueHeader) pure() MVCCValueHeaderPure {
	return MVCCValueHeaderPure{
		LocalTimestamp:   h.LocalTimestamp,
		OmitInRangefeeds: h.OmitInRangefeeds,
		ImportEpoch:      h.ImportEpoch,
	}
}

func (h *MVCCValueHeader) crdbTest() MVCCValueHeaderCrdbTest {
	return (MVCCValueHeaderCrdbTest)(*h)
}

// Size implements protoutil.Message.
func (h *MVCCValueHeader) Size() int {
	if buildutil.CrdbTestBuild && h.KVNemesisSeq.Get() != 0 {
		ht := h.crdbTest() //gcassert:noescape
		return ht.Size()
	}
	p := h.pure() //gcassert:noescape
	return p.Size()
}

// Marshal implements protoutil.Message.
func (h *MVCCValueHeader) Marshal() ([]byte, error) {
	if buildutil.CrdbTestBuild && h.KVNemesisSeq.Get() != 0 {
		ht := h.crdbTest() //gcassert:noescape
		return ht.Marshal()
	}
	p := h.pure() //gcassert:noescape
	return p.Marshal()
}

// MarshalTo implements protoutil.Message.
func (h *MVCCValueHeader) MarshalTo(buf []byte) (int, error) {
	if buildutil.CrdbTestBuild && h.KVNemesisSeq.Get() != 0 {
		ht := h.crdbTest() //gcassert:noescape
		return ht.MarshalTo(buf)
	}
	p := h.pure() //gcassert:noescape
	return p.MarshalTo(buf)
}

// MarshalToSizedBuffer implements protoutil.Message.
func (h *MVCCValueHeader) MarshalToSizedBuffer(buf []byte) (int, error) {
	if buildutil.CrdbTestBuild && h.KVNemesisSeq.Get() != 0 {
		ht := h.crdbTest() //gcassert:noescape
		return ht.MarshalToSizedBuffer(buf)
	}
	p := h.pure() //gcassert:noescape
	return p.MarshalToSizedBuffer(buf)
}
