// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enginepb

import "github.com/cockroachdb/cockroach/pkg/util/buildutil"

// IsEmpty returns true if the header is empty.
// gcassert:inline
func (h MVCCValueHeader) IsEmpty() bool {
	// NB: We don't use a struct comparison like h == MVCCValueHeader{} due to a
	// Go 1.19 performance regression, see:
	// https://github.com/cockroachdb/cockroach/issues/88818
	return h.LocalTimestamp.IsEmpty() && h.KVNemesisSeq.Get() == 0
}

func (h *MVCCValueHeader) pure() MVCCValueHeaderPure {
	return MVCCValueHeaderPure{
		LocalTimestamp: h.LocalTimestamp,
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
