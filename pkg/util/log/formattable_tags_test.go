// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"testing"

	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
)

func TestFormattableTags(t *testing.T) {
	testCases := []struct {
		ctx        context.Context
		redactable bool
		safe       redact.RedactableString
		buf        string
		json       string
		rvals      string
	}{
		{
			ctx:        context.Background(),
			redactable: false,
			safe:       "", buf: "", json: "", rvals: "",
		},
		{
			ctx:        context.Background(),
			redactable: true,
			safe:       "", buf: "", json: "", rvals: "",
		},
		{
			ctx: logtags.AddTag(logtags.AddTag(logtags.AddTag(logtags.AddTag(logtags.AddTag(context.Background(),
				"noval", nil),
				"n", 1),
				"m", "uns‹afe"),
				"z", "x\x00y"),
				"long", redact.Sprintf(`safe "%s"`, "unsafe")),
			redactable: false,
			// Because the entry is not redactable to start with, when
			// emitting in a safe writer context, all the value strings are
			// considered unsafe and any special characters, e.g. redaction
			// markers, get escaped.
			safe: `noval,n‹1›,m‹uns?afe›,z‹x?y›,long=‹safe "?unsafe?"›`,
			// Because the entry is not redactable, when emitting raw
			// we do not care about escaping. So it's possible for
			// redaction markers to be unbalanced.
			buf: `noval,n1,muns‹afe,zx?y,long=safe "‹unsafe›"`,
			// Ditto for json.
			json: `"noval":"","n":"1","m":"uns‹afe","z":"x?y","long":"safe \"‹unsafe›\""`,
			// Redacted values everywhere.
			rvals: "noval,n×,m×,z×,long=×",
		},
		{
			ctx: logtags.AddTag(logtags.AddTag(logtags.AddTag(logtags.AddTag(logtags.AddTag(context.Background(),
				"noval", nil),
				"n", 1),
				"m", "uns‹afe"),
				"z", "x\x00y"),
				"long", redact.Sprintf(`safe "%s"`, "unsafe")),
			redactable: true,
			// The entry is redactable, so we can do the right thing in the various output contexts.
			safe: `noval,n1,m‹uns?afe›,z‹x?y›,long=safe "‹unsafe›"`,
			buf:  `noval,n1,m‹uns?afe›,z‹x?y›,long=safe "‹unsafe›"`,
			json: `"noval":"","n":"1","m":"‹uns?afe›","z":"‹x?y›","long":"safe \"‹unsafe›\""`,
			// In any case, redacted values everywhere.
			rvals: "noval,n×,m×,z×,long=×",
		},
	}

	for i, tc := range testCases {
		tags := makeFormattableTags(tc.ctx, tc.redactable)

		var rbuf redact.StringBuilder
		tags.formatToSafeWriter(&rbuf, tc.redactable)
		assert.Equal(t, tc.safe, rbuf.RedactableString(), "safeprint %d", i)

		var buf buffer
		tags.formatToBuffer(&buf)
		assert.Equal(t, tc.buf, buf.String(), "bufprint %d", i)

		buf = buffer{}
		tags.formatJSONToBuffer(&buf)
		assert.Equal(t, tc.json, buf.String(), "jsonprint %d", i)

		buf = buffer{}
		rtags := tags.redactTagValues(false /* preserve markers */)
		rtags.formatToBuffer(&buf)

		assert.Equal(t, tc.rvals, buf.String(), "redactvals %d", i)
	}
}

func TestEscapeNulBytes(t *testing.T) {
	assert.Equal(t, string(escapeNulBytes(nil, "")), "")
	assert.Equal(t, string(escapeNulBytes(nil, "\x00")), "?")
	assert.Equal(t, string(escapeNulBytes(nil, "abc\x00")), "abc?")
	assert.Equal(t, string(escapeNulBytes(nil, "\x00abc")), "?abc")
	assert.Equal(t, string(escapeNulBytes(nil, "aa\x00bb\x00\x00cc")), "aa?bb??cc")
}
