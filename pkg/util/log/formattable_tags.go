// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"github.com/cockroachdb/redact/interfaces"
)

// formattableTags is a memory- efficient encoded representation of
// the logging tags, suitable for re-formatting in different output
// log formats.
//
// Internally, it is a sequence of nul-delimited strings,
// interleaving tag key and value strings. For example:
//
//	{'n', 0, '1', 0, 's', 0, '2', 0}
//
// to encode e.g. n=1,s=2
//
// Note that we preserve the invariant that there is always a value
// part for every key, and a nul byte after each part. So we have a
// guarantee that the number of nul bytes is even and all parts are
// terminated.
//
// If the formattableTags was constructed for a redactable
// entry, the value sub-strings are really RedactableStrings.
// If the entry is not redactable, the sub-strings are not
// RedactableStrings and should be escaped if emitted
// in a redactable context.
type formattableTags []byte

func makeFormattableTags(ctx context.Context, redactable bool) (res formattableTags) {
	tBuf := logtags.FromContext(ctx)
	if tBuf == nil {
		return nil
	}
	for _, t := range tBuf.Get() {
		// The key is always considered safe.
		//
		// TODO(obs-inf/server): this assumes that log tag keys are safe,
		// but this is not enforced throughout the code base. We could
		// lint that it is true similar to how we lint that the format
		// strings for `log.Infof` etc are const strings.
		res = append(res, t.Key()...)
		res = append(res, 0)

		// Encode the value. If there's no value, skip it. If it's the
		// (common) empty string, don't bother - we know it's going to
		// produce an empty encoding. This latter case is an optimization.
		if val := t.Value(); val != nil && val != "" {
			if !redactable {
				// Pass the value as-is.
				res = escapeNulBytes(res, fmt.Sprint(val))
			} else {
				// Make the value redactable, which adds redaction markers around unsafe bits.
				res = escapeNulBytes(res, string(redact.Sprint(val)))
			}
		}
		res = append(res, 0)
	}
	return res
}

func escapeNulBytes(res []byte, s string) []byte {
	k := 0
	for i := 0; i < len(s); i++ {
		if s[i] == 0 {
			res = append(res, s[k:i]...)
			res = append(res, '?')
			k = i + 1
		}
	}
	res = append(res, s[k:]...)
	return res
}

// formattableTagsIterator is a helper for the various formatting functions below.
type formattableTagsIterator struct {
	tags []byte
}

// next advances the iterator to the next key/val pair.
func (i *formattableTagsIterator) next() (key, val []byte, done bool) {
	if len(i.tags) == 0 {
		return nil, nil, true
	}
	// Advance the cursor to the beginning of the first next value.
	// No need to check for the value -1 because of the invariant
	// that all parts are nul-terminated.
	nv := bytes.IndexByte(i.tags, 0)
	// The key is everything so far.
	key = i.tags[:nv]
	// Truncate the buffer to the value and everything after.
	i.tags = i.tags[nv+1:]
	// Advance the cursor to the beginning of the first next key.
	// Ditto invariant.
	nk := bytes.IndexByte(i.tags, 0)
	// The value is everything so far.
	val = i.tags[:nk]
	// Truncate the buffer to the next key and everything after.
	i.tags = i.tags[nk+1:]
	return key, val, false
}

// redactTagValues redacts the values entirely. This is used when
// converting a redactable=false entry to redacted=true output.
func (f formattableTags) redactTagValues(preserveMarkers bool) (res formattableTags) {
	// heuristic: output is not longer than input.
	// (It could be in the case when a value string is shorter than the redacted marker.)
	res = make([]byte, 0, len(f))
	marker := redactedMarker
	if !preserveMarkers {
		marker = strippedMarker
	}
	fi := formattableTagsIterator{tags: []byte(f)}
	for {
		key, val, done := fi.next()
		if done {
			break
		}
		res = append(res, key...)
		res = append(res, 0)
		if len(val) > 0 {
			res = append(res, marker...)
		}
		res = append(res, 0)
	}
	return res
}

// formatToSafeWriter emits the tags to a safe writer, which means preserve
// redaction markers that were there to start with, if any, but be careful
// not to introduce imbalanced redaction markers.
func (f formattableTags) formatToSafeWriter(w interfaces.SafeWriter, redactable bool) {
	fi := formattableTagsIterator{tags: []byte(f)}
	for i := 0; ; i++ {
		key, val, done := fi.next()
		if done {
			break
		}
		if i > 0 {
			w.SafeRune(',')
		}
		// The key part is always considered safe.
		w.Print(redact.RedactableBytes(key))
		if len(val) > 0 {
			if len(key) != 1 {
				// We skip the `=` sign for 1-letter keys: we write "n1" in logs, not "n=1".
				w.SafeRune('=')
			}
			if redactable {
				// If the entry was redactable to start with, the value part
				// is redactable already and can be passed through as-is.
				w.Print(redact.RedactableBytes(val))
			} else {
				// Otherwise, the value part is unsafe and must be escaped.
				w.Print(string(val))
			}
		}
	}
}

// formatToBuffer emits the key=value pairs to the output buffer
// separated by commas.
func (f formattableTags) formatToBuffer(buf *buffer) {
	fi := formattableTagsIterator{tags: []byte(f)}
	for i := 0; ; i++ {
		key, val, done := fi.next()
		if done {
			break
		}
		if i > 0 {
			buf.WriteByte(',')
		}
		writeTagToBuffer(buf, key, val)
	}
}

// writeTagToBuffer writes the provided log tag key and value to buf.
// If the given key is empty or nil, this is a noop.
//
// The written format varies depending on the length of the given key.
// If len(key) == 1, we avoid separating the key and value with a '=',
// otherwise, we insert a '=' between the key and value. For example:
//
//	k = []byte("p"), 	  v = []byte("123") -> "p123"
//	k = []byte("tagKey"), v = []byte("456") -> "tagKey=456"
//
// If the value is empty, we still write the key with no accompanying
// value.
func writeTagToBuffer(buf *buffer, k, v []byte) {
	if len(k) == 0 {
		return
	}
	buf.Write(k)
	if len(v) > 0 {
		if len(k) != 1 {
			// We skip the `=` sign for 1-letter keys: we write "n1" in logs, not "n=1".
			buf.WriteByte('=')
		}
		buf.Write(v)
	}
}

// formatToBuffer emits the "key":"value" pairs to the output buffer
// separated by commas, in JSON. Special JSON characters in the
// keys/values get escaped.
func (f formattableTags) formatJSONToBuffer(buf *buffer) {
	fi := formattableTagsIterator{tags: []byte(f)}
	for i := 0; ; i++ {
		key, val, done := fi.next()
		if done {
			break
		}
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		escapeString(buf, string(key))
		buf.WriteString(`":"`)
		escapeString(buf, string(val))
		buf.WriteByte('"')
	}
}
