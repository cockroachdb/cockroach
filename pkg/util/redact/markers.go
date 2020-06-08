// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package redact

// RedactableString is a string that contains a mix of safe and unsafe
// bits of data, but where it is known that unsafe bits are enclosed
// by redaction markers ‹ and ›, and occurrences of the markers
// inside the original data items have been escaped.
//
// Instances of RedactableString should not be constructed directly;
// instead use the facilities from print.go (Sprint, Sprintf)
// or the methods below.
type RedactableString string

// StripMarkers removes the redaction markers from the
// RedactableString. This returns an unsafe string where all safe and
// unsafe bits are mixed together.
func (s RedactableString) StripMarkers() string {
	return reStripMarkers.ReplaceAllString(string(s), "")
}

// Redact replaces all occurrences of unsafe substrings by the
// “Redacted” marker, ‹×›. The result string is still safe.
func (s RedactableString) Redact() RedactableString {
	return RedactableString(reStripSensitive.ReplaceAllString(string(s), redactedS))
}

// ToBytes converts the string to a byte slice.
func (s RedactableString) ToBytes() RedactableBytes {
	return RedactableBytes([]byte(string(s)))
}

// RedactableBytes is like RedactableString but is a byte slice.
//
// Instances of RedactableBytes should not be constructed directly;
// instead use the facilities from print.go (Sprint, Sprintf)
// or the methods below.
type RedactableBytes []byte

// StripMarkers removes the redaction markers from the
// RedactableBytes. This returns an unsafe string where all safe and
// unsafe bits are mixed together.
func (s RedactableBytes) StripMarkers() []byte {
	return reStripMarkers.ReplaceAll([]byte(s), nil)
}

// Redact replaces all occurrences of unsafe substrings by the
// “Redacted” marker, ‹×›.
func (s RedactableBytes) Redact() RedactableBytes {
	return RedactableBytes(reStripSensitive.ReplaceAll(s, redactedBytes))
}

// ToString converts the byte slice to a string.
func (s RedactableBytes) ToString() RedactableString {
	return RedactableString(string([]byte(s)))
}

// EscapeBytes escapes markers inside the given byte slice and encloses
// the entire byte slice between redaction markers.
func EscapeBytes(s []byte) RedactableBytes {
	s = reStripMarkers.ReplaceAll(s, escapeBytes)
	enclosed := make([]byte, 0, len(s)+len(startRedactableBytes)+len(endRedactableBytes))
	enclosed = append(enclosed, startRedactableBytes...)
	enclosed = append(enclosed, s...)
	enclosed = append(enclosed, endRedactableBytes...)
	return RedactableBytes(enclosed)
}

// StartMarker returns the start delimiter for an unsafe string.
func StartMarker() []byte { return startRedactableBytes }

// EndMarker returns the end delimiter for an unsafe string.
func EndMarker() []byte { return endRedactableBytes }

// RedactedMarker returns the special string used by Redact.
func RedactedMarker() []byte { return redactedBytes }

// EscapeMarkers escapes the special delimiters from the provided
// byte slice.
func EscapeMarkers(s []byte) []byte {
	return reStripMarkers.ReplaceAll(s, escapeBytes)
}
