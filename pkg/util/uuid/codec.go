// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright (C) 2013-2018 by Maxim Bublis <b@codemonkey.ru>
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-gofrs.txt.

// This code originated in github.com/gofrs/uuid.

package uuid

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// FromBytes returns a UUID generated from the raw byte slice input.
// It will return an error if the slice isn't 16 bytes long.
func FromBytes(input []byte) (UUID, error) {
	u := UUID{}
	err := u.UnmarshalBinary(input)
	return u, err
}

// FromBytesOrNil returns a UUID generated from the raw byte slice input.
// Same behavior as FromBytes(), but returns uuid.Nil instead of an error.
func FromBytesOrNil(input []byte) UUID {
	uuid, err := FromBytes(input)
	if err != nil {
		return Nil
	}
	return uuid
}

// FromString returns a UUID parsed from the input string.
// Input is expected in a form accepted by UnmarshalText.
func FromString(input string) (UUID, error) {
	u := UUID{}
	err := u.UnmarshalText([]byte(input))
	return u, err
}

// FromStringOrNil returns a UUID parsed from the input string.
// Same behavior as FromString(), but returns uuid.Nil instead of an error.
func FromStringOrNil(input string) UUID {
	uuid, err := FromString(input)
	if err != nil {
		return Nil
	}
	return uuid
}

// MarshalText implements the encoding.TextMarshaler interface.
// The encoding is the same as returned by the String() method.
func (u UUID) MarshalText() ([]byte, error) {
	return []byte(u.String()), nil
}

// UnmarshalText implements the encoding.TextUnmarshaler interface.
// Following formats are supported:
//
//   "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
//   "{6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
//   "urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8"
//   "6ba7b8109dad11d180b400c04fd430c8"
//   "{6ba7b8109dad11d180b400c04fd430c8}",
//   "urn:uuid:6ba7b8109dad11d180b400c04fd430c8"
//
// ABNF for supported UUID text representation follows:
//
//   URN := 'urn'
//   UUID-NID := 'uuid'
//
//   hexdig := '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' |
//             'a' | 'b' | 'c' | 'd' | 'e' | 'f' |
//             'A' | 'B' | 'C' | 'D' | 'E' | 'F'
//
//   hexoct := hexdig hexdig
//   2hexoct := hexoct hexoct
//   4hexoct := 2hexoct 2hexoct
//   6hexoct := 4hexoct 2hexoct
//   12hexoct := 6hexoct 6hexoct
//
//   hashlike := 12hexoct
//   canonical := 4hexoct '-' 2hexoct '-' 2hexoct '-' 6hexoct
//
//   plain := canonical | hashlike
//   uuid := canonical | hashlike | braced | urn
//
//   braced := '{' plain '}' | '{' hashlike  '}'
//   urn := URN ':' UUID-NID ':' plain
//
func (u *UUID) UnmarshalText(text []byte) error {
	switch len(text) {
	case 32:
		return u.decodeHashLike(text)
	case 34, 38:
		return u.decodeBraced(text)
	case 36:
		return u.decodeCanonical(text)
	case 41, 45:
		return u.decodeURN(text)
	default:
		return fmt.Errorf("uuid: incorrect UUID length: %s", text)
	}
}

// decodeCanonical decodes UUID strings that are formatted as defined in RFC-4122 (section 3):
// "6ba7b810-9dad-11d1-80b4-00c04fd430c8".
func (u *UUID) decodeCanonical(t []byte) error {
	if t[8] != '-' || t[13] != '-' || t[18] != '-' || t[23] != '-' {
		return fmt.Errorf("uuid: incorrect UUID format %s", t)
	}

	src := t
	dst := u[:]

	for i, byteGroup := range byteGroups {
		if i > 0 {
			src = src[1:] // skip dash
		}
		_, err := hex.Decode(dst[:byteGroup/2], src[:byteGroup])
		if err != nil {
			return err
		}
		src = src[byteGroup:]
		dst = dst[byteGroup/2:]
	}

	return nil
}

// decodeHashLike decodes UUID strings that are using the following format:
//  "6ba7b8109dad11d180b400c04fd430c8".
func (u *UUID) decodeHashLike(t []byte) error {
	src := t[:]
	dst := u[:]

	_, err := hex.Decode(dst, src)
	return err
}

// decodeBraced decodes UUID strings that are using the following formats:
//  "{6ba7b810-9dad-11d1-80b4-00c04fd430c8}"
//  "{6ba7b8109dad11d180b400c04fd430c8}".
func (u *UUID) decodeBraced(t []byte) error {
	l := len(t)

	if t[0] != '{' || t[l-1] != '}' {
		return fmt.Errorf("uuid: incorrect UUID format %s", t)
	}

	return u.decodePlain(t[1 : l-1])
}

// decodeURN decodes UUID strings that are using the following formats:
//  "urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8"
//  "urn:uuid:6ba7b8109dad11d180b400c04fd430c8".
func (u *UUID) decodeURN(t []byte) error {
	total := len(t)

	urnUUIDPrefix := t[:9]

	if !bytes.Equal(urnUUIDPrefix, urnPrefix) {
		return fmt.Errorf("uuid: incorrect UUID format: %s", t)
	}

	return u.decodePlain(t[9:total])
}

// decodePlain decodes UUID strings that are using the following formats:
//  "6ba7b810-9dad-11d1-80b4-00c04fd430c8" or in hash-like format
//  "6ba7b8109dad11d180b400c04fd430c8".
func (u *UUID) decodePlain(t []byte) error {
	switch len(t) {
	case 32:
		return u.decodeHashLike(t)
	case 36:
		return u.decodeCanonical(t)
	default:
		return fmt.Errorf("uuid: incorrrect UUID length: %s", t)
	}
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
func (u UUID) MarshalBinary() ([]byte, error) {
	return u.bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
// It will return an error if the slice isn't 16 bytes long.
func (u *UUID) UnmarshalBinary(data []byte) error {
	if len(data) != Size {
		return fmt.Errorf("uuid: UUID must be exactly 16 bytes long, got %d bytes", len(data))
	}
	copy(u[:], data)

	return nil
}
