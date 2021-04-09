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
//   "{6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
//   "urn:uuid:6ba7b8109dad11d180b400c04fd430c8",
//   "urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8",
//	 "6ba7-b810-9dad-11d1-80b4-00c0-4fd4-30c8"
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
//   hyphenated := hyphen after any group of 4 hexdig
//   Ex.6ba7-b810-9dad-11d1-80b4-00c0-4fd4-30c8
//   Ex.6ba7-b810-9dad11d1-80b400c0-4fd4-30c8
//
//   uuid := hyphenated | hashlike | braced | urn
//
//   braced := '{' hyphenated '}' | '{' hashlike  '}'
//   urn := URN ':' UUID-NID ':' hyphenated
//
func (u *UUID) UnmarshalText(text []byte) error {
	l := len(text)
	stringifiedText := string(text)

	if l < 32 || l > 48 {
		return fmt.Errorf("uuid: incorrect UUID length: %s", text)
	} else if stringifiedText[0] == '{' && stringifiedText[l-1] == '}' {
		return u.decodeHyphenated(text[1 : l-1])
	} else if bytes.Equal(text[:9], urnPrefix) {
		return u.decodeHyphenated(text[9:l])
	} else {
		return u.decodeHyphenated(text)
	}
}

// decodeHashLike decodes UUID strings that are using the following format:
//  "6ba7b8109dad11d180b400c04fd430c8".
func (u *UUID) decodeHashLike(t []byte) error {
	src := t[:]
	dst := u[:]

	_, err := hex.Decode(dst, src)
	return err
}

// decodeHyphenated decodes UUID strings that are using the following format:
//  "6ba7-b810-9dad-11d1-80b4-00c0-4fd4-30c8"
//  "6ba7b810-9dad-11d1-80b400c0-4fd4-30c8"
func (u *UUID) decodeHyphenated(t []byte) error {
	l := len(t)
	if l < 32 || l > 40 {
		return fmt.Errorf("uuid: incorrect UUID format: %s", t)
	}

	hashLike := make([]byte, 32)
	countSinceHyphen := 0
	i := 0
	for _, c := range t {
		if i >= len(hashLike) {
			return fmt.Errorf("uuid: incorrect UUID format: %s", t)
		}
		if c == '-' {
			if countSinceHyphen == 0 || countSinceHyphen%4 != 0 {
				return fmt.Errorf("uuid: incorrect UUID format: %s", t)
			}
			countSinceHyphen = 0
			continue
		}
		hashLike[i] = c
		i++
		countSinceHyphen++
	}
	if i != len(hashLike) {
		return fmt.Errorf("uuid: incorrect UUID format: %s", t)
	}
	return u.decodeHashLike(hashLike)
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
