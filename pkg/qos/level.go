// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package qos

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
)

// Level represents a quality of service level.
// The quality of service space is broken up into explicit classes which are
// subdivided into shards. This tuple space allows nodes to make indepedent
// admission decisions which will cooperate to backpressure traffic in an
// orderly manner without explicit coordination.
//
// While its fields are uint8, the values are dense in (NumClasses, 0] and
// (NumShards, 0] than being spread out over the uint8 space. A Level which
// contains a Class or Shard outside of that range is not valid. Levels which
// are not valid will cause a panic during encoding. Encoded values however are
// spread over the uint8 space to allow for future versions to use different
// numbers of classes and shards in a forward and backward compatible way.
type Level struct {

	// Class indicates the level associated with this priority.
	// For a valid Level its value lies in (NumClasses, 0].
	Class Class

	// Shard indicates the priority shard within this level.
	// For a valid Level its value lies in (NumShards, 0].
	Shard Shard
}

// Class indicates traffic of a certain quality.
type Class uint8

// Shard indicates a shard within a Class.
type Shard uint8

// Decode decodes a priority from a uint32.
// The high 16 bits are ignored.
func Decode(p uint32) Level {
	return Level{
		Class: decodeClass(p),
		Shard: decodeShard(p),
	}
}

// IsValid return true if p has a valid value.
func (l Level) IsValid() bool {
	return l.Class.IsValid() && l.Shard.IsValid()
}

// IsValid is true if Class is in (NumClasses, 0].
func (l Class) IsValid() bool {
	return l < NumClasses
}

// IsValid is true if Shard is in (NumShards, 0].
func (s Shard) IsValid() bool {
	return s < NumShards
}

// Encode encodes a priority to a uint32.
// Encoded priorities can be compared using normal comparison operators.
func (l Level) Encode() uint32 {
	return uint32(encodeClass(l.Class))<<8 | uint32(encodeShard(l.Shard))
}

const (
	levelMask = 0x0000FF00
	shardMask = 0x000000FF
)

func encodeClass(l Class) uint8 {
	if !l.IsValid() {
		panic(fmt.Errorf("cannot encode invalid level %d", l))
	}
	return uint8(255 - ((NumClasses - 1 - l) * levelStep))
}

func encodeShard(s Shard) uint8 {
	if !s.IsValid() {
		panic(fmt.Errorf("cannot encode invalid shard %d", s))
	}
	return uint8(255 - ((NumShards - 1 - s) * shardStep))
}

func decodeShard(e uint32) Shard {
	return Shard(e) / shardStep
}

func decodeClass(e uint32) Class {
	return Class(e>>8) / levelStep
}

// Dec returns the next lower priority value unless p is the minimum value
// in which case p is returned.
func (l Level) Dec() Level {
	if l.Shard > 0 {
		l.Shard--
	} else if l.Class > 0 {
		l.Class--
		l.Shard = NumShards - 1
	}
	return l
}

// Inc returns the next higher priority value unless p is the maximum value
// in which case p is returned.
func (l Level) Inc() Level {
	if l.Shard < NumShards-1 {
		l.Shard++
	} else if l.Class < NumClasses-1 {
		l.Class++
		l.Shard = 0
	}
	return l
}

// Less returns true if p is less than other.
func (l Level) Less(other Level) (r bool) {
	if l.Class == other.Class {
		return l.Shard < other.Shard
	}
	return l.Class < other.Class
}

const (
	// NumClasses is the total number of levels.
	NumClasses = 3
	// NumShards is tje total number of logical shards.
	NumShards = 128

	levelStep = math.MaxUint8 / (NumClasses - 1)
	shardStep = math.MaxUint8 / (NumShards - 1)
)

const (
	// ClassHigh is the highest quality of service class.
	ClassHigh Class = NumClasses - 1 - iota
	// ClassDefault is the default quality of service class.
	ClassDefault
	// ClassLow is the lowest quality of service class.
	ClassLow
)

var levelStrings = [NumClasses]string{
	ClassHigh:    "h",
	ClassDefault: "d",
	ClassLow:     "l",
}

var marshaledText [NumClasses][NumShards][textLen]byte

// textLen is the length of a hex-encoded Level.
const textLen = 4 // 2 * 2 bytes = 4

func init() {
	for c := Class(0); c < NumClasses; c++ {
		for s := Shard(0); s < NumShards; s++ {
			marshaledText[c][s] = levelToText(Level{c, s})
		}
	}
}

func levelToText(l Level) [textLen]byte {
	var data [2]byte
	var out [4]byte
	data[0] = encodeClass(l.Class)
	data[1] = encodeShard(l.Shard)
	if n := hex.Encode(out[:], data[:]); n != len(out) {
		panic(fmt.Errorf("expected to encode %d bytes, got %d", n, len(out)))
	}
	return out
}

// String returns a string
func (l Level) String() string {
	if l.Class >= NumClasses {
		return strconv.Itoa(int(l.Class)) + ":" + strconv.Itoa(int(l.Shard))
	}
	return levelStrings[l.Class] + ":" + strconv.Itoa(int(l.Shard))
}

// MarshalText implements encoding.TextMarshaler.
func (l Level) MarshalText() ([]byte, error) {
	if !l.IsValid() {
		return nil, fmt.Errorf("cannot marshal invalid Level (%d, %d) to text", l.Class, l.Shard)
	}
	return marshaledText[l.Class][l.Shard][:], nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (l *Level) UnmarshalText(data []byte) error {
	if len(data) != textLen {
		return fmt.Errorf("invalid data length %d, expected %d", len(data), textLen)
	}
	var decoded [2]byte
	if _, err := hex.Decode(decoded[:], data); err != nil {
		return err
	}
	*l = Decode(uint32(binary.BigEndian.Uint16(decoded[:])))
	return nil
}
