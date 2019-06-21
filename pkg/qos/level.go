// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package qos

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
)

// Class indicates traffic of a certain quality.
// The values of Class are explicitly defined as ClassHigh, ClassDefault, and
// ClassLow. Class represents a client intention to prioritize this request
// relative to requests of different Classes. Each Class is broken into NumShard
// Shards which enables the system to make finer granularity admission control
// decisions than if it could only rely uniformly on Class.
type Class uint8

const (
	// ClassHigh is the highest quality of service class.
	ClassHigh Class = NumClasses - 1 - iota
	// ClassDefault is the default quality of service class.
	ClassDefault
	// ClassLow is the lowest quality of service class.
	ClassLow

	// NumClasses is the total number of levels.
	NumClasses = iota // 3
)

// IsValid is true if Class is in [0, NumClasses).
func (l Class) IsValid() bool {
	return l < NumClasses
}

// Shard indicates a shard within a Class.
// A shard subdivides a class generally based on a constant property of the
// client connection from which the corresponding request originates. Shard
// provides the system with a finer granularity for admission control decisions.
// Shard helps the distributed system to mitigate the challenges due to
// "multiple overload" whereby during the processing of a single client request
// may need to visit more than downstream server which is overloaded. If traffic
// of a given Class were uniformly affected then even if each individual server
// were to only backpressure a small fraction of incident requests, a large
// fraction could observe some backpressure. Using a Shard in conjunction with a
// Class allows servers to agree on which traffic of a Class should experience
// backpressure without explicit coordination.
type Shard uint8

// NumShards is the total number of logical shards.
// The current value is arbitrary and could change over time. Given a small
// number of classes, having a large number of shards gives the system the
// most granularity on which to make qos decisions.
const NumShards = 128

// IsValid is true if Shard is in [0, NumShards).
func (s Shard) IsValid() bool {
	return s < NumShards
}

// Level represents a quality of service level.
//
// The qos space is an ordered tuple of Class and Shard where Class is the
// higher order field (i.e. if a Level has a larger Class value than another
// it is larger). In general, Class represents user intention of
//
// The quality of service space is broken up into explicit classes which are
// subdivided into shards. This tuple space allows nodes to make indepedent
// admission decisions which will cooperate to backpressure traffic in an
// orderly manner without explicit coordination.
//
// While its fields are uint8, the values for a valid Class are contained in
// [0, NumClasses) and the values for a valid Shard are contained in
// [0, NumShards). A Level which contains a Class or Shard outside of that
// range is not valid. Levels which are not valid will cause a panic during
// encoding. Encoded values however are spread over the uint8 space to allow
// for future versions to use different numbers of classes and shards in a
// forward and backward compatible way.
type Level struct {

	// Class indicates the level associated with this priority.
	// For a valid Level its value lies in [0, NumClasses).
	Class Class

	// Shard indicates the priority shard within this level.
	// For a valid Level its value lies in [0, NumShards).
	Shard Shard
}

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
func (l Level) Less(other Level) bool {
	if l.Class == other.Class {
		return l.Shard < other.Shard
	}
	return l.Class < other.Class
}

// Encode encodes a priority to a uint32.
// Encoded priorities can be compared using normal comparison operators.
func (l Level) Encode() uint32 {
	return uint32(encodeClass(l.Class))<<8 | uint32(encodeShard(l.Shard))
}

// String returns a string formatted as Class:Shard where Shard is always an
// integer and Class uses a shorthand string if it is valid or an intger if not.
func (l Level) String() string {
	if l.Class >= NumClasses {
		return strconv.Itoa(int(l.Class)) + ":" + strconv.Itoa(int(l.Shard))
	}
	return classStrings[l.Class] + ":" + strconv.Itoa(int(l.Shard))
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

// classStep and shardStep are used in encoding a decoding.
// When encoded the Class and Shard values are spread over the space of uint8
// in order to accommodate later changes to NumClasses or NumShards.
const (
	classStep = math.MaxUint8 / (NumClasses - 1)
	shardStep = math.MaxUint8 / (NumShards - 1)
)

func encodeClass(l Class) uint8 {
	if !l.IsValid() {
		panic(fmt.Errorf("cannot encode invalid level %d", l))
	}
	const minEncodedClass = math.MaxUint8 % classStep
	return minEncodedClass + uint8(l*classStep)
}

func encodeShard(s Shard) uint8 {
	if !s.IsValid() {
		panic(fmt.Errorf("cannot encode invalid shard %d", s))
	}
	const minEncodedShard = math.MaxUint8 % shardStep
	return minEncodedShard + uint8(s*shardStep)
}

func decodeShard(e uint32) Shard {
	return Shard(e) / shardStep
}

func decodeClass(e uint32) Class {
	return Class(e>>8) / classStep
}

var classStrings = [NumClasses]string{
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
