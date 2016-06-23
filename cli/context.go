// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package cli

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
)

// statementsValue is an implementation of pflag.Value that appends any
// argument to a slice.
type statementsValue []string

func (s *statementsValue) String() string {
	return strings.Join(*s, ";")
}

func (s *statementsValue) Type() string {
	return "statementsValue"
}

func (s *statementsValue) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type cliContext struct {
	// Embed the base context.
	*base.Context

	// prettyFmt indicates whether tables should be pretty-formatted in
	// the output during non-interactive execution.
	prettyFmt bool
}

func (ctx *cliContext) InitCLIDefaults() {
	ctx.prettyFmt = false
}

type sqlContext struct {
	// Embed the cli context.
	*cliContext

	// execStmts is a list of statements to execute.
	execStmts statementsValue
}

type keyType int

//go:generate stringer -type=keyType
const (
	keyRaw keyType = iota
	keyPretty
	keyRangeID
)

type key struct {
	key string
	typ keyType
}

func (k key) roachKey(defacto engine.MVCCKey) (engine.MVCCKey, error) {
	switch k.typ {
	case keyRaw:
		if len(k.key) > 0 {
			return engine.MakeMVCCMetadataKey(roachpb.Key(k.key)), nil
		}
		return defacto, nil
	case keyPretty:
		key, err := keys.UglyPrint(k.key)
		if err != nil {
			return engine.MVCCKey{}, err
		}
		return engine.MakeMVCCMetadataKey(key), nil
	case keyRangeID:
		fromID, err := parseRangeID(k.key)
		if err != nil {
			return engine.MVCCKey{}, err
		}
		return engine.MakeMVCCMetadataKey(keys.MakeRangeIDPrefix(fromID)), nil
	default:
		return engine.MVCCKey{}, fmt.Errorf("unknown key type %s", k.typ)
	}
}

func (k *key) String() string {
	return k.key + k.typ.String()
}

func (k *key) Set(value string) error {
	switch i := strings.LastIndexByte(value, ':'); i {
	case -1:
		return fmt.Errorf("malformed key '%s', expected <key>:<type>", value)
	default:
		k.key = value[:i]
		for j := 0; j+1 < len(_keyType_index); j++ {
			if strings.EqualFold(value[i:], _keyType_name[_keyType_index[j]:_keyType_index[j+1]]) {
				k.typ = keyType(j)
				return nil
			}
		}
		return fmt.Errorf("unknown key type '%s'", value[i:])
	}
}

func (k *key) Type() string {
	return "key"
}

type debugContext struct {
	startKey, endKey key
	values           bool
}
