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
	raw keyType = iota
	human
	rangeID
)

var _keyTypes []string

func keyTypes() []string {
	if _keyTypes == nil {
		for i := 0; i+1 < len(_keyType_index); i++ {
			_keyTypes = append(_keyTypes, _keyType_name[_keyType_index[i]:_keyType_index[i+1]])
		}
	}
	return _keyTypes
}

func parseKeyType(value string) (keyType, error) {
	for i, typ := range keyTypes() {
		if strings.EqualFold(value, typ) {
			return keyType(i), nil
		}
	}
	return 0, fmt.Errorf("unknown key type '%s'", value)
}

type mvccKey engine.MVCCKey

func (k *mvccKey) String() string {
	return engine.MVCCKey(*k).String()
}

func (k *mvccKey) Set(value string) error {
	var typ keyType
	var keyStr string
	i := strings.IndexByte(value, ':')
	if i == -1 {
		keyStr = value
	} else {
		var err error
		typ, err = parseKeyType(value[:i])
		if err != nil {
			return err
		}
		keyStr = value[i+1:]
	}

	switch typ {
	case raw:
		*k = mvccKey(engine.MakeMVCCMetadataKey(roachpb.Key(keyStr)))
	case human:
		key, err := keys.UglyPrint(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(engine.MakeMVCCMetadataKey(key))
	case rangeID:
		fromID, err := parseRangeID(keyStr)
		if err != nil {
			return err
		}
		*k = mvccKey(engine.MakeMVCCMetadataKey(keys.MakeRangeIDPrefix(fromID)))
	default:
		return fmt.Errorf("unknown key type %s", typ)
	}

	return nil
}

func (k *mvccKey) Type() string {
	return "engine.MVCCKey"
}

type debugContext struct {
	startKey, endKey engine.MVCCKey
	values           bool
	sizes            bool
}
