// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keysutil

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// UglyPrint is a partial right inverse to PrettyPrint: it takes a key
// formatted for human consumption and attempts to translate it into a
// roachpb.Key. Not all key types are supported and no optimization has been
// performed. This is intended for use in debugging only.
func UglyPrint(input string) (_ roachpb.Key, rErr error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				rErr = err
				return
			}
			rErr = errors.Errorf("%v", r)
		}
	}()

	origInput := input
	var output roachpb.Key

	mkErr := func(err error) (roachpb.Key, error) {
		if err == nil {
			err = errIllegalInput
		}
		err = errors.Errorf(`can't parse "%s" after reading %s: %s`,
			input, origInput[:len(origInput)-len(input)], err)
		return nil, &keys.ErrUglifyUnsupported{Wrapped: err}
	}

	var entries []keys.DictEntry // nil if not pinned to a subrange
outer:
	for len(input) > 0 {
		if entries != nil {
			for _, v := range entries {
				if strings.HasPrefix(input, v.Name) {
					input = input[len(v.Name):]
					if v.PSFunc == nil {
						return mkErr(nil)
					}
					remainder, key := v.PSFunc(input)
					input = remainder
					output = append(output, key...)
					entries = nil
					continue outer
				}
			}
			return nil, &keys.ErrUglifyUnsupported{
				Wrapped: errors.New("known key, but unsupported subtype"),
			}
		}
		for _, v := range keys.ConstKeyDict {
			if strings.HasPrefix(input, v.Name) {
				output = append(output, v.Value...)
				input = input[len(v.Name):]
				continue outer
			}
		}
		for _, v := range keys.KeyDict {
			if strings.HasPrefix(input, v.Name) {
				// No appending to output yet, the dictionary will take care of
				// it.
				input = input[len(v.Name):]
				entries = v.Entries
				continue outer
			}
		}
		return mkErr(errors.New("can't handle key"))
	}
	if out := keys.PrettyPrint(nil /* valDirs */, output); out != origInput {
		return nil, errors.Errorf("constructed key deviates from original: %s vs %s", out, origInput)
	}
	return output, nil
}

var errIllegalInput = errors.New("illegal input")
