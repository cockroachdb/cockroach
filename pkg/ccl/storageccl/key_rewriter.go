// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package storageccl

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// KeyRewriter is an matcher for an ordered list of pairs of byte prefix rewrite
// rules. For dependency reasons, the implementation of the matching is here,
// but the interesting constructor is in sqlccl.
type KeyRewriter []roachpb.KeyRewrite

// RewriteKey modifies key using the first matching rule and returns it. If no
// rules matched, returns false and the original input key.
func (kr KeyRewriter) RewriteKey(key []byte) ([]byte, bool) {
	for _, rewrite := range kr {
		if bytes.HasPrefix(key, rewrite.OldPrefix) {
			if len(rewrite.OldPrefix) == len(rewrite.NewPrefix) {
				copy(key[:len(rewrite.OldPrefix)], rewrite.NewPrefix)
				return key, true
			}
			// TODO(dan): Special case when key's cap() is enough.
			newKey := make([]byte, 0, len(rewrite.NewPrefix)+len(key)-len(rewrite.OldPrefix))
			newKey = append(newKey, rewrite.NewPrefix...)
			newKey = append(newKey, key[len(rewrite.OldPrefix):]...)
			return newKey, true
		}
	}
	return key, false
}
