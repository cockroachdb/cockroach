// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mvcceval

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// MaybeConditionFailedError returns a non-nil ConditionFailedError if the
// expected value does not match the actual value. If allowNoExisting is true,
// then a non-existent actual value is allowed even when expected-value is
// non-empty.
func MaybeConditionFailedError(
	expBytes []byte, actVal *roachpb.Value, actValPresent bool, allowNoExisting bool,
) *kvpb.ConditionFailedError {
	expValPresent := len(expBytes) != 0
	if expValPresent && actValPresent {
		if !bytes.Equal(expBytes, actVal.TagAndDataBytes()) {
			return &kvpb.ConditionFailedError{
				ActualValue: actVal,
			}
		}
	} else if expValPresent != actValPresent && (actValPresent || !allowNoExisting) {
		return &kvpb.ConditionFailedError{
			ActualValue: actVal,
		}
	}
	return nil
}
