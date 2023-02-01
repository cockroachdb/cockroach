// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverpb

import (
	"context"

	"github.com/cockroachdb/errors"
)

// SafeValue implements the redact.SafeValue interface.
func (SnapshotRequest_Type) SafeValue() {}

// Error returns the error contained in the snapshot response, if any.
//
// The bool indicates whether this message uses the deprecated behavior of
// encoding an error as a string.
func (m *DelegateSnapshotResponse) Error() error {
	if m.Status != DelegateSnapshotResponse_ERROR {
		return nil
	}
	return errors.DecodeError(context.Background(), m.EncodedError)
}

// Error returns the error contained in the snapshot response, if any.
//
// The bool indicates whether this message uses the deprecated behavior of
// encoding an error as a string.
func (m *SnapshotResponse) Error() (deprecated bool, _ error) {
	if m.Status != SnapshotResponse_ERROR {
		return false, nil
	}
	if m.EncodedError.IsSet() {
		return false, errors.DecodeError(context.Background(), m.EncodedError)
	}
	return true, errors.Newf("%s", m.DeprecatedMessage)
}
