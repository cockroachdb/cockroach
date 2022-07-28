// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package connectionpb

import "github.com/cockroachdb/errors"

// Type returns the ConnectionType of the receiver.
func (d *ConnectionDetails) Type() ConnectionType {
	switch t := d.Details.(type) {
	case *ConnectionDetails_Nodelocal:
		return TypeStorage
	case *ConnectionDetails_GCSKMS:
		return TypeKMS
	default:
		panic(errors.AssertionFailedf("ConnectionDetails.Type called on a details with an unknown type: %T", t))
	}
}
