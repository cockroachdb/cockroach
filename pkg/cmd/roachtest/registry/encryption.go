// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registry

import "fmt"

// EncryptionSupport encodes the relationship of a test with
// encryption-at-rest. Tests can either opt-in to metamorphic
// encryption, or require that encryption is always on or always off
// (default).
type EncryptionSupport int

func (es EncryptionSupport) String() string {
	switch es {
	case EncryptionAlwaysEnabled:
		return "always-enabled"
	case EncryptionAlwaysDisabled:
		return "always-disabled"
	case EncryptionMetamorphic:
		return "metamorphic"
	default:
		return fmt.Sprintf("unknown-%d", es)
	}
}

const (
	// EncryptionAlwaysDisabled indicates that the test requires
	// encryption to be disabled. The test will only run on clusters
	// with encryption disabled.
	EncryptionAlwaysDisabled = EncryptionSupport(iota)
	// EncryptionAlwaysEnabled indicates that the test requires
	// encryption to be enabled. The test will only run on clusters
	// with encryption enabled.
	EncryptionAlwaysEnabled
	// EncryptionMetamorphic indicates that a test opted-in to
	// metamorphic encryption. Whether the test runs on a cluster with
	// encryption enabled depends on the probability passed to
	// --metamorphic-encryption-probability.
	EncryptionMetamorphic
)
