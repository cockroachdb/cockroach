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
// encryption-at-rest. Tests can specify their encryption support so
// that if tests are run with encryption enabled (--encrypt=true), for
// example, they can be skipped.
type EncryptionSupport int

func (es EncryptionSupport) String() string {
	switch es {
	case EncryptionAllowed:
		return "allowed"
	case EncryptionRequired:
		return "required"
	case EncryptionDisabled:
		return "disabled"
	default:
		return fmt.Sprintf("unknown-%d", es)
	}
}

const (
	// EncryptionDisabled indicates that a test requires encryption to
	// be able to run. Even if --encrypt=true is passed, the cluster
	// will still have encryption disabled (default).
	EncryptionDisabled = EncryptionSupport(iota)
	// EncryptionAllowed indicates that a test can run with or without
	// encryption. When --encrypt=random is passed, the cluster where
	// the test runs may or may not have encryption enabled.
	EncryptionAllowed
	// EncryptionRequired indicates that a test requires encryption to
	// be able to run. Even if --encrypt=false is passed, the cluster
	// will still have encryption enabled.
	EncryptionRequired
)
