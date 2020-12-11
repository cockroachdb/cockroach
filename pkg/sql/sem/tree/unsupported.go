// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

var _ error = &Unsupported{}

// Unsupported is an error object which is returned by some unimplemented SQL
// statements. It is currently only used to skip over PGDUMP statements during
// an import.
type Unsupported struct {
	Err                    error
	SkipDuringImportPGDump bool
	FeatureName            string
}

func (u *Unsupported) Error() string {
	return u.Err.Error()
}
