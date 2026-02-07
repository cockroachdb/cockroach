// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package clusterversion

// Refer allows tying one or more objects (or even a code comment, etc)
// to a cluster version, to denote that these objects that be cleaned-up
// once min supported version moves above the referred-to version.
//
// This is helpful when there is otherwise no way to tie the code whose
// cleanup is desired to a particular version. For example, when a proto
// field becomes deprecated, there may be a version gate in the code
// somewhere, but it may not refer directly to the proto field; yet one
// may desire removing that the proto field exactly when the version
// disappears.
//
// Usage:
// - version:
//    - should be a base version const, i.e. V22_1, V22_2, etc.
//    - is the maximum version where we still need the field, i.e.
//      when we delete `version`, we can delete `obj`.
// - obj:
//    - is the object(s) that should remain in the code as long as
//    `version` exists.
//
// Example:
//
// (1) In your code you have:
//
//    // We need to keep it for 22.1, but can remove it once we're on 22.2+ binaries
//    const NeededFor22_1 = "blah"
//
// (2) Use Refer to map object to the last required version, i.e.:
//
//    func init() {
//      // 22.1 is the maximum version where we still need the field,
//      // i.e. when we delete 22.1, we can delete the field.
//      clusterversion.Refer(clusterversion.V22_1, NeededFor22_1)
//    }
//
func Refer(version Key, obj ...interface{}) {
	_ = 0 // no-op
}
