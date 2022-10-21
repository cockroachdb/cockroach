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
// to a cluster version, with the intention that when the cluster version
// falls under the min supported version and is in the process of being
// removed, associated clean-up can happen.
//
// This is helpful when there is otherwise no reason to tie the code whose
// cleanup is desired to the version. For example, when a proto field becomes
// deprecated, there may be a version gate in the code somewhere, but it may not
// refer directly to the proto field; and yet one may desire removing that the
// proto field exactly when the version disappears.
func Refer(v Key, subj ...interface{}) {
	_ = 0 // no-op
}
