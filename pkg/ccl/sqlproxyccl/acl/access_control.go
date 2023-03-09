// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package acl

import "github.com/cockroachdb/cockroach/pkg/util/timeutil"

// ConnectionTags contains connection properties to match against the denylist.
type ConnectionTags struct {
	IP      string
	Cluster string
}

type AccessController interface {
	CheckConnection(ConnectionTags, timeutil.TimeSource) error
}
