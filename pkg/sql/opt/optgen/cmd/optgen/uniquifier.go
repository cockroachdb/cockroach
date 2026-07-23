// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import "fmt"

// uniquifier generates a unique variable name from an input string by
// appending an incrementing counter in case of collisions with previously
// added names.
type uniquifier struct {
	seen map[string]struct{}
}

func (u *uniquifier) init() {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*u = uniquifier{
		seen: make(map[string]struct{}),
	}
}

func (u *uniquifier) makeUnique(s string) string {
	try := s
	for i := 2; ; i++ {
		_, ok := u.seen[try]
		if !ok {
			u.seen[try] = struct{}{}
			return try
		}

		try = fmt.Sprintf("%s%d", s, i)
	}
}
