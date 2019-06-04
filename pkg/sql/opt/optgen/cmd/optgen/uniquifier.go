// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package main

import "fmt"

// uniquifier generates a unique variable name from an input string by
// appending an incrementing counter in case of collisions with previously
// added names.
type uniquifier struct {
	seen map[string]struct{}
}

func (u *uniquifier) init() {
	u.seen = make(map[string]struct{})
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
