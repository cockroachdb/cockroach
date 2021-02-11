// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//go:generate sh generate.sh

package wkt

import "github.com/twpayne/go-geom"

// Unmarshal accepts a string and parses it to a geom.T.
func Unmarshal(wkt string) (geom.T, error) {
	wktlex := newWKTLex(wkt)
	wktParse(wktlex)
	if wktlex.lastErr != nil {
		return nil, wktlex.lastErr
	}
	return wktlex.ret, nil
}
