// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import "github.com/cockroachdb/cockroach/pkg/util"

type onlyField struct {
	_ util.NoCopy
}

type firstField struct {
	_ util.NoCopy
	a int64
}

type middleField struct {
	a int64
	_ util.NoCopy // want `Illegal use of util.NoCopy - must be first field in struct`
	b int64
}

type lastField struct {
	a int64
	_ util.NoCopy // want `Illegal use of util.NoCopy - must be first field in struct`
}

type embeddedField struct {
	util.NoCopy // want `Illegal use of util.NoCopy - should not be embedded`
}

type multiField struct {
	_, _ util.NoCopy // want `Illegal use of util.NoCopy - should be included only once`
}

type namedField struct {
	noCopy util.NoCopy // want `Illegal use of util.NoCopy - should be unnamed`
}
