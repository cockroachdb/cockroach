// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// This file provides generic interfaces that allow tests to set up test tenants
// without importing the server package (avoiding circular dependencies). This

package serverutils

import "net/url"

type SessionType int

const (
	UnknownSession SessionType = iota
	SingleTenantSession
	MultiTenantSession
)

type TestURL struct {
	*url.URL
}

func NewTestURL(path string) TestURL {
	u, err := url.Parse(path)
	if err != nil {
		panic(err)
	}
	return TestURL{u}
}

// WithPath is a helper that allows the user of the `TestURL` to easily
// append paths to use for testing. Please be aware that your path will
// automatically be escaped for you, but if it includes any invalid hex
// escapes (eg: `%s`) it will fail silently and you'll get back a blank
// URL.
func (t *TestURL) WithPath(path string) *TestURL {
	newPath, err := url.JoinPath(t.Path, path)
	if err != nil {
		panic(err)
	}
	t.Path = newPath
	return t
}
