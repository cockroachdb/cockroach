// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package codec

func init() {
	Register(new(Dog))
	Register(new(Cat))
}

func (d Dog) GetTypeName() TypeName {
	return ResolveTypeName(d)
}

func (c Cat) GetTypeName() TypeName {
	return ResolveTypeName(c)
}
