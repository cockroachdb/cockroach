// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"github.com/bufbuild/buf/private/pkg/normalpath"
)

// Mapper is a path mapper.
//
// This will cause a Bucket to operate as if the Mapper has all paths mapped.
type Mapper interface {
	// Map maps the path to the full path.
	//
	// The path is expected to be normalized and validated.
	// The returned path is expected to be normalized and validated.
	// If the path cannot be mapped, this returns false.
	MapPath(path string) (string, bool)
	// Map maps the prefix to the full prefix.
	//
	// The path is expected to be normalized and validated.
	// The returned path is expected to be normalized and validated.
	// If the path cannot be mapped, this returns false.
	MapPrefix(prefix string) (string, bool)
	// UnmapFullPath maps the full path to the path.
	//
	// Returns false if the full path does not apply.
	// The path is expected to be normalized and validated.
	// The returned path is expected to be normalized and validated.
	UnmapFullPath(fullPath string) (string, bool, error)
	isMapper()
}

// MapOnPrefix returns a Mapper that will map the Bucket as if it was created on the given prefix.
//
// The prefix is expected to be normalized and validated.
func MapOnPrefix(prefix string) Mapper {
	return prefixMapper{prefix}
}

// MapChain chains the mappers.
//
// If any mapper does not match, this stops checking Mappers and returns
// an empty path and false. This is as opposed to MatchAnd, that runs
// every Matcher and returns the path regardless.
//
// If the Mappers are empty, a no-op Mapper is returned.
// If there is more than one Mapper, the Mappers are called in order
// for UnmapFullPath, with the order reversed for MapPath and MapPrefix.
//
// That is, order these assuming you are starting with a full path and
// working to a path.
func MapChain(mappers ...Mapper) Mapper {
	switch len(mappers) {
	case 0:
		return nopMapper{}
	case 1:
		return mappers[0]
	default:
		return chainMapper{mappers}
	}
}

// ***** private *****

type prefixMapper struct {
	prefix string
}

func (p prefixMapper) MapPath(path string) (string, bool) {
	return normalpath.Join(p.prefix, path), true
}

func (p prefixMapper) MapPrefix(prefix string) (string, bool) {
	return normalpath.Join(p.prefix, prefix), true
}

func (p prefixMapper) UnmapFullPath(fullPath string) (string, bool, error) {
	if !normalpath.EqualsOrContainsPath(p.prefix, fullPath, normalpath.Relative) {
		return "", false, nil
	}
	path, err := normalpath.Rel(p.prefix, fullPath)
	if err != nil {
		return "", false, err
	}
	return path, true, nil
}

func (prefixMapper) isMapper() {}

type chainMapper struct {
	mappers []Mapper
}

func (c chainMapper) MapPath(path string) (string, bool) {
	return c.mapFunc(path, Mapper.MapPath)
}

func (c chainMapper) MapPrefix(prefix string) (string, bool) {
	return c.mapFunc(prefix, Mapper.MapPrefix)
}

func (c chainMapper) UnmapFullPath(fullPath string) (string, bool, error) {
	path := fullPath
	var matches bool
	var err error
	for _, mapper := range c.mappers {
		path, matches, err = mapper.UnmapFullPath(path)
		if err != nil {
			return "", false, err
		}
		if !matches {
			return "", false, nil
		}
	}
	return path, true, nil
}

func (c chainMapper) mapFunc(
	pathOrPrefix string,
	f func(Mapper, string) (string, bool),
) (string, bool) {
	fullPathOrPrefix := pathOrPrefix
	var matches bool
	for i := len(c.mappers) - 1; i >= 0; i-- {
		mapper := c.mappers[i]
		fullPathOrPrefix, matches = f(mapper, fullPathOrPrefix)
		if !matches {
			return "", false
		}
	}
	return fullPathOrPrefix, true
}

func (chainMapper) isMapper() {}

type nopMapper struct{}

func (n nopMapper) MapPath(path string) (string, bool) {
	return path, true
}

func (n nopMapper) MapPrefix(prefix string) (string, bool) {
	return prefix, true
}

func (nopMapper) UnmapFullPath(fullPath string) (string, bool, error) {
	return fullPath, true, nil
}

func (nopMapper) isMapper() {}
