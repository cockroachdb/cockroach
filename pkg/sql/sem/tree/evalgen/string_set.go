// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import "sort"

type stringSet map[string]struct{}

func (ss stringSet) add(s string) { ss[s] = struct{}{} }

func (ss stringSet) removeAll(other stringSet) {
	for s := range other {
		delete(ss, s)
	}
}

func (ss stringSet) addAll(other stringSet) {
	for s := range other {
		ss.add(s)
	}
}

func (ss stringSet) ordered() []string {
	list := make([]string, 0, len(ss))
	for s := range ss {
		list = append(list, s)
	}
	sort.Strings(list)
	return list
}

func (ss stringSet) contains(name string) bool {
	_, exists := ss[name]
	return exists
}

func (ss stringSet) intersection(other stringSet) stringSet {
	intersection := stringSet{}
	for s := range ss {
		if other.contains(s) {
			intersection.add(s)
		}
	}
	return intersection
}
