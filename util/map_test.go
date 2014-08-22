// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf <tobias.schottdorf@gmail.com>

package util

import (
	"reflect"
	"sort"
	"testing"
)

// TODO(jqmp): I'm not sure any testing is going on here.
func TestMapKeys(t *testing.T) {
	// For an uninitialized map, an empty slice should be returned.
	var nilMap map[uint]bool
	reflect.DeepEqual(MapKeys(nilMap).([]uint), []uint{})
	reflect.DeepEqual(MapKeys(map[string]struct{}{"a": {}}).([]string), []string{"a"})
	// Multiple elements need to be sorted to check equality.
	m := map[int]string{9: "Hola", 1: "Señor", 2: "Espencie"}
	sort.Ints(MapKeys(m).([]int))
	reflect.DeepEqual(m, []int{1, 2, 9})
}
