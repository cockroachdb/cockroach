// Copyright 2018 The Cockroach Authors.
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

package ui

// ErrorsByError compares error objects by their Error() value.
type ErrorsByError []error

func (l ErrorsByError) Len() int {
	return len(l)
}
func (l ErrorsByError) Less(i, j int) bool {
	return l[i].Error() < l[j].Error()
}
func (l ErrorsByError) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
