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

package git

type ref struct {
	ref string
}

func newRef(name string) *ref {
	return &ref{
		ref: name,
	}
}

func (r *ref) cloneBranch() string {
	return ""
}

func (r *ref) checkout() string {
	if r == nil {
		return ""
	}
	return r.ref
}

// Used for logging
func (r *ref) MarshalJSON() ([]byte, error) {
	return []byte(`"` + r.checkout() + `"`), nil
}

func (r *ref) String() string {
	return r.checkout()
}
