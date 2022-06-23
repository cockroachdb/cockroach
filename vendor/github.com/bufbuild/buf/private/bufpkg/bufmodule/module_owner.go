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

package bufmodule

type moduleOwner struct {
	remote string
	owner  string
}

func newModuleOwner(
	remote string,
	owner string,
) (*moduleOwner, error) {
	moduleOwner := &moduleOwner{
		remote: remote,
		owner:  owner,
	}
	if err := validateModuleOwner(moduleOwner); err != nil {
		return nil, err
	}
	return moduleOwner, nil
}

func (m *moduleOwner) Remote() string {
	return m.remote
}

func (m *moduleOwner) Owner() string {
	return m.owner
}

func (*moduleOwner) isModuleOwner() {}
