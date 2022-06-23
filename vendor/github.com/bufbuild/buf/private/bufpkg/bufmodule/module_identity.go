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

type moduleIdentity struct {
	remote     string
	owner      string
	repository string
}

func newModuleIdentity(
	remote string,
	owner string,
	repository string,
) (*moduleIdentity, error) {
	moduleIdentity := &moduleIdentity{
		remote:     remote,
		owner:      owner,
		repository: repository,
	}
	if err := validateModuleIdentity(moduleIdentity); err != nil {
		return nil, err
	}
	return moduleIdentity, nil
}

func (m *moduleIdentity) Remote() string {
	return m.remote
}

func (m *moduleIdentity) Owner() string {
	return m.owner
}

func (m *moduleIdentity) Repository() string {
	return m.repository
}

func (m *moduleIdentity) IdentityString() string {
	return m.remote + "/" + m.owner + "/" + m.repository
}

func (*moduleIdentity) isModuleOwner()    {}
func (*moduleIdentity) isModuleIdentity() {}
