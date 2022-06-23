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

package protosource

import "fmt"

type fileImport struct {
	descriptor

	imp      string
	isPublic bool
	isWeak   bool
	isUnused bool
	path     []int32
}

func newFileImport(
	descriptor descriptor,
	imp string,
	path []int32,
) (*fileImport, error) {
	if imp == "" {
		return nil, fmt.Errorf("no dependency value in %q", descriptor.File().Path())
	}
	return &fileImport{
		descriptor: descriptor,
		imp:        imp,
		path:       path,
	}, nil
}

func (f *fileImport) Import() string {
	return f.imp
}

func (f *fileImport) IsPublic() bool {
	return f.isPublic
}

func (f *fileImport) IsWeak() bool {
	return f.isWeak
}

func (f *fileImport) IsUnused() bool {
	return f.isUnused
}

func (f *fileImport) Location() Location {
	return f.getLocation(f.path)
}

func (f *fileImport) setIsPublic() {
	f.isPublic = true
}

func (f *fileImport) setIsWeak() {
	f.isWeak = true
}

func (f *fileImport) setIsUnused() {
	f.isUnused = true
}
