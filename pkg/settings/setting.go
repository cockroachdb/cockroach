// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

package settings

// Setting implementations wrap a val with atomic access.
type Setting interface {
	setToDefault()
	// Typ returns the short (1 char) string denoting the type of setting.
	Typ() string
	String() string

	// AddChangeCallback registers a closure that is run after the setting
	// changes value.
	AddChangeCallback(cb func())
}

type numericSetting interface {
	Setting
	Validate(i int64) error
	set(i int64) error
}

type settingBase struct {
	changeCallbacks []func()
}

func (s *settingBase) AddChangeCallback(cb func()) {
	s.changeCallbacks = append(s.changeCallbacks, cb)
}

func (s *settingBase) runChangeCallbacks() {
	for _, cb := range s.changeCallbacks {
		cb()
	}
}
