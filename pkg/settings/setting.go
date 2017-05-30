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

// Setting implementions wrap a val with atomic access.
type Setting interface {
	setToDefault()
	// Typ returns the short (1 char) string denoting the type of setting.
	Typ() string
	String() string

	Description() string
	setDescription(desc string)
	Hidden() bool
}

type common struct {
	description string
	hidden      bool
	onChange    func()
}

func (i *common) setDescription(s string) {
	i.description = s
}

func (i common) Description() string {
	return i.description
}
func (i common) Hidden() bool {
	return i.hidden
}

func (i common) changed() {
	if i.onChange != nil {
		i.onChange()
	}
}

// Hide prevents a setting from showing up in SHOW ALL CLUSTER SETTINGS. It can
// still be used with SET and SHOW if the exact setting name is known. Use Hide
// for in-development features and other settings that should not be
// user-visible.
func (i *common) Hide() {
	i.hidden = true
}

// setOnChange installs a callback to be called when a setting's value changes.
// `fn` should avoid doing long-running or blocking work as it is called on the
// goroutine which handles all settings updates.
func (i *common) setOnChange(fn func()) {
	i.onChange = fn
}

type numericSetting interface {
	Setting
	Validate(i int64) error
	set(i int64) error
}
