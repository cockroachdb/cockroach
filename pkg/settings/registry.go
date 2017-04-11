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

// registry contains all defined settings, their types and default values.
//
// Entries in registry should be accompanied by an exported, typesafe getter
// that then wraps one of the private `getBool`, `getString`, etc helpers.
//
// Registry should never be mutated after init (except in tests), as it is read
// concurrently by different callers.
var registry = map[string]value{
	"enterprise.enabled": {typ: BoolValue},
}

// value holds the (parsed, typed) value of a setting.
// raw settings are stored in system.settings as human-readable strings, but are
// cached interally after parsing in these appropriately typed fields (which is
// basically a poor-man's union, without boxing).
type value struct {
	typ ValueType
	// Exactly one of these will be set, determined by typ.
	s string
	b bool
	i int
	f float64
}

// TypeOf returns the type of a setting, if it is defined.
func TypeOf(key string) (ValueType, bool) {
	d, ok := registry[key]
	return d.typ, ok
}

// GetEnterpriseEnabled returns the "enterprise.enabled" setting.
// "enterprise.enabled" allows the use of the enterprise functionality (which
// requires an enterprise license).
// This is a temporary setting and will be replaced in the future.
func GetEnterpriseEnabled() bool {
	return getBool("enterprise.enabled")
}

// We export Testing* helpers for the settings-related tests in the SQL package.
const (
	testingStr = "testing.str"
	testingInt = "testing.int"
)

// TestingAddTestVars registers placeholder string and int settings, returning
// their names. They default to "<default>" and 1.
func TestingAddTestVars() (string, string, func()) {
	registry[testingStr] = value{typ: StringValue, s: "<default>"}
	registry[testingInt] = value{typ: IntValue, i: 1}
	return testingStr, testingInt, func() {
		delete(registry, testingStr)
		delete(registry, testingInt)
	}
}

// TestingGetString gets the current value for the testing string placeholder.
func TestingGetString() string {
	return getString(testingStr)
}

// TestingGetInt gets the current value for the testing int placeholder.
func TestingGetInt() int {
	return getInt(testingInt)
}
