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

/*
Package settings provides a central registry of runtime-chanable settings and
accompanying helper functions for retreiving their current values.

Settings values are stored in the system.settings table (which is gossiped). A
gossip-driven worker updates this package's cached value when the table changes.

To add a new setting, update `registry.go` to define it in the `registry` map,
then add an exported accessor (wrapping one of the `getBool`, `getString`, etc
helpers).

For example, to add an "enterprise" flag:
var registry = map[string]settingValue{
	....
+ "enterprise.enabled": {typ: BoolValue},

...

+// GetEnterpriseEnabled returns the enterprise.enabled setting.
+func GetEnterpriseEnabled() bool {
+   return getBool("enterprise.enabled")
+}
*/
package settings
