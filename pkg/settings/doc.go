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
Package settings provides a central registry of runtime editable settings and
accompanying helper functions for retreiving their current values.

Settings values are stored in the system.settings table (which is gossiped). A
gossip-driven worker updates this package's cached value when the table changes
(see the `RefreshSettings`` worker in the `sql` package).

The package's cache is global -- while all the usual drawbacks of mutable global
state obviously apply, it is needed to make the package's functionality
available to a wide variety of callsites, that may or may not have a *Server or
similar available to plumb though.

To add a new setting, call one of the `Register` methods in `registry.go` and
save the accessor created by the register function in the package where the
setting is to be used. For example, to add an "enterprise" flag, adding into
license_check.go:

var enterpriseEnabled = settings.RegisterBoolSetting(
   "enterprise.enabled", "some doc for the setting", false,
)

Then use with `if enterpriseEnabled.Get() ...`

Settings should always be defined with "safe" default values -- until a node
receives values via gossip, or even after that, if it cannot read them for some
reason, it will use the default values, so define defaults that "fail safe".

In cases where the "safe" default doesn't actually match the desired default,
like respecting an opt-*out* setting, we can default to `false` (opted out) and
then use a migration to write an explicit `true`: in practice you'd still expect
to read `true` unless a preference is expressed, but in the rare cases where you
read a default, you don't risk ignoring an expressed opt-out.
*/
package settings
