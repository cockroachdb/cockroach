// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package settings provides a central registry of runtime editable settings and
accompanying helper functions for retrieving their current values.

Settings values are stored in the system.settings table (which is gossiped). A
gossip-driven worker updates this package's cached value when the table changes
(see the `RefreshSettings` worker in the `sql` package).

The package's cache is global -- while all the usual drawbacks of mutable global
state obviously apply, it is needed to make the package's functionality
available to a wide variety of callsites, that may or may not have a *Server or
similar available to access settings.

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

Ideally, when passing configuration into some structure or subsystem, e.g. a
rate limit into a client or something, passing a `*FooSetting` rather than a
`Foo` and waiting to call `.Get()` until the value is actually used ensures
observing the latest value.

Settings may become irrelevant over time, especially when introduced to provide
a workaround to a system limitation which is later corrected. When deleting a
setting's registration from the codebase, add its name to the list of
`retiredSettings` in settings/registry.go -- this ensures the name cannot be
accidentally reused, and suppresses log spam about the existing value.

That list of retired settings can periodically (i.e. in major versions) be
"flushed" by adding a migration that deletes all stored values for those keys
at which point the key would be available for reuse in a later version. Is is
only safe to run such a migration after the cluster upgrade process ensures no
older nodes are still using the values for those old settings though, so such a
migration needs to be version gated.

Existing/off-the-shelf systems generally will not be defined in terms of our
settings, but if they can either be swapped at runtime or expose some `setFoo`
method, that can be used in conjunction with a change callback registered via
OnChange.
*/
package settings
