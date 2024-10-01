// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package settings provides a central registry of runtime editable settings and
accompanying helper functions for retrieving their current values.

# Overview

Settings values are stored in the system.settings table. A
rangefeed-driven worker updates the cached value when the table
changes (see package 'settingswatcher').

To add a new setting, call one of the `Register` methods in `registry.go` and
save the accessor created by the register function in the package where the
setting is to be used. For example, to add an "enterprise" flag, adding into
license_check.go:

	var enterpriseEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"enterprise.enabled",
		"some doc for the setting",
		false,
	)

Then use with `if enterpriseEnabled.Get(...) ...`

# Setting names

Settings have both a "key" and a "name".

The key is what is used to persist the value and synchronize it across
nodes. The key should remain immutable through the lifetime of the
setting. This is the main argument passed through the Register() calls.

The name is what end-users see in docs and can use via the SQL
statements SET/SHOW CLUSTER SETTING. It can change over time. It is
also subject to a linter; for example boolean settings should have a
name ending with '.enabled'.

When no name is specified, it defaults to the key. Another name
can be specified using `WithName()`, for example:

	var mySetting = settings.RegisterBoolSetting(
		"mykey", ...,
		settings.WithName("descriptive.name.enabled"),
	)

For convenience, users can also refer to a setting using its key
in the SET/SHOW CLUSTER SETTING statements. Because of this,
the keys and names are in the same namespace and cannot overlap.

# Careful choice of default values, value propagation delay

Settings should always be defined with "safe" default values -- until a node
receives values asynchronously, or even after that, if it cannot read them for some
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

# Changing names of settings

Sometimes for UX or documentation purposes it is useful to rename a setting.
However, to preserve backward-compatibility, its key cannot change.

There are two situations possible:

  - The setting currently has the same name as its key. To rename the setting,
    use the `WithName()` option.

  - The setting already has another name than its key. In this case,
    modify the name in the `WithName()` option and also add a
    `WithRetiredName()` option with the previous name. A SQL notice
    will redirect users to the new name.

# Retiring settings

Settings may become irrelevant over time, especially when introduced to provide
a workaround to a system limitation which is later corrected. There are two
possible scenarios:

  - The setting may still be referred to from automation (e.g. external
    scripts). In this case, use the option `settings.Retired` at the
    registration point.

  - The setting will not ever be reused anywhere and can be deleted.
    When deleting a setting's registration from the codebase, add its
    name to the list of `retiredSettings` in settings/registry.go --
    this ensures the name cannot be accidentally reused, and suppresses
    log spam about the existing value.

The list of deleted+retired settings can periodically (i.e. in major versions) be
"flushed" by adding a migration that deletes all stored values for those keys
at which point the key would be available for reuse in a later version. Is is
only safe to run such a migration after the cluster upgrade process ensures no
older nodes are still using the values for those old settings though, so such a
migration needs to be version gated.
*/
package settings
