// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/stretchr/testify/assert"
)

func TestAliasedSettings(t *testing.T) {
	defer settings.TestingSaveRegistry()()

	_ = settings.RegisterBoolSetting(settings.SystemOnly, "s1key", "desc", false)
	_ = settings.RegisterBoolSetting(settings.SystemOnly, "s2key", "desc", false, settings.WithName("s2name"))
	_ = settings.RegisterBoolSetting(settings.SystemOnly, "s3key", "desc", false,
		settings.WithName("s3name-new"), settings.WithRetiredName("s3name-old"))

	k, found, _ := settings.NameToKey("unknown")
	assert.False(t, found)
	assert.Equal(t, settings.InternalKey(""), k)

	k, found, status := settings.NameToKey("s1key")
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s1key"), k)
	assert.Equal(t, settings.NameActive, status)

	k, found, status = settings.NameToKey("s2key")
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s2key"), k)
	assert.Equal(t, settings.NameRetired, status)

	k, found, status = settings.NameToKey("s2name")
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s2key"), k)
	assert.Equal(t, settings.NameActive, status)

	k, found, status = settings.NameToKey("s3key")
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s3key"), k)
	assert.Equal(t, settings.NameRetired, status)

	k, found, status = settings.NameToKey("s3name-new")
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s3key"), k)
	assert.Equal(t, settings.NameActive, status)

	k, found, status = settings.NameToKey("s3name-old")
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s3key"), k)
	assert.Equal(t, settings.NameRetired, status)

	s, found, status := settings.LookupForLocalAccess("s1key", true)
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s1key"), s.InternalKey())
	assert.Equal(t, settings.NameActive, status)

	s, found, status = settings.LookupForLocalAccess("s2key", true)
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s2key"), s.InternalKey())
	assert.Equal(t, settings.SettingName("s2name"), s.Name())
	assert.Equal(t, settings.NameRetired, status)

	s, found, status = settings.LookupForLocalAccess("s2name", true)
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s2key"), s.InternalKey())
	assert.Equal(t, settings.NameActive, status)

	s, found, status = settings.LookupForLocalAccess("s3key", true)
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s3key"), s.InternalKey())
	assert.Equal(t, settings.SettingName("s3name-new"), s.Name())
	assert.Equal(t, settings.NameRetired, status)

	s, found, status = settings.LookupForLocalAccess("s3name-old", true)
	assert.True(t, found)
	assert.Equal(t, settings.InternalKey("s3key"), s.InternalKey())
	assert.Equal(t, settings.SettingName("s3name-new"), s.Name())
	assert.Equal(t, settings.NameRetired, status)
}
