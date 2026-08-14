// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactEventInfo(t *testing.T) {
	decode := func(t *testing.T, info string) map[string]interface{} {
		m := map[string]interface{}{}
		require.NoError(t, json.Unmarshal([]byte(info), &m))
		return m
	}

	t.Run("setting-change event hides the value", func(t *testing.T) {
		info := `{"SettingName": "server.oidc_authentication.client_secret", ` +
			`"Value": "hunter2-secret"}`
		got := RedactEventInfo("set_cluster_setting", info)
		require.NotContains(t, got, "hunter2-secret")
		require.Equal(t, "<hidden>", decode(t, got)["Value"])
	})

	t.Run("tenant setting-change event hides the value, keeps tenant fields", func(t *testing.T) {
		info := `{"SettingName": "server.oidc_authentication.client_secret", ` +
			`"Value": "hunter2-secret", "TenantId": "2"}`
		got := RedactEventInfo("set_tenant_cluster_setting", info)
		require.NotContains(t, got, "hunter2-secret")
		m := decode(t, got)
		require.Equal(t, "<hidden>", m["Value"])
		require.Equal(t, "2", m["TenantId"])
	})

	t.Run("statement and placeholder values hidden for any event type", func(t *testing.T) {
		info := `{"Statement": "CREATE ROLE app WITH PASSWORD $1", ` +
			`"PlaceholderValues": ["'hunter2-secret'"], "User": "root", ` +
			`"Value": "not-a-setting-event"}`
		got := RedactEventInfo("create_role", info)
		require.NotContains(t, got, "hunter2-secret")
		m := decode(t, got)
		require.Equal(t, "<hidden>", m["Statement"])
		require.Equal(t, []interface{}{"<hidden>"}, m["PlaceholderValues"])
		require.Equal(t, "root", m["User"])
		// Typed Value fields of non-setting events are not touched.
		require.Equal(t, "not-a-setting-event", m["Value"])
	})

	t.Run("unparsable info is dropped", func(t *testing.T) {
		require.Equal(t, "", RedactEventInfo("node_join", "not json {"))
	})
}
