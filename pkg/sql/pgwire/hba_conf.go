// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgwire

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// This file contains the logic for the configuration of HBA rules.
//
// In a nutshell, administrators customize the cluster setting
// `server.host_based_authentication.configuration`; each time they
// do so, all the nodes parse this configuration and re-initialize
// their authentication rules (a list of entries) from the setting.
//
// The HBA configuration is an ordered list of rules. Each time
// a client attempts to connect, the server scans the
// rules from the beginning of the list. The first rule that
// matches the connection decides how to authenticate.
//
// The syntax is inspired/derived from that of PostgreSQL's pg_hba.conf:
// https://www.postgresql.org/docs/12/auth-pg-hba-conf.html
//
// For now, CockroachDB only supports the following syntax:
//
//     host  all  <user[,user]...>  <IP-address/mask-length>  <auth-method>
//
// The matching rules are as follows:
// - A rule matches if the connecting username matches either of the
//   usernames listed in the rule, or if the pseudo-user 'all' is
//   present in the user column.
// - A rule matches if the connecting client's IP address is included
//   in the network address specified in the CIDR notation.
//

// serverHBAConfSetting is the name of the cluster setting that holds
// the HBA configuration.
const serverHBAConfSetting = "server.host_based_authentication.configuration"

// connAuthConf is the cluster setting that holds the HBA configuration.
var connAuthConf = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		serverHBAConfSetting,
		"host-based authentication configuration to use during connection authentication",
		"",
		checkHBASyntaxBeforeUpdatingSetting,
	)
	s.SetVisibility(settings.Public)
	return s
}()

// loadLocalAuthConfigUponRemoteSettingChange initializes the local node's cache
// of the auth configuration each time the cluster setting is updated.
func loadLocalAuthConfigUponRemoteSettingChange(
	ctx context.Context, server *Server, st *cluster.Settings,
) {
	val := connAuthConf.Get(&st.SV)
	server.auth.Lock()
	defer server.auth.Unlock()
	if val == "" {
		server.auth.conf = nil
		return
	}
	conf, err := hba.Parse(val)
	if err != nil {
		log.Warningf(ctx, "invalid %s: %v", serverHBAConfSetting, err)
		conf = nil
	}
	NormalizeHBAEntries(conf)
	server.auth.conf = conf
}

// NormalizeHBAEntries normalizes the entries in the HBA configuration.
func NormalizeHBAEntries(conf *hba.Conf) {
	// Usernames are normalized during session init. Normalize the HBA usernames
	// in the same way.
	for _, entry := range conf.Entries {
		for iu := range entry.User {
			user := &entry.User[iu]
			user.Value = tree.Name(user.Value).Normalize()
		}
	}
}

// checkHBASyntaxBeforeUpdatingSetting is run by the SQL gateway each time
// a client attempts to update the cluster setting.
func checkHBASyntaxBeforeUpdatingSetting(values *settings.Values, s string) error {
	if s == "" {
		// An empty configuration is always valid.
		return nil
	}
	conf, err := hba.Parse(s)
	if err != nil {
		return err
	}
	for _, entry := range conf.Entries {
		if entry.Type != "host" {
			return unimplemented.Newf("hba-type-"+entry.Type, "unsupported connection type: %s", entry.Type)
		}
		for _, db := range entry.Database {
			if !db.IsKeyword("all") {
				return errors.WithHint(
					unimplemented.New("hba-per-db", "per-database HBA rules are not supported"),
					"Use the special value 'all' (without quotes) to match all databases.")
			}
		}
		if addr, ok := entry.Address.(hba.String); ok && !addr.IsKeyword("all") {
			return errors.WithHint(
				unimplemented.New("hba-hostnames", "hostname-based HBA rules are not supported"),
				"List the numeric CIDR notation instead, for example: 127.0.0.1/8.")
		}
		if hbaAuthMethods[entry.Method] == nil {
			return errors.WithHintf(unimplemented.Newf("hba-method-"+entry.Method,
				"unknown auth method %q", entry.Method),
				"Supported methods: %s", listRegisteredMethods())
		}
		if check := hbaCheckHBAEntries[entry.Method]; check != nil {
			if err := check(entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// RegisterAuthMethod registers an AuthMethod for pgwire
// authentication and for use in HBA configuration.
//
// The checkEntry method, if provided, is called upon configuration
// the cluster setting in the SQL client which attempts to change the
// configuration. It can block the configuration if e.g. the syntax is
// invalid.
func RegisterAuthMethod(method string, fn AuthMethod, checkEntry CheckHBAEntry) {
	hbaAuthMethods[method] = fn
	if checkEntry != nil {
		hbaCheckHBAEntries[method] = checkEntry
	}
}

// listsupportedMethods returns a sorted, comma-delimited list
// of registered AuthMethods.
func listRegisteredMethods() string {
	methods := make([]string, 0, len(hbaAuthMethods))
	for method := range hbaAuthMethods {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	return strings.Join(methods, ", ")
}

var (
	hbaAuthMethods     = map[string]AuthMethod{}
	hbaCheckHBAEntries = map[string]CheckHBAEntry{}
)

// CheckHBAEntry defines a method for validating an hba Entry upon
// configuration of the cluster setting by a SQL client.
type CheckHBAEntry func(hba.Entry) error

// TestingGetHBAConf exposes the cached hba.Conf for use in testing.
func (s *Server) TestingGetHBAConf() *hba.Conf {
	s.auth.RLock()
	defer s.auth.RUnlock()
	return s.auth.conf
}

// HBADebugFn exposes the computed HBA configuration via the debug
// interface, for inspection by tests.
func (s *Server) HBADebugFn() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		s.auth.RLock()
		auth := s.auth.conf
		s.auth.RUnlock()

		_, _ = w.Write([]byte("# Cache of the HBA configuration on this node:\n"))
		if auth == nil {
			_, _ = w.Write([]byte("# (configuration is empty)"))
		} else {
			_, _ = w.Write([]byte(auth.String()))
		}
	}
}
