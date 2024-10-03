// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"net"
	"net/http"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/settings/rulebasedscanner"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
// If the cluster setting is not initialized, or when it is assigned
// the empty string, a special "default" configuration is used
// instead:
//
//     host all root all cert           # require certs for root
//     host all all  all cert-password  # require certs or password for everyone else
//
// (In fact, the first line `host all root all cert` is always
// inserted at the start of any custom configuration, as a safeguard.)
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

// chainOptions and requireClusterVersion will be used in an upcoming PR.
// Referencing them temporarily to pass the "unused linter" warning.
// See comment in https://github.com/cockroachdb/cockroach/pull/85777.
var _ = chainOptions
var _ = requireClusterVersion

// serverHBAConfSetting is the name of the cluster setting that holds
// the HBA configuration.
const serverHBAConfSetting = "server.host_based_authentication.configuration"

// connAuthConf is the cluster setting that holds the HBA
// configuration.
var connAuthConf = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	serverHBAConfSetting,
	"host-based authentication configuration to use during connection authentication",
	"",
	settings.WithValidateString(checkHBASyntaxBeforeUpdatingSetting),
	settings.WithPublic,
	settings.WithReportable(false),
	settings.Sensitive,
)

// loadLocalHBAConfigUponRemoteSettingChange initializes the local
// node's cache of the HBA configuration each time the cluster setting
// is updated.
func loadLocalHBAConfigUponRemoteSettingChange(
	ctx context.Context, server *Server, st *cluster.Settings,
) {
	val := connAuthConf.Get(&st.SV)

	// An empty HBA configuration is special and means "use the
	// default".
	hbaConfig := DefaultHBAConfig
	if val != "" {
		var err error
		hbaConfig, err = ParseAndNormalize(val)
		if err != nil {
			// The default is also used if the node is unable to load the
			// config from the cluster setting.
			log.Ops.Warningf(ctx, "invalid %s: %v", serverHBAConfSetting, err)
			hbaConfig = DefaultHBAConfig
		}
	}

	server.auth.Lock()
	defer server.auth.Unlock()
	server.auth.conf = hbaConfig
}

// checkHBASyntaxBeforeUpdatingSetting is run by the SQL gateway each
// time a SQL client attempts to update the cluster setting.
// It is also used when initially loading the default value.
func checkHBASyntaxBeforeUpdatingSetting(values *settings.Values, s string) error {
	if s == "" {
		// An empty configuration is always valid.
		return nil
	}
	// Note: we parse, but do not normalize, here, so as to
	// check for unsupported features in the input.
	conf, err := hba.Parse(s)
	if err != nil {
		return err
	}

	if len(conf.Entries) == 0 {
		// If the string was not empty, the user likely intended to have
		// *something* in the configuration, so us not finding anything
		// likely indicates either a parsing bug, or that the user
		// mistakenly put only comments in their config.
		return errors.WithHint(errors.New("no entries"),
			"To use the default configuration, assign the empty string ('').")
	}

	for _, entry := range conf.Entries {
		switch entry.ConnType {
		case hba.ConnHostAny:
		case hba.ConnLocal:
		case hba.ConnHostSSL, hba.ConnHostNoSSL:

		default:
			return unimplemented.Newf("hba-type-"+entry.ConnType.String(),
				"unsupported connection type: %s", entry.ConnType)
		}
		for _, db := range entry.Database {
			if !db.IsKeyword("all") {
				return errors.WithHint(
					unimplemented.New("hba-per-db", "per-database HBA rules are not supported"),
					"Use the special value 'all' (without quotes) to match all databases.")
			}
		}

		if entry.ConnType != hba.ConnLocal {
			// Verify the user is not requesting hostname-based validation,
			// which is not yet implemented.
			addrOk := true
			switch t := entry.Address.(type) {
			case *net.IPNet:
			case hba.AnyAddr:
			case rulebasedscanner.String:
				addrOk = t.IsKeyword("all")
			default:
				addrOk = false
			}
			if !addrOk {
				return errors.WithHint(
					unimplemented.New("hba-hostnames", "hostname-based HBA rules are not supported"),
					"List the numeric CIDR notation instead, for example: 127.0.0.1/8.\n"+
						"Alternatively, use 'all' (without quotes) for any IPv4/IPv6 address.")
			}
		}

		// Verify that the auth method is supported.
		method, ok := hbaAuthMethods[entry.Method.Value]
		if !ok || method.fn == nil {
			return errors.WithHintf(unimplemented.Newf("hba-method-"+entry.Method.Value,
				"unknown auth method %q", entry.Method.Value),
				"Supported methods: %s", listRegisteredMethods())
		}
		// Run the per-method validation.
		if check := hbaCheckHBAEntries[entry.Method.Value]; check != nil {
			if err := check(values, entry); err != nil {
				return err
			}
		}
	}
	return nil
}

// ParseAndNormalize calls hba.ParseAndNormalize and also ensures the
// configuration starts with a rule that authenticates the root user
// with client certificates.
//
// This prevents users from shooting themselves in the foot and making
// root not able to login, thus disallowing anyone from fixing the HBA
// configuration.
func ParseAndNormalize(val string) (*hba.Conf, error) {
	conf, err := hba.ParseAndNormalize(val)
	if err != nil {
		return conf, err
	}

	if len(conf.Entries) == 0 || !(conf.Entries[0].Equivalent(rootEntry) || conf.Entries[0].Equivalent(rootLocalEntry)) {
		entries := make([]hba.Entry, 1, len(conf.Entries)+1)
		entries[0] = rootEntry
		entries = append(entries, conf.Entries...)
		conf.Entries = entries
	}

	// If not rule for ConnInternalLoopback connections has been specified, add
	// one similar to the default.
	found := false
	for _, e := range conf.Entries {
		if e.ConnType == hba.ConnInternalLoopback {
			found = true
			break
		}
	}
	if !found {
		entries := make([]hba.Entry, 1, len(conf.Entries)+1)
		entries[0] = hba.Entry{
			ConnType: hba.ConnInternalLoopback,
			User:     []rulebasedscanner.String{{Value: "all", Quoted: false}},
			Address:  hba.AnyAddr{},
			Method:   rulebasedscanner.String{Value: "trust"},
			Input:    "loopback all all all trust       # built-in CockroachDB default",
		}
		entries = append(entries, conf.Entries...)
		conf.Entries = entries
	}

	// Lookup and cache the auth methods.
	for i := range conf.Entries {
		method := conf.Entries[i].Method.Value
		info, ok := hbaAuthMethods[method]
		if !ok {
			// TODO(knz): Determine if an error should be reported
			// upon unknown auth methods.
			// See: https://github.com/cockroachdb/cockroach/issues/43716
			return nil, errors.Errorf("unknown auth method %s", method)
		}
		conf.Entries[i].MethodFn = info
	}

	return conf, nil
}

type hbaEntryType string

const (
	jwtHBAEntry         hbaEntryType = "jwt_token"
	certHBAEntry        hbaEntryType = "cert"
	passwordHBAEntry    hbaEntryType = "password"
	ldapHBAEntry        hbaEntryType = "ldap"
	gssHBAEntry         hbaEntryType = "gss"
	scramSHA256HBAEntry hbaEntryType = "scram-sha-256"
)

func (h hbaEntryType) string() string {
	return string(h)
}

var insecureEntry = hba.Entry{
	ConnType: hba.ConnHostAny,
	User:     []rulebasedscanner.String{{Value: "all", Quoted: false}},
	Address:  hba.AnyAddr{},
	Method:   rulebasedscanner.String{Value: "insecure"},
}

var sessionRevivalEntry = hba.Entry{
	ConnType: hba.ConnHostAny,
	User:     []rulebasedscanner.String{{Value: "all", Quoted: false}},
	Address:  hba.AnyAddr{},
	Method:   rulebasedscanner.String{Value: "session_revival_token"},
}

var jwtAuthEntry = hba.Entry{
	ConnType: hba.ConnHostAny,
	User:     []rulebasedscanner.String{{Value: "all", Quoted: false}},
	Address:  hba.AnyAddr{},
	Method:   rulebasedscanner.String{Value: jwtHBAEntry.string()},
}

var rootEntry = hba.Entry{
	ConnType: hba.ConnHostAny,
	User:     []rulebasedscanner.String{{Value: username.RootUser, Quoted: false}},
	Address:  hba.AnyAddr{},
	Method:   rulebasedscanner.String{Value: "cert-password"},
	Input:    "host  all root all cert-password # CockroachDB mandatory rule",
}

var _, localhostCidrBytes, _ = net.ParseCIDR("127.0.0.1/32")
var rootLocalEntry = hba.Entry{
	ConnType: hba.ConnHostAny,
	User:     []rulebasedscanner.String{{Value: username.RootUser, Quoted: false}},
	Address:  localhostCidrBytes,
	Method:   rulebasedscanner.String{Value: "cert-password"},
	Input:    "host all root 127.0.0.1/32 cert-password # Alternative to the CockroachDB mandatory rule",
}

// DefaultHBAConfig is used when the stored HBA configuration string
// is empty or invalid.
var DefaultHBAConfig = func() *hba.Conf {
	loadDefaultMethods()
	conf, err := ParseAndNormalize(`
loopback all all all trust       # built-in CockroachDB default
host     all all all cert-password # built-in CockroachDB default
local    all all     password      # built-in CockroachDB default
`)
	if err != nil {
		panic(err)
	}
	return conf
}()

// GetAuthenticationConfiguration retrieves the current applicable
// authentication configuration.
//
// This is guaranteed to return a valid configuration. Additionally,
// the various setters for the configuration also pass through
// ParseAndNormalize(), whereby an entry is always present at the start,
// to enable root to log in with a valid client cert.
//
// The data returned by this method is also observable via the debug
// endpoint /debug/hba_conf.
func (s *Server) GetAuthenticationConfiguration() (*hba.Conf, *identmap.Conf) {
	s.auth.RLock()
	auth := s.auth.conf
	idMap := s.auth.identityMap
	s.auth.RUnlock()

	if auth == nil {
		// This can happen when using the value for the first time before
		// the cluster setting has ever been set.
		auth = DefaultHBAConfig
	}
	if idMap == nil {
		idMap = identmap.Empty()
	}
	return auth, idMap
}

// RegisterAuthMethod registers an AuthMethod for pgwire
// authentication and for use in HBA configuration.
//
// The minReqVersion is checked upon configuration to verify whether
// the current active cluster version is at least the version
// specified.
//
// The validConnTypes is checked during rule matching when accepting
// connections: if the connection type is not accepted by the auth
// method, authentication is refused upfront. For example, the "cert"
// method requires SSL; if a rule specifies "host .... cert" and the
// client connects without SSL, the authentication is refused.
// (To express "cert on SSL, password on non-SSL", the HBA conf
// can list 'hostssl ... cert; hostnossl .... password' instead.)
//
// The checkEntry method, if provided, is called upon configuration
// the cluster setting in the SQL client which attempts to change the
// configuration. It can block the configuration if e.g. the syntax is
// invalid.
func RegisterAuthMethod(
	method string, fn AuthMethod, validConnTypes hba.ConnType, checkEntry CheckHBAEntry,
) {
	hbaAuthMethods[method] = methodInfo{validConnTypes, fn}
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
	hbaAuthMethods     = map[string]methodInfo{}
	hbaCheckHBAEntries = map[string]CheckHBAEntry{}
)

type methodInfo struct {
	validConnTypes hba.ConnType
	fn             AuthMethod
}

// CheckHBAEntry defines a method for validating an hba Entry upon
// configuration of the cluster setting by a SQL client.
type CheckHBAEntry func(*settings.Values, hba.Entry) error

// NoOptionsAllowed is a CheckHBAEntry that returns an error if any
// options are present in the entry.
var NoOptionsAllowed CheckHBAEntry = func(sv *settings.Values, e hba.Entry) error {
	if len(e.Options) != 0 {
		return errors.Newf("the HBA method %q does not accept options", e.Method)
	}
	return nil
}

// chainOptions is an option that combines its argument options.
func chainOptions(opts ...CheckHBAEntry) CheckHBAEntry {
	return func(values *settings.Values, e hba.Entry) error {
		for _, o := range opts {
			if err := o(values, e); err != nil {
				return err
			}
		}
		return nil
	}
}

// requireClusterVersion is an HBA option check function that verifies
// that the given cluster version key has been enabled before allowing
// a client to use this authentication method.
func requireClusterVersion(versionkey clusterversion.Key) CheckHBAEntry {
	return func(values *settings.Values, e hba.Entry) error {
		// Retrieve the cluster version handle. We'll need to check the current cluster version.
		var vh clusterversion.Handle
		if values != nil {
			vh = values.Opaque().(clusterversion.Handle)
		}
		if vh != nil &&
			!vh.IsActive(context.TODO(), versionkey) {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				`HBA authentication method %q requires all nodes to be upgraded to %s`,
				e.Method,
				versionkey,
			)
		}
		return nil
	}
}

// HBADebugFn exposes the computed HBA configuration via the debug
// interface, for inspection by tests.
func (s *Server) HBADebugFn() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		auth, usernames := s.GetAuthenticationConfiguration()

		_, _ = w.Write([]byte("# Active authentication configuration on this node:\n"))
		_, _ = w.Write([]byte(auth.String()))
		if !usernames.Empty() {
			_, _ = w.Write([]byte("# Active identity mapping on this node:\n"))
			_, _ = w.Write([]byte(usernames.String()))
		}
	}
}
