// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// Log entries mentioning a sensitive cluster setting may carry the setting's
// value (a secret), e.g. the statement text of SET CLUSTER SETTING in SQL
// exec logs or the settings-change structured event. The same goes for
// role-change structured events that recorded a bound password in their
// placeholder values. Debug zips and merged logs are meant to be shared with
// Cockroach Labs, so such entries are scrubbed client-side even when no
// redaction was requested - including historical entries written by older
// server versions.

// sensitiveSettingNames returns the lowercased names and internal keys of all
// Sensitive-marked cluster settings known to this client's registry. The
// registry is immutable after init, so the result is computed once and
// shared by every caller.
var sensitiveSettingNames = sync.OnceValue(func() []string {
	var names []string
	for _, k := range settings.Keys(true /* forSystemTenant */) {
		s, ok := settings.LookupForLocalAccessByKey(k, true /* forSystemTenant */)
		if !ok || !s.IsSensitive() {
			continue
		}
		// Lowercased: callers match against a lowercased message, so that a
		// setting named in a case other than the registry's is still caught.
		names = append(names, strings.ToLower(string(s.Name())))
		if string(s.Name()) != string(k) {
			names = append(names, strings.ToLower(string(k)))
		}
	}
	return names
})

// scrubSensitiveSettingLogEntry scrubs the given log entry if its message
// mentions a sensitive cluster setting or is a role-change event carrying
// raw bound placeholder values (a bound password). Redactable entries are
// redacted in place, which removes the unsafe payloads (among them the
// secret) while preserving the rest of the message. Non-redactable entries
// offer no way to locate the secret within the message, so the entire
// message is replaced with the tombstone string.
func scrubSensitiveSettingLogEntry(
	e *logpb.Entry, names []string, tombstone string,
) (scrubbed, tombstoned bool) {
	mentions := roleEventWithRawBinds(e.Message)
	if !mentions {
		msg := strings.ToLower(e.Message)
		mentions = slices.ContainsFunc(names, func(n string) bool {
			return strings.Contains(msg, n)
		})
	}
	if !mentions {
		return false, false
	}
	if e.Redactable {
		e.Message = string(redact.RedactableString(e.Message).Redact())
		return true, false
	}
	e.Message = tombstone
	return true, true
}

// roleEventWithRawBinds reports whether the message looks like a role-change
// structured event that recorded raw placeholder values, which can carry a
// bound password (CREATE ROLE ... WITH PASSWORD $1). Events written since
// password binds began being substituted at write time carry the
// substitution marker among the placeholder values and are skipped, so this
// rule fires only for historical entries and goes quiet once they age out of
// retention.
//
// The marker is looked for in the placeholder values alone, not in the
// message at large: the statement text renders a password option as
// PASSWORD '*****' whether the password was a literal or a placeholder (see
// tree.KVOptions.formatAsRoleOptions), so a message-wide search matches every
// role event and would let the entries this rule exists for slip through.
func roleEventWithRawBinds(msg string) bool {
	if !strings.Contains(msg, `"EventType":"create_role"`) &&
		!strings.Contains(msg, `"EventType":"alter_role"`) {
		return false
	}
	binds, ok := placeholderValues(msg)
	return ok && !strings.Contains(binds, tree.PasswordSubstitution)
}

// placeholderValues returns the text of the message's PlaceholderValues JSON
// array and whether the field was present. The array is taken to end at the
// first ']', which a value containing that byte cuts short - JSON does not
// escape it. Erring short is deliberate: a span that stops before the
// substitution marker costs an unnecessary scrub, whereas one that ran past
// the array could pick up the marker from the statement text and skip a scrub
// that was needed. A field with no ']' at all (a truncated message) yields the
// empty span for the same reason.
func placeholderValues(msg string) (string, bool) {
	const field = `"PlaceholderValues":[`
	start := strings.Index(msg, field)
	if start < 0 {
		return "", false
	}
	rest := msg[start+len(field):]
	end := strings.IndexByte(rest, ']')
	if end < 0 {
		return "", true
	}
	return rest[:end], true
}

// sensitiveScrubFallbackTables lists the zip tables whose unredacted primary
// query scrubs sensitive cluster settings. Their fallbacks cannot tell a
// sensitive setting from an ordinary one, so they redact indiscriminately;
// the resulting loss of detail is surfaced in the zip's warning file.
var sensitiveScrubFallbackTables = map[string]struct{}{
	"system.eventlog":        {},
	"system.tenant_settings": {},
}

// sensitiveScrubStats aggregates, across an entire debug zip run, the
// client-side scrubbing performed because of sensitive cluster settings. The
// counts are surfaced in a warning file inside the artifact so that both the
// collector and the recipient can see that (and how much) content was
// removed.
type sensitiveScrubStats struct {
	syncutil.Mutex
	redactedEntries   int
	tombstonedEntries int
	// fallbackTables is a set rather than a list: the same table falls back
	// once per tenant, and the tenants are visited concurrently, so a list
	// would report duplicates in an arbitrary order.
	fallbackTables map[string]struct{}
}

func (s *sensitiveScrubStats) addLogEntries(redacted, tombstoned int) {
	if s == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	s.redactedEntries += redacted
	s.tombstonedEntries += tombstoned
}

func (s *sensitiveScrubStats) addFallbackTable(table string) {
	if s == nil {
		return
	}
	s.Lock()
	defer s.Unlock()
	if s.fallbackTables == nil {
		s.fallbackTables = make(map[string]struct{})
	}
	s.fallbackTables[table] = struct{}{}
}

// sensitiveSettingsWarningFileName is the name of the warning file placed at
// the top level of the debug zip when any sensitive-setting scrubbing left
// gaps in the collected data.
const sensitiveSettingsWarningFileName = "sensitive_settings_warning.txt"

// warningFileContents returns the contents for the in-artifact warning file,
// or "" if there is nothing to warn about.
func (s *sensitiveScrubStats) warningFileContents() string {
	if s == nil {
		return ""
	}
	s.Lock()
	defer s.Unlock()
	if s.redactedEntries == 0 && s.tombstonedEntries == 0 && len(s.fallbackTables) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(
		`Some collected data referenced sensitive cluster settings (settings that
hold secrets, such as authentication keys). To keep those secrets out of
this artifact, the following was scrubbed or degraded during collection:

`)
	if s.redactedEntries > 0 {
		fmt.Fprintf(&sb,
			"- %d log entries referencing a sensitive cluster setting or a bound\n"+
				"  password had their unsafe payloads redacted.\n", s.redactedEntries)
	}
	if s.tombstonedEntries > 0 {
		fmt.Fprintf(&sb,
			"- %d log entries referencing a sensitive cluster setting or a bound\n"+
				"  password were not redactable (redactable logging is disabled) and\n"+
				"  were replaced entirely with REDACTEDBYZIP. Enable redactable logs\n"+
				"  to preserve the non-sensitive parts of such entries in future\n"+
				"  zips.\n",
			s.tombstonedEntries)
	}
	if len(s.fallbackTables) > 0 {
		sb.WriteString(
			"- The sanitizing queries for the tables below did not run (the server\n" +
				"  likely predates them, or they timed out). Their fallbacks cannot\n" +
				"  identify which settings are sensitive, so every setting value and\n" +
				"  SQL constant they touch was redacted, not just the secrets:\n")
		tables := slices.Sorted(maps.Keys(s.fallbackTables))
		for _, table := range tables {
			fmt.Fprintf(&sb, "    %s\n", table)
		}
	}
	return sb.String()
}
