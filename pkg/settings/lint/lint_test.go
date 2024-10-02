// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lint_test

import (
	"context"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/base"
	// Ensure that the CCL packages are imported to see all cluster settings.
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func TestShowAllClusterSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "lint only test")
	skip.UnderDeadlock(t, "lint only test")
	skip.UnderStress(t, "lint only test")

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	defer s.Stopper().Stop(context.Background())

	rows, err := sqlDB.Query(`SELECT variable FROM [SHOW ALL CLUSTER SETTINGS]`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	shownNames := make(map[settings.SettingName]struct{})
	for rows.Next() {
		var settingName settings.SettingName
		if err := rows.Scan(&settingName); err != nil {
			t.Fatal(err)
		}
		if _, ok := shownNames[settingName]; ok {
			t.Errorf("setting %q reported twice in SHOW ALL CLUSTER SETTINGS", settingName)
		}
		shownNames[settingName] = struct{}{}
	}
	keysForShownNames := make(map[settings.InternalKey]struct{})
	for settingName := range shownNames {
		setting, ok, status := settings.LookupForLocalAccess(settingName, true /* forSystemTenant */)
		if !ok {
			t.Errorf("registry bug: setting with name %q not found", settingName)
			continue
		}
		if status == settings.NameRetired {
			t.Errorf("%s: retired name reported in SHOW ALL CLUSTER SETTINGS", settingName)
		}
		keysForShownNames[setting.InternalKey()] = struct{}{}
	}

	allKeysSlice := settings.Keys(true /* forSystemTenant */)
	allKeys := make(map[settings.InternalKey]struct{})
	for _, settingKey := range allKeysSlice {
		allKeys[settingKey] = struct{}{}
	}

	for k := range allKeys {
		if _, ok := keysForShownNames[k]; !ok {
			t.Errorf("setting %q not reported in SHOW ALL CLUSTER SETTINGS", k)
		}
	}
	for k := range keysForShownNames {
		if _, ok := allKeys[k]; !ok {
			t.Errorf("setting %q reported in SHOW ALL CLUSTER SETTINGS but not in settings.Keys()", k)
		}
	}
}

func TestLintClusterSettingNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderRace(t, "lint only test")
	skip.UnderDeadlock(t, "lint only test")
	skip.UnderStress(t, "lint only test")

	keys := settings.Keys(true /* forSystemTenant */)
	for _, settingKey := range keys {
		setting, ok := settings.LookupForLocalAccessByKey(settingKey, true /* forSystemTenant */)
		if !ok {
			t.Errorf("registry bug: setting with key %q not found", settingKey)
			continue
		}

		sType := setting.Typ()
		settingName := string(setting.Name())

		if strings.ToLower(settingName) != settingName {
			t.Errorf("%s: variable name must be all lowercase", settingName)
		}

		suffixSuggestions := map[string]struct {
			suggestion string
			exceptions []string
		}{
			"_ttl":     {suggestion: ".ttl"},
			"_enabled": {suggestion: ".enabled", exceptions: []string{".fraction_enabled"}},
			"_timeout": {suggestion: ".timeout", exceptions: []string{".read_timeout", ".write_timeout"}},
		}

		nameErr := func() error {
			segments := strings.Split(settingName, ".")
			for _, segment := range segments {
				if strings.TrimSpace(segment) != segment {
					return errors.Errorf("%s: part %q has heading or trailing whitespace", settingName, segment)
				}
				tokens, ok := parser.Tokens(segment)
				if !ok {
					return errors.Errorf("%s: part %q does not scan properly", settingName, segment)
				}
				if len(tokens) == 0 || len(tokens) > 1 {
					return errors.Errorf("%s: part %q has invalid structure", settingName, segment)
				}
				if tokens[0].TokenID != parser.IDENT {
					cat, ok := lexbase.KeywordsCategories[tokens[0].Str]
					if !ok {
						return errors.Errorf("%s: part %q has invalid structure", settingName, segment)
					}
					if cat == "R" {
						return errors.Errorf("%s: part %q is a reserved keyword", settingName, segment)
					}
				}
			}

			if !strings.HasPrefix(settingName, "sql.defaults.") {
				// The sql.default settings are special cased: they correspond
				// to same-name session variables, and session var names cannot
				// contain periods.
				for suffix, repl := range suffixSuggestions {
					if strings.HasSuffix(settingName, suffix) {
						hasException := false
						for _, e := range repl.exceptions {
							if strings.HasSuffix(settingName, e) {
								hasException = true
								break
							}
						}
						if !hasException {
							return errors.Errorf("%s: use %q instead of %q", settingName, repl.suggestion, suffix)
						}
					}
				}

				if sType == "b" && !strings.HasSuffix(settingName, ".enabled") && !strings.HasSuffix(settingName, ".disabled") {
					return errors.Errorf("%s: use .enabled for booleans (or, rarely, .disabled)", settingName)
				}
			}

			if strings.Contains(settingName, "unsafe") && !setting.IsUnsafe() {
				return errors.Errorf("%s: setting name contains \"unsafe\" but is not marked unsafe (hint: use option settings.WithUnsafe)", settingName)
			}
			if setting.IsUnsafe() && !strings.Contains(settingName, "unsafe") {
				return errors.Errorf("%s: setting marked as unsafe but its name does not contain \"unsafe\"", settingName)
			}

			return nil
		}()
		if nameErr != nil {
			t.Error(nameErr)
		}
	}
}

func TestLintClusterSettingDescriptions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderRace(t, "lint only test")
	skip.UnderDeadlock(t, "lint only test")
	skip.UnderStress(t, "lint only test")

	keys := settings.Keys(true /* forSystemTenant */)
	for _, settingKey := range keys {
		setting, ok := settings.LookupForLocalAccessByKey(settingKey, true /* forSystemTenant */)
		if !ok {
			t.Errorf("registry bug: setting with key %q not found", settingKey)
			continue
		}
		desc := setting.Description()
		settingName := string(setting.Name())
		sType := setting.Typ()

		if strings.TrimSpace(desc) != desc {
			t.Errorf("%s: description %q has heading or trailing whitespace", settingName, desc)
		}

		if len(desc) == 0 {
			t.Errorf("%s: description is empty", settingName)
		} else {
			if r, _ := utf8.DecodeRuneInString(desc); unicode.IsUpper(r) {
				t.Errorf("%s: description %q must not start with capital", settingName, desc)
			}
			if sType != "e" && (desc[len(desc)-1] == '.') && !strings.Contains(desc, ". ") {
				// TODO(knz): this check doesn't work with the way enum values are added to their descriptions.
				t.Errorf("%s: description %q must end with period only if it contains a secondary sentence", settingName, desc)
			}
			for _, c := range desc {
				if c == unicode.ReplacementChar {
					t.Errorf("%s: setting descriptions must be valid UTF-8: %q", settingName, desc)
				}
				if unicode.IsControl(c) {
					t.Errorf("%s: setting descriptions cannot contain control character %q, %q", settingName, c, desc)
				}
			}
		}
	}
}

func TestLintClusterVisibility(t *testing.T) {
	defer leaktest.AfterTest(t)()

	skip.UnderRace(t, "lint only test")
	skip.UnderDeadlock(t, "lint only test")
	skip.UnderStress(t, "lint only test")

	keys := settings.Keys(true /* forSystemTenant */)
	for _, settingKey := range keys {
		setting, ok := settings.LookupForLocalAccessByKey(settingKey, true /* forSystemTenant */)
		if !ok {
			t.Errorf("registry bug: setting with key %q not found", settingKey)
			continue
		}

		if setting.IsUnsafe() && setting.Visibility() == settings.Public {
			t.Errorf("%s: unsafe settings must not be public", setting.Name())
		}
	}
}
