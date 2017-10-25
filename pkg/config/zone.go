// Copyright 2015 The Cockroach Authors.
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

package config

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

// Several ranges outside of the SQL keyspace are given special names so they
// can be targeted by zone configs.
const (
	DefaultZoneName    = "default"
	MetaZoneName       = "meta"
	SystemZoneName     = "system"
	TimeseriesZoneName = "timeseries"
)

// NamedZones maps named zones to their pseudo-table ID that can be used to
// install an entry into the system.zones table.
var NamedZones = map[string]uint32{
	DefaultZoneName:    keys.RootNamespaceID,
	MetaZoneName:       keys.MetaRangesID,
	SystemZoneName:     keys.SystemRangesID,
	TimeseriesZoneName: keys.TimeseriesRangesID,
}

// NamedZonesByID is the inverse of NamedZones: it maps pseudo-table IDs to
// their zone names.
var NamedZonesByID = func() map[uint32]string {
	out := map[uint32]string{}
	for name, id := range NamedZones {
		out[id] = name
	}
	return out
}()

// ZoneSpecifierFromID creates a parser.ZoneSpecifier for the zone with the
// given ID.
func ZoneSpecifierFromID(
	id uint32, resolveID func(id uint32) (parentID uint32, name string, err error),
) (parser.ZoneSpecifier, error) {
	if name, ok := NamedZonesByID[id]; ok {
		return parser.ZoneSpecifier{NamedZone: parser.Name(name)}, nil
	}
	parentID, name, err := resolveID(id)
	if err != nil {
		return parser.ZoneSpecifier{}, err
	}
	if parentID == keys.RootNamespaceID {
		return parser.ZoneSpecifier{Database: parser.Name(name)}, nil
	}
	_, db, err := resolveID(parentID)
	if err != nil {
		return parser.ZoneSpecifier{}, err
	}
	tn := &parser.TableName{DatabaseName: parser.Name(db), TableName: parser.Name(name)}
	return parser.ZoneSpecifier{
		Table: parser.NormalizableTableName{TableNameReference: tn},
	}, nil
}

// ParseCLIZoneSpecifier converts a single string s identifying a zone, as would
// be used to name a zone on the command line, to a ZoneSpecifier. A valid CLI
// zone specifier is either 1) a database or table reference of the form
// DATABASE[.TABLE], or 2) a special named zone of the form [.NAME].
func ParseCLIZoneSpecifier(s string) (parser.ZoneSpecifier, error) {
	if len(s) > 0 && s[0] == '.' {
		name := s[1:]
		if name == "" {
			return parser.ZoneSpecifier{}, errors.New("missing zone name")
		}
		return parser.ZoneSpecifier{NamedZone: parser.Name(name)}, nil
	}
	// ParseTableName is not vulnerable to SQL injection, so passing s directly
	// is safe. See #8389 for details.
	tn, err := parser.ParseTableName(s)
	if err != nil {
		return parser.ZoneSpecifier{}, fmt.Errorf("malformed name: %q", s)
	}
	db := tn.Database()
	if db == "" {
		// No database was specified, so interpret the table name as the database.
		db = tn.Table()
		return parser.ZoneSpecifier{Database: parser.Name(db)}, nil
	}
	return parser.ZoneSpecifier{
		Table: parser.NormalizableTableName{TableNameReference: tn},
	}, nil
}

// CLIZoneSpecifier converts a parser.ZoneSpecifier to a CLI zone specifier as
// described in ParseCLIZoneSpecifier.
func CLIZoneSpecifier(zs parser.ZoneSpecifier) string {
	if zs.NamedZone != "" {
		return "." + string(zs.NamedZone)
	}
	if zs.Database != "" {
		return zs.Database.String()
	}
	return zs.Table.String()
}

// ResolveZoneSpecifier converts a zone specifier to the ID of most specific
// zone whose config applies.
func ResolveZoneSpecifier(
	zs parser.ZoneSpecifier, resolveName func(parentID uint32, name string) (id uint32, err error),
) (uint32, error) {
	if zs.NamedZone != "" {
		if zs.NamedZone == DefaultZoneName {
			return keys.RootNamespaceID, nil
		}
		if id, ok := NamedZones[string(zs.NamedZone)]; ok {
			return id, nil
		}
		return 0, fmt.Errorf("%q is not a built-in zone", string(zs.NamedZone))
	}

	if zs.Database != "" {
		return resolveName(keys.RootNamespaceID, string(zs.Database))
	}

	tn, err := zs.Table.NormalizeTableName()
	if err != nil {
		return 0, err
	}
	databaseID, err := resolveName(keys.RootNamespaceID, tn.Database())
	if err != nil {
		return 0, err
	}
	return resolveName(databaseID, tn.Table())
}

func (c Constraint) String() string {
	var str string
	switch c.Type {
	case Constraint_REQUIRED:
		str += "+"
	case Constraint_PROHIBITED:
		str += "-"
	}
	if len(c.Key) > 0 {
		str += c.Key + "="
	}
	str += c.Value
	return str
}

// FromString populates the constraint from the constraint shorthand notation.
func (c *Constraint) FromString(short string) error {
	switch short[0] {
	case '+':
		c.Type = Constraint_REQUIRED
		short = short[1:]
	case '-':
		c.Type = Constraint_PROHIBITED
		short = short[1:]
	default:
		c.Type = Constraint_POSITIVE
	}
	parts := strings.Split(short, "=")
	if len(parts) == 1 {
		c.Value = parts[0]
	} else if len(parts) == 2 {
		c.Key = parts[0]
		c.Value = parts[1]
	} else {
		return errors.Errorf("constraint needs to be in the form \"(key=)value\", not %q", short)
	}
	return nil
}

var _ yaml.Marshaler = Constraints{}
var _ yaml.Unmarshaler = &Constraints{}

// MarshalYAML implements yaml.Marshaler.
func (c Constraints) MarshalYAML() (interface{}, error) {
	short := make([]string, len(c.Constraints))
	for i, c := range c.Constraints {
		short[i] = c.String()
	}
	return short, nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (c *Constraints) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var shortConstraints []string
	if err := unmarshal(&shortConstraints); err != nil {
		return err
	}
	constraints := make([]Constraint, len(shortConstraints))
	for i, short := range shortConstraints {
		if err := constraints[i].FromString(short); err != nil {
			return err
		}
	}
	c.Constraints = constraints
	return nil
}

// minRangeMaxBytes is the minimum value for range max bytes.
const minRangeMaxBytes = 64 << 10 // 64 KB

// defaultZoneConfig is the default zone configuration used when no custom
// config has been specified.
var defaultZoneConfig = ZoneConfig{
	NumReplicas:   3,
	RangeMinBytes: 1 << 20,  // 1 MB
	RangeMaxBytes: 64 << 20, // 64 MB
	GC: GCPolicy{
		// Use 25 hours instead of the previous 24 to make users successful by
		// default. Users desiring to take incremental backups every 24h may
		// incorrectly assume that the previous default 24h was sufficient to do
		// that. But the equation for incremental backups is:
		// 	GC TTLSeconds >= (desired backup interval) + (time to perform incremental backup)
		// We think most new users' incremental backups will complete within an
		// hour, and larger clusters will have more experienced operators and will
		// understand how to change these settings if needed.
		TTLSeconds: 25 * 60 * 60,
	},
}

// DefaultZoneConfig is the default zone configuration used when no custom
// config has been specified.
func DefaultZoneConfig() ZoneConfig {
	testingLock.Lock()
	defer testingLock.Unlock()
	return defaultZoneConfig
}

// TestingSetDefaultZoneConfig is a testing-only function that changes the
// default zone config and returns a function that reverts the change.
func TestingSetDefaultZoneConfig(cfg ZoneConfig) func() {
	testingLock.Lock()
	oldConfig := defaultZoneConfig
	defaultZoneConfig = cfg
	testingLock.Unlock()

	return func() {
		testingLock.Lock()
		defaultZoneConfig = oldConfig
		testingLock.Unlock()
	}
}

// Validate verifies some ZoneConfig fields.
// This should be used to validate user input when setting a new zone config.
func (z ZoneConfig) Validate() error {
	switch z.NumReplicas {
	case 0:
		return fmt.Errorf("attributes for at least one replica must be specified in zone config")
	case 2:
		return fmt.Errorf("at least 3 replicas are required for multi-replica configurations")
	}
	if z.RangeMaxBytes < minRangeMaxBytes {
		return fmt.Errorf("RangeMaxBytes %d less than minimum allowed %d",
			z.RangeMaxBytes, minRangeMaxBytes)
	}
	if z.RangeMinBytes >= z.RangeMaxBytes {
		return fmt.Errorf("RangeMinBytes %d is greater than or equal to RangeMaxBytes %d",
			z.RangeMinBytes, z.RangeMaxBytes)
	}
	return nil
}

// GetSubzoneForKeySuffix returns the ZoneConfig for the subzone that contains
// keySuffix, if it exists.
func (z ZoneConfig) GetSubzoneForKeySuffix(keySuffix []byte) (ZoneConfig, bool) {
	// TODO(benesch): Use binary search instead.
	for _, s := range z.SubzoneSpans {
		// The span's Key is stored with the prefix removed, so we can compare
		// directly to keySuffix. An unset EndKey implies Key.PrefixEnd().
		if (s.Key.Compare(keySuffix) <= 0) &&
			((s.EndKey == nil && bytes.HasPrefix(keySuffix, s.Key)) || s.EndKey.Compare(keySuffix) > 0) {
			return z.Subzones[s.SubzoneIndex].Config, true
		}
	}
	return ZoneConfig{}, false
}
