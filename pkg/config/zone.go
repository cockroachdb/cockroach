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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

// Several ranges outside of the SQL keyspace are given special names so they
// can be targeted by zone configs.
const (
	DefaultZoneName    = "default"
	LivenessZoneName   = "liveness"
	MetaZoneName       = "meta"
	SystemZoneName     = "system"
	TimeseriesZoneName = "timeseries"
)

// NamedZones maps named zones to their pseudo-table ID that can be used to
// install an entry into the system.zones table.
var NamedZones = map[string]uint32{
	DefaultZoneName:    keys.RootNamespaceID,
	LivenessZoneName:   keys.LivenessRangesID,
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

// ZoneSpecifierFromID creates a tree.ZoneSpecifier for the zone with the
// given ID.
func ZoneSpecifierFromID(
	id uint32, resolveID func(id uint32) (parentID uint32, name string, err error),
) (tree.ZoneSpecifier, error) {
	if name, ok := NamedZonesByID[id]; ok {
		return tree.ZoneSpecifier{NamedZone: tree.UnrestrictedName(name)}, nil
	}
	parentID, name, err := resolveID(id)
	if err != nil {
		return tree.ZoneSpecifier{}, err
	}
	if parentID == keys.RootNamespaceID {
		return tree.ZoneSpecifier{Database: tree.Name(name)}, nil
	}
	_, db, err := resolveID(parentID)
	if err != nil {
		return tree.ZoneSpecifier{}, err
	}
	tn := &tree.TableName{DatabaseName: tree.Name(db), TableName: tree.Name(name)}
	return tree.ZoneSpecifier{
		TableOrIndex: tree.TableNameWithIndex{
			Table: tree.NormalizableTableName{TableNameReference: tn},
		},
	}, nil
}

// ParseCLIZoneSpecifier converts a single string s identifying a zone, as would
// be used to name a zone on the command line, to a ZoneSpecifier. A valid CLI
// zone specifier is either 1) a database or table reference of the form
// DATABASE[.TABLE[.PARTITION|@INDEX]], or 2) a special named zone of the form
// .NAME.
func ParseCLIZoneSpecifier(s string) (tree.ZoneSpecifier, error) {
	if len(s) > 0 && s[0] == '.' {
		name := s[1:]
		if name == "" {
			return tree.ZoneSpecifier{}, errors.New("missing zone name")
		}
		return tree.ZoneSpecifier{NamedZone: tree.UnrestrictedName(name)}, nil
	}
	// ParseTableNameWithIndex is not vulnerable to SQL injection, so passing s
	// directly is safe. See #8389 for details.
	parsed, err := parser.ParseTableNameWithIndex(s)
	if err != nil {
		return tree.ZoneSpecifier{}, fmt.Errorf("malformed name: %q", s)
	}
	parsed.SearchTable = false
	var partition tree.Name
	if un := parsed.Table.TableNameReference.(*tree.UnresolvedName); len(*un) == 1 {
		// Unlike in SQL, where a name with one part indicates a table in the
		// current database, if a CLI specifier has just one name part, it indicates
		// a database.
		return tree.ZoneSpecifier{Database: *(*un)[0].(*tree.Name)}, nil
	} else if len(*un) == 3 {
		// If a CLI specifier has three name parts, the last name is a partition.
		// Pop it off so TableNameReference.Normalize sees only the table name
		// below.
		partition = *(*un)[2].(*tree.Name)
		tPref := (*un)[:2]
		parsed.Table.TableNameReference = &tPref
	}
	// We've handled the special cases for named zones, databases and partitions;
	// have TableNameReference.Normalize tell us whether what remains is a valid
	// table or index name.
	if _, err = parsed.Table.Normalize(); err != nil {
		return tree.ZoneSpecifier{}, err
	}
	if parsed.Index != "" && partition != "" {
		return tree.ZoneSpecifier{}, fmt.Errorf(
			"index and partition cannot be specified simultaneously: %q", s)
	}
	return tree.ZoneSpecifier{
		TableOrIndex: parsed,
		Partition:    partition,
	}, nil
}

// CLIZoneSpecifier converts a tree.ZoneSpecifier to a CLI zone specifier as
// described in ParseCLIZoneSpecifier.
func CLIZoneSpecifier(zs *tree.ZoneSpecifier) string {
	if zs.NamedZone != "" {
		return "." + string(zs.NamedZone)
	}
	if zs.Database != "" {
		return zs.Database.String()
	}
	ti := zs.TableOrIndex
	if zs.Partition != "" {
		tn := ti.Table.TableName()
		ti.Table = tree.NormalizableTableName{
			TableNameReference: &tree.UnresolvedName{&tn.DatabaseName, &tn.TableName, &zs.Partition},
		}
		// The index is redundant when the partition is specified, so omit it.
		ti.Index = ""
	}
	return tree.AsStringWithFlags(&ti, tree.FmtAlwaysQualifyTableNames)
}

// ResolveZoneSpecifier converts a zone specifier to the ID of most specific
// zone whose config applies.
func ResolveZoneSpecifier(
	zs *tree.ZoneSpecifier,
	sessionDB string,
	resolveName func(parentID uint32, name string) (id uint32, err error),
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

	tn, err := zs.TableOrIndex.Table.NormalizeWithDatabaseName(sessionDB)
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

// Validate returns an error if the ZoneConfig specifies a known-dangerous
// configuration.
func (z *ZoneConfig) Validate() error {
	for _, s := range z.Subzones {
		if err := s.Config.Validate(); err != nil {
			return err
		}
	}
	switch z.NumReplicas {
	case 0:
		if len(z.Subzones) > 0 {
			// NumReplicas == 0 is allowed when this ZoneConfig is a subzone
			// placeholder. See IsSubzonePlaceholder.
			return nil
		}
		return fmt.Errorf("at least one replica is required")
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

// DeleteTableConfig removes any configuration that applies to the table
// targeted by this ZoneConfig, leaving only its subzone configs, if any. After
// calling DeleteTableConfig, IsZubzonePlaceholder will return true.
//
// Only table zones can have subzones, so it does not make sense to call this
// method on non-table ZoneConfigs.
func (z *ZoneConfig) DeleteTableConfig() {
	*z = ZoneConfig{
		Subzones:     z.Subzones,
		SubzoneSpans: z.SubzoneSpans,
	}
}

// IsSubzonePlaceholder returns whether the ZoneConfig exists only to store
// subzones. The configuration fields (e.g., RangeMinBytes) in a subzone
// placeholder should be ignored; instead, the configuration from the parent
// ZoneConfig applies.
func (z *ZoneConfig) IsSubzonePlaceholder() bool {
	// A ZoneConfig with zero replicas is otherwise invalid, so we repurpose it to
	// indicate that a ZoneConfig is a placeholder for subzones rather than
	// introducing a dedicated IsPlaceholder flag.
	return z.NumReplicas == 0
}

// GetSubzone returns the most specific Subzone that applies to the specified
// index ID and partition, if any exists. The partition can be left unspecified
// to get the Subzone for an entire index, if it exists. indexID, however, must
// always be provided, even when looking for a partition's Subzone.
func (z *ZoneConfig) GetSubzone(indexID uint32, partition string) *Subzone {
	for _, s := range z.Subzones {
		if s.IndexID == indexID && s.PartitionName == partition {
			return &s
		}
	}
	if partition != "" {
		return z.GetSubzone(indexID, "")
	}
	return nil
}

// GetSubzoneForKeySuffix returns the ZoneConfig for the subzone that contains
// keySuffix, if it exists.
func (z ZoneConfig) GetSubzoneForKeySuffix(keySuffix []byte) *Subzone {
	// TODO(benesch): Use binary search instead.
	for _, s := range z.SubzoneSpans {
		// The span's Key is stored with the prefix removed, so we can compare
		// directly to keySuffix. An unset EndKey implies Key.PrefixEnd().
		if (s.Key.Compare(keySuffix) <= 0) &&
			((s.EndKey == nil && bytes.HasPrefix(keySuffix, s.Key)) || s.EndKey.Compare(keySuffix) > 0) {
			return &z.Subzones[s.SubzoneIndex]
		}
	}
	return nil
}

// SetSubzone installs subzone into the ZoneConfig, overwriting any existing
// subzone with the same IndexID and PartitionName.
func (z *ZoneConfig) SetSubzone(subzone Subzone) {
	for i, s := range z.Subzones {
		if s.IndexID == subzone.IndexID && s.PartitionName == subzone.PartitionName {
			z.Subzones[i] = subzone
			return
		}
	}
	z.Subzones = append(z.Subzones, subzone)
}

// DeleteSubzone removes the subzone with the specified index ID and partition.
// It returns whether it performed any work.
func (z *ZoneConfig) DeleteSubzone(indexID uint32, partition string) bool {
	for i, s := range z.Subzones {
		if s.IndexID == indexID && s.PartitionName == partition {
			z.Subzones = append(z.Subzones[:i], z.Subzones[i+1:]...)
			return true
		}
	}
	return false
}

// DeleteIndexSubzones deletes all subzones that refer to the index with the
// specified ID. This includes subzones for partitions of the index as well as
// the index subzone itself.
func (z *ZoneConfig) DeleteIndexSubzones(indexID uint32) {
	subzones := z.Subzones[:0]
	for _, s := range z.Subzones {
		if s.IndexID != indexID {
			subzones = append(subzones, s)
		}
	}
	z.Subzones = subzones
}

func (z ZoneConfig) subzoneSplits() []roachpb.RKey {
	var out []roachpb.RKey
	for _, span := range z.SubzoneSpans {
		// TODO(benesch): avoid a split at the first partition's start key when it
		// is the minimum possible value.
		if len(out) == 0 || !out[len(out)-1].Equal(span.Key) {
			// Only split at the start key when it differs from the last end key.
			out = append(out, roachpb.RKey(span.Key))
		}
		endKey := span.EndKey
		if len(endKey) == 0 {
			endKey = span.Key.PrefixEnd()
		}
		out = append(out, roachpb.RKey(endKey))
		// TODO(benesch): avoid a split at the last partition's end key when it is
		// the maximum possible value.
	}
	return out
}
