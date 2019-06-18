// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package config

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
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
	return tree.ZoneSpecifier{
		TableOrIndex: tree.TableIndexName{
			Table: tree.MakeTableName(tree.Name(db), tree.Name(name)),
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
	// ParseTableIndexName is not vulnerable to SQL injection, so passing s
	// directly is safe. See #8389 for details.
	parsed, err := parser.ParseTableIndexName(s)
	if err != nil {
		return tree.ZoneSpecifier{}, fmt.Errorf("malformed name: %q", s)
	}

	tn := parsed.Table
	if tn.Table() == "" {
		// In this case, the input was parsed as [[CATALOG.]SCHEMA.]INDEX
		// (this is the syntax when specifying an index, e.g. for ALTER INDEX).
		tn.TableName = tree.Name(parsed.Index)
		parsed.Index = ""
	}

	// To reuse the SQL parsing code, we unfortunately have to abuse TableIndexName
	// here. A table index name is of the form:
	//   [[CATALOG.]SCHEMA.]TABLE[@INDEX]
	// and we want to reinterpret this as
	//   DATABASE[.TABLE[.PARTITION|@INDEX]]
	//
	// To make this conversion we have three cases:
	//   1) one part:    TABLE -> DATABASE
	//   2) two parts:   SCHEMA.TABLE -> DATABASE.TABLE
	//   3) three parts: CATALOG.SCHEMA.TABLE -> DATABASE.TABLE.PARTITION

	if !tn.ExplicitSchema {
		// Case 1: TABLE -> DATABASE.
		return tree.ZoneSpecifier{Database: tn.TableName}, nil
	}

	if !tn.ExplicitCatalog {
		// Case 2: SCHEMA.TABLE -> DATABASE.TABLE
		database, table := tn.SchemaName, tn.TableName
		return tree.ZoneSpecifier{
			TableOrIndex: tree.TableIndexName{
				Table: tree.MakeTableName(database, table),
				Index: parsed.Index,
			},
		}, nil
	}

	// Case 3: CATALOG.SCHEMA.TABLE -> DATABASE.TABLE.PARTITION
	if parsed.Index != "" {
		return tree.ZoneSpecifier{}, fmt.Errorf(
			"index and partition cannot be specified simultaneously: %q", s)
	}
	database, table, partition := tn.CatalogName, tn.SchemaName, tn.TableName
	return tree.ZoneSpecifier{
		TableOrIndex: tree.TableIndexName{
			Table: tree.MakeTableName(database, table),
		},
		Partition: partition,
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

	// The table name may have a schema specifier. CLI zone specifiers
	// do not support this, so strip it.
	ctx := tree.NewFmtCtx(tree.FmtSimple)
	catalog := tree.Name(ti.Table.Catalog())
	ctx.FormatNode(&catalog)
	ctx.WriteByte('.')
	table := tree.UnrestrictedName(ti.Table.Table())
	ctx.FormatNode(&table)

	if zs.Partition != "" {
		ctx.WriteByte('.')
		partition := tree.UnrestrictedName(zs.Partition)
		ctx.FormatNode(&partition)
	} else if ti.Index != "" {
		ctx.WriteByte('@')
		ctx.FormatNode(&ti.Index)
	}
	return ctx.CloseAndGetString()
}

// ResolveZoneSpecifier converts a zone specifier to the ID of most specific
// zone whose config applies.
func ResolveZoneSpecifier(
	zs *tree.ZoneSpecifier, resolveName func(parentID uint32, name string) (id uint32, err error),
) (uint32, error) {
	// A zone specifier has one of 3 possible structures:
	// - a predefined named zone;
	// - a database name;
	// - a table or index name.
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

	// Third case: a table or index name. We look up the table part here.

	tn := &zs.TableOrIndex.Table
	if tn.SchemaName != tree.PublicSchemaName {
		return 0, pgerror.Newf(pgcode.ReservedName,
			"only schema \"public\" is supported: %q", tree.ErrString(tn))
	}
	databaseID, err := resolveName(keys.RootNamespaceID, tn.Catalog())
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
	if len(short) == 0 {
		return fmt.Errorf("the empty string is not a valid constraint")
	}
	switch short[0] {
	case '+':
		c.Type = Constraint_REQUIRED
		short = short[1:]
	case '-':
		c.Type = Constraint_PROHIBITED
		short = short[1:]
	default:
		c.Type = Constraint_DEPRECATED_POSITIVE
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

// minRangeMaxBytes is the minimum value for range max bytes.
const minRangeMaxBytes = 64 << 10 // 64 KB

// NewZoneConfig is the zone configuration used when no custom
// config has been specified.
func NewZoneConfig() *ZoneConfig {
	return &ZoneConfig{
		InheritedConstraints:      true,
		InheritedLeasePreferences: true,
	}
}

// EmptyCompleteZoneConfig is the zone configuration where
// all fields are set but set to their respective zero values.
func EmptyCompleteZoneConfig() *ZoneConfig {
	return &ZoneConfig{
		NumReplicas:               proto.Int32(0),
		RangeMinBytes:             proto.Int64(0),
		RangeMaxBytes:             proto.Int64(0),
		GC:                        &GCPolicy{TTLSeconds: 0},
		InheritedConstraints:      true,
		InheritedLeasePreferences: true,
	}
}

// DefaultZoneConfig is the default zone configuration used when no custom
// config has been specified.
func DefaultZoneConfig() ZoneConfig {
	return ZoneConfig{
		NumReplicas:   proto.Int32(3),
		RangeMinBytes: proto.Int64(16 << 20), // 16 MB
		RangeMaxBytes: proto.Int64(64 << 20), // 64 MB
		GC: &GCPolicy{
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
}

// DefaultZoneConfigRef is the default zone configuration used when no custom
// config has been specified.
func DefaultZoneConfigRef() *ZoneConfig {
	zoneConfig := DefaultZoneConfig()
	return &zoneConfig
}

// DefaultSystemZoneConfig is the default zone configuration used when no custom
// config has been specified.
func DefaultSystemZoneConfig() ZoneConfig {
	return ZoneConfig{
		NumReplicas:   proto.Int32(5),
		RangeMinBytes: proto.Int64(16 << 20), // 16 MB
		RangeMaxBytes: proto.Int64(64 << 20), // 64 MB
		GC: &GCPolicy{
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
}

// DefaultSystemZoneConfigRef is the default zone configuration used when no custom
// config has been specified.
func DefaultSystemZoneConfigRef() *ZoneConfig {
	systemZoneConfig := DefaultSystemZoneConfig()
	return &systemZoneConfig
}

// IsComplete returns whether all the fields are set.
func (z *ZoneConfig) IsComplete() bool {
	return ((z.NumReplicas != nil) && (z.RangeMinBytes != nil) &&
		(z.RangeMaxBytes != nil) && (z.GC != nil) &&
		(!z.InheritedConstraints) && (!z.InheritedLeasePreferences))
}

// ValidateTandemFields returns an error if the ZoneConfig to be written
// specifies a configuration that could cause problems with the introduction
// of cascading zone configs.
func (z *ZoneConfig) ValidateTandemFields() error {
	var numConstrainedRepls int32
	for _, constraint := range z.Constraints {
		numConstrainedRepls += constraint.NumReplicas
	}

	if numConstrainedRepls > 0 && z.NumReplicas == nil {
		return fmt.Errorf("when per-replica constraints are set, num_replicas must be set as well")
	}
	if (z.RangeMinBytes != nil || z.RangeMaxBytes != nil) &&
		(z.RangeMinBytes == nil || z.RangeMaxBytes == nil) {
		return fmt.Errorf("range_min_bytes and range_max_bytes must be set together")
	}
	if !z.InheritedLeasePreferences && z.InheritedConstraints {
		return fmt.Errorf("lease preferences can not be set unless the constraints are explicitly set as well")
	}
	return nil
}

// Validate returns an error if the ZoneConfig specifies a known-dangerous or
// disallowed configuration.
func (z *ZoneConfig) Validate() error {
	for _, s := range z.Subzones {
		if err := s.Config.Validate(); err != nil {
			return err
		}
	}

	if z.NumReplicas != nil {
		switch {
		case *z.NumReplicas < 0:
			return fmt.Errorf("at least one replica is required")
		case *z.NumReplicas == 0:
			if len(z.Subzones) > 0 {
				// NumReplicas == 0 is allowed when this ZoneConfig is a subzone
				// placeholder. See IsSubzonePlaceholder.
				return nil
			}
			return fmt.Errorf("at least one replica is required")
		case *z.NumReplicas == 2:
			return fmt.Errorf("at least 3 replicas are required for multi-replica configurations")
		}
	}

	if z.RangeMaxBytes != nil && *z.RangeMaxBytes < minRangeMaxBytes {
		return fmt.Errorf("RangeMaxBytes %d less than minimum allowed %d",
			*z.RangeMaxBytes, minRangeMaxBytes)
	}

	if z.RangeMinBytes != nil && *z.RangeMinBytes < 0 {
		return fmt.Errorf("RangeMinBytes %d less than minimum allowed 0", *z.RangeMinBytes)
	}
	if z.RangeMinBytes != nil && z.RangeMaxBytes != nil && *z.RangeMinBytes >= *z.RangeMaxBytes {
		return fmt.Errorf("RangeMinBytes %d is greater than or equal to RangeMaxBytes %d",
			*z.RangeMinBytes, *z.RangeMaxBytes)
	}

	// Reserve the value 0 to potentially have some special meaning in the future,
	// such as to disable GC.
	if z.GC != nil && z.GC.TTLSeconds < 1 {
		return fmt.Errorf("GC.TTLSeconds %d less than minimum allowed 1", z.GC.TTLSeconds)
	}

	for _, constraints := range z.Constraints {
		for _, constraint := range constraints.Constraints {
			if constraint.Type == Constraint_DEPRECATED_POSITIVE {
				return fmt.Errorf("constraints must either be required (prefixed with a '+') or " +
					"prohibited (prefixed with a '-')")
			}
		}
	}

	// We only need to further validate constraints if per-replica constraints
	// are in use. The old style of constraints that apply to all replicas don't
	// require validation.
	if len(z.Constraints) > 1 || (len(z.Constraints) == 1 && z.Constraints[0].NumReplicas != 0) {
		var numConstrainedRepls int64
		for _, constraints := range z.Constraints {
			if constraints.NumReplicas <= 0 {
				return fmt.Errorf("constraints must apply to at least one replica")
			}
			numConstrainedRepls += int64(constraints.NumReplicas)
			for _, constraint := range constraints.Constraints {
				// TODO(a-robinson): Relax this constraint to allow prohibited replicas,
				// as discussed on #23014.
				if constraint.Type != Constraint_REQUIRED && z.NumReplicas != nil && constraints.NumReplicas != *z.NumReplicas {
					return fmt.Errorf(
						"only required constraints (prefixed with a '+') can be applied to a subset of replicas")
				}
			}
		}
		if z.NumReplicas != nil && numConstrainedRepls > int64(*z.NumReplicas) {
			return fmt.Errorf("the number of replicas specified in constraints (%d) cannot be greater "+
				"than the number of replicas configured for the zone (%d)",
				numConstrainedRepls, *z.NumReplicas)
		}
	}

	for _, leasePref := range z.LeasePreferences {
		if len(leasePref.Constraints) == 0 {
			return fmt.Errorf("every lease preference must include at least one constraint")
		}
		for _, constraint := range leasePref.Constraints {
			if constraint.Type == Constraint_DEPRECATED_POSITIVE {
				return fmt.Errorf("lease preference constraints must either be required " +
					"(prefixed with a '+') or prohibited (prefixed with a '-')")
			}
		}
	}

	return nil
}

// InheritFromParent hydrates a zones missing fields from its parent.
func (z *ZoneConfig) InheritFromParent(parent *ZoneConfig) {
	if z.NumReplicas == nil {
		if parent.NumReplicas != nil {
			z.NumReplicas = proto.Int32(*parent.NumReplicas)
		}
	}
	if z.RangeMinBytes == nil {
		if parent.RangeMinBytes != nil {
			z.RangeMinBytes = proto.Int64(*parent.RangeMinBytes)
		}
	}
	if z.RangeMaxBytes == nil {
		if parent.RangeMaxBytes != nil {
			z.RangeMaxBytes = proto.Int64(*parent.RangeMaxBytes)
		}
	}
	if z.GC == nil {
		if parent.GC != nil {
			tempGC := *parent.GC
			z.GC = &tempGC
		}
	}
	if z.InheritedConstraints {
		if !parent.InheritedConstraints {
			z.Constraints = parent.Constraints
			z.InheritedConstraints = false
		}
	}
	if z.InheritedLeasePreferences {
		if !parent.InheritedLeasePreferences {
			z.LeasePreferences = parent.LeasePreferences
			z.InheritedLeasePreferences = false
		}
	}
}

// CopyFromZone copies over the specified fields from the other zone.
func (z *ZoneConfig) CopyFromZone(other ZoneConfig, fieldList []tree.Name) {
	for _, fieldName := range fieldList {
		if fieldName == "num_replicas" {
			z.NumReplicas = nil
			if other.NumReplicas != nil {
				z.NumReplicas = proto.Int32(*other.NumReplicas)
			}
		}
		if fieldName == "range_min_bytes" {
			z.RangeMinBytes = nil
			if other.RangeMinBytes != nil {
				z.RangeMinBytes = proto.Int64(*other.RangeMinBytes)
			}
		}
		if fieldName == "range_max_bytes" {
			z.RangeMaxBytes = nil
			if other.RangeMaxBytes != nil {
				z.RangeMaxBytes = proto.Int64(*other.RangeMaxBytes)
			}
		}
		if fieldName == "gc.ttlseconds" {
			z.GC = nil
			if other.GC != nil {
				tempGC := *other.GC
				z.GC = &tempGC
			}
		}
		if fieldName == "constraints" {
			z.Constraints = other.Constraints
			z.InheritedConstraints = other.InheritedConstraints
		}
		if fieldName == "lease_preferences" {
			z.LeasePreferences = other.LeasePreferences
			z.InheritedLeasePreferences = other.InheritedLeasePreferences
		}
	}
}

// StoreMatchesConstraint returns whether a store matches the given constraint.
func StoreMatchesConstraint(store roachpb.StoreDescriptor, constraint Constraint) bool {
	hasConstraint := storeHasConstraint(store, constraint)
	if (constraint.Type == Constraint_REQUIRED && !hasConstraint) ||
		(constraint.Type == Constraint_PROHIBITED && hasConstraint) {
		return false
	}
	return true
}

// storeHasConstraint returns whether a store's attributes or node's locality
// matches the key value pair in the constraint. It notably ignores whether
// the constraint is required, prohibited, positive, or otherwise.
func storeHasConstraint(store roachpb.StoreDescriptor, c Constraint) bool {
	if c.Key == "" {
		for _, attrs := range []roachpb.Attributes{store.Attrs, store.Node.Attrs} {
			for _, attr := range attrs.Attrs {
				if attr == c.Value {
					return true
				}
			}
		}
	} else {
		for _, tier := range store.Node.Locality.Tiers {
			if c.Key == tier.Key && c.Value == tier.Value {
				return true
			}
		}
	}
	return false
}

// DeleteTableConfig removes any configuration that applies to the table
// targeted by this ZoneConfig, leaving only its subzone configs, if any. After
// calling DeleteTableConfig, IsZubzonePlaceholder will return true.
//
// Only table zones can have subzones, so it does not make sense to call this
// method on non-table ZoneConfigs.
func (z *ZoneConfig) DeleteTableConfig() {
	*z = ZoneConfig{
		// Have to set NumReplicas to 0 so it is recognized as a placeholder.
		NumReplicas:  proto.Int32(0),
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
	return z.NumReplicas != nil && *z.NumReplicas == 0
}

// GetSubzone returns the most specific Subzone that applies to the specified
// index ID and partition, if any exists. The partition can be left unspecified
// to get the Subzone for an entire index, if it exists. indexID, however, must
// always be provided, even when looking for a partition's Subzone.
func (z *ZoneConfig) GetSubzone(indexID uint32, partition string) *Subzone {
	for _, s := range z.Subzones {
		if s.IndexID == indexID && s.PartitionName == partition {
			copySubzone := s
			return &copySubzone
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
			copySubzone := z.Subzones[s.SubzoneIndex]
			return &copySubzone
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

// ReplicaConstraintsCount is part of the cat.Zone interface.
func (z *ZoneConfig) ReplicaConstraintsCount() int {
	return len(z.Constraints)
}

// ReplicaConstraints is part of the cat.Zone interface.
func (z *ZoneConfig) ReplicaConstraints(i int) cat.ReplicaConstraints {
	return &z.Constraints[i]
}

// LeasePreferenceCount is part of the cat.Zone interface.
func (z *ZoneConfig) LeasePreferenceCount() int {
	return len(z.LeasePreferences)
}

// LeasePreference is part of the cat.Zone interface.
func (z *ZoneConfig) LeasePreference(i int) cat.ConstraintSet {
	return &z.LeasePreferences[i]
}

// ConstraintCount is part of the cat.LeasePreference interface.
func (l *LeasePreference) ConstraintCount() int {
	return len(l.Constraints)
}

// Constraint is part of the cat.LeasePreference interface.
func (l *LeasePreference) Constraint(i int) cat.Constraint {
	return &l.Constraints[i]
}

// ReplicaCount is part of the cat.ReplicaConstraints interface.
func (c *Constraints) ReplicaCount() int32 {
	return c.NumReplicas
}

// ConstraintCount is part of the cat.ReplicaConstraints interface.
func (c *Constraints) ConstraintCount() int {
	return len(c.Constraints)
}

// Constraint is part of the cat.ReplicaConstraints interface.
func (c *Constraints) Constraint(i int) cat.Constraint {
	return &c.Constraints[i]
}

// IsRequired is part of the cat.Constraint interface.
func (c *Constraint) IsRequired() bool {
	return c.Type == Constraint_REQUIRED
}

// GetKey is part of the cat.Constraint interface.
func (c *Constraint) GetKey() string {
	return c.Key
}

// GetValue is part of the cat.Constraint interface.
func (c *Constraint) GetValue() string {
	return c.Value
}
