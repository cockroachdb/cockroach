// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zonepb

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// Several ranges outside of the SQL keyspace are given special names so they
// can be targeted by zone configs.
const (
	DefaultZoneName    = "default"
	LivenessZoneName   = "liveness"
	MetaZoneName       = "meta"
	SystemZoneName     = "system"
	TimeseriesZoneName = "timeseries"
	TenantsZoneName    = "tenants"
)

// NamedZones maps named zones to their pseudo-table ID that can be used to
// install an entry into the system.zones table.
var NamedZones = map[string]uint32{
	DefaultZoneName:    keys.RootNamespaceID,
	LivenessZoneName:   keys.LivenessRangesID,
	MetaZoneName:       keys.MetaRangesID,
	SystemZoneName:     keys.SystemRangesID,
	TimeseriesZoneName: keys.TimeseriesRangesID,
	TenantsZoneName:    keys.TenantsRangesID,
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

// MultiRegionZoneConfigFields are the fields on a zone configuration which
// may be set by the system for multi-region objects".
var MultiRegionZoneConfigFields = []tree.Name{
	"global_reads",
	"num_replicas",
	"num_voters",
	"constraints",
	"voter_constraints",
	"lease_preferences",
}

// MultiRegionZoneConfigFieldsSet contain the items in
// MultiRegionZoneConfigFields but in a set form for fast lookup.
var MultiRegionZoneConfigFieldsSet = func() map[tree.Name]struct{} {
	ret := make(map[tree.Name]struct{}, len(MultiRegionZoneConfigFields))
	for _, f := range MultiRegionZoneConfigFields {
		ret[f] = struct{}{}
	}
	return ret
}()

// ZoneSpecifierFromID creates a tree.ZoneSpecifier for the zone with the
// given ID.
func ZoneSpecifierFromID(
	id uint32, resolveID func(id uint32) (parentID, parentSchemaID uint32, name string, err error),
) (tree.ZoneSpecifier, error) {
	if name, ok := NamedZonesByID[id]; ok {
		return tree.ZoneSpecifier{NamedZone: tree.UnrestrictedName(name)}, nil
	}
	parentID, parentSchemaID, name, err := resolveID(id)
	if err != nil {
		return tree.ZoneSpecifier{}, err
	}
	if parentID == keys.RootNamespaceID {
		return tree.ZoneSpecifier{Database: tree.Name(name)}, nil
	}
	_, _, schemaName, err := resolveID(parentSchemaID)
	if err != nil {
		return tree.ZoneSpecifier{}, err
	}
	_, _, databaseName, err := resolveID(parentID)
	if err != nil {
		return tree.ZoneSpecifier{}, err
	}
	return tree.ZoneSpecifier{
		TableOrIndex: tree.TableIndexName{
			Table: tree.MakeTableNameWithSchema(tree.Name(databaseName), tree.Name(schemaName), tree.Name(name)),
		},
	}, nil
}

// ResolveZoneSpecifier converts a zone specifier to the ID of most specific
// zone whose config applies.
func ResolveZoneSpecifier(
	zs *tree.ZoneSpecifier,
	resolveName func(parentID uint32, schemaID uint32, name string) (id uint32, err error),
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
		return resolveName(keys.RootNamespaceID, keys.RootNamespaceID, string(zs.Database))
	}

	// Third case: a table or index name. We look up the table part here.

	tn := &zs.TableOrIndex.Table
	databaseID, err := resolveName(keys.RootNamespaceID, keys.RootNamespaceID, tn.Catalog())
	if err != nil {
		return 0, err
	}
	schemaID := uint32(keys.PublicSchemaID)
	if tn.SchemaName != tree.PublicSchemaName {
		schemaID, err = resolveName(databaseID, keys.RootNamespaceID, tn.Schema())
		if err != nil {
			return 0, err
		}
	}
	tableID, err := resolveName(databaseID, schemaID, tn.Table())
	if err != nil {
		return 0, err
	}
	return tableID, err
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
		NumVoters:                 proto.Int32(0),
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
		RangeMinBytes: proto.Int64(128 << 20), // 128 MB
		RangeMaxBytes: proto.Int64(512 << 20), // 512 MB
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
		// The default zone is supposed to have empty VoterConstraints.
		NullVoterConstraintsIsEmpty: true,
	}
}

// DefaultZoneConfigRef is the default zone configuration used when no custom
// config has been specified.
func DefaultZoneConfigRef() *ZoneConfig {
	zoneConfig := DefaultZoneConfig()
	return &zoneConfig
}

// DefaultSystemZoneConfig is the default zone configuration used when no custom
// config has been specified. The DefaultSystemZoneConfig is like the
// DefaultZoneConfig but has a replication factor of 5 instead of 3.
func DefaultSystemZoneConfig() ZoneConfig {
	defaultSystemZoneConfig := DefaultZoneConfig()
	defaultSystemZoneConfig.NumReplicas = proto.Int32(5)
	return defaultSystemZoneConfig
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
		(!z.InheritedVoterConstraints()) && (!z.InheritedConstraints) &&
		(!z.InheritedLeasePreferences))
}

// InheritedVoterConstraints determines whether the `VoterConstraints` field is
// explicitly set on this zone or if it is to be inherited from its parent.
func (z *ZoneConfig) InheritedVoterConstraints() bool {
	return len(z.VoterConstraints) == 0 && !z.NullVoterConstraintsIsEmpty
}

// ValidateTandemFields returns an error if the ZoneConfig to be written
// specifies a configuration that could cause problems with the introduction
// of cascading zone configs.
func (z *ZoneConfig) ValidateTandemFields() error {
	var numConstrainedRepls int32
	numVotersExplicit := z.NumVoters != nil && *z.NumVoters > 0
	for _, constraint := range z.Constraints {
		numConstrainedRepls += constraint.NumReplicas
	}

	if numConstrainedRepls > 0 && z.NumReplicas == nil {
		return fmt.Errorf("when per-replica constraints are set, num_replicas must be set as well")
	}

	var numConstrainedVoters int32
	for _, constraint := range z.VoterConstraints {
		numConstrainedVoters += constraint.NumReplicas
	}

	if (numConstrainedVoters > 0 && z.NumVoters == nil) ||
		(!numVotersExplicit && len(z.VoterConstraints) > 0) {
		return fmt.Errorf("when voter_constraints are set, num_voters must be set as well")
	}

	if (z.RangeMinBytes != nil || z.RangeMaxBytes != nil) &&
		(z.RangeMinBytes == nil || z.RangeMaxBytes == nil) {
		return fmt.Errorf("range_min_bytes and range_max_bytes must be set together")
	}
	if numVotersExplicit {
		if !z.InheritedLeasePreferences && z.InheritedVoterConstraints() {
			return fmt.Errorf("lease preferences can not be set unless the voter_constraints are explicitly set as well")
		}
	} else if !z.InheritedLeasePreferences && z.InheritedConstraints {
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
			if !(z.NumVoters != nil && *z.NumVoters > 0) {
				return fmt.Errorf("at least 3 replicas are required for multi-replica configurations")
			}
		}
	}

	var numVotersExplicit bool
	if z.NumVoters != nil {
		numVotersExplicit = true
		switch {
		case *z.NumVoters <= 0:
			return fmt.Errorf("at least one voting replica is required")
		case *z.NumVoters == 2:
			return fmt.Errorf("at least 3 voting replicas are required for multi-replica configurations")
		}
		if z.NumReplicas != nil && *z.NumVoters > *z.NumReplicas {
			return fmt.Errorf("num_voters cannot be greater than num_replicas")
		}
	}

	if z.RangeMaxBytes != nil && *z.RangeMaxBytes < base.MinRangeMaxBytes {
		return fmt.Errorf("RangeMaxBytes %d less than minimum allowed %d",
			*z.RangeMaxBytes, base.MinRangeMaxBytes)
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

	for _, constraints := range z.VoterConstraints {
		for _, constraint := range constraints.Constraints {
			if constraint.Type == Constraint_DEPRECATED_POSITIVE {
				return fmt.Errorf("voter_constraints must be of type 'required' (prefixed with a '+')")
			}
			// TODO(aayush): Allowing these makes validating `voter_constraints`
			// against `constraints` harder. Revisit this decision if need be.
			if constraint.Type == Constraint_PROHIBITED {
				return fmt.Errorf("voter_constraints cannot contain prohibitive constraints")
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

	// If we have per replica constraints inside voter_constraints, make sure
	// that the number of replicas adds up to less than the number of voters.
	//
	// NB: We intentionally allow the number of replicas constrained by
	// `constraints` plus the number of voters constrained by `voter_constraints`
	// to exceed num_voters.
	// For instance, the following would be a valid zone configuration:
	// num_replicas = 3
	// num_voters = 3
	// constraints = {"+region=A": 1, "+region=B": 1, "+region=C": 1}
	// voter_constraints = {"+ssd": 3}
	// In the current state of our zone config validation logic, allowing examples
	// like the one shown above also allows the user to walk themselves into
	// unsatisfiable zone configurations like the following:
	// num_replicas = 3
	// num_voters = 3
	// constraints = {"+region=A": 2, "+region=B": 1}
	// voter_constraints = {"+region=C": 2, "+region=D": 1}
	if numVotersExplicit {
		if len(z.VoterConstraints) > 1 || (len(z.VoterConstraints) == 1 && z.VoterConstraints[0].NumReplicas != 0) {
			var numConstrainedRepls int64
			for _, constraints := range z.VoterConstraints {
				if constraints.NumReplicas <= 0 {
					return fmt.Errorf("constraints must apply to at least one replica")
				}
				numConstrainedRepls += int64(constraints.NumReplicas)
			}
			// NB: These nil checks are not required in production code but they are
			// for testing as some tests run `Validate()` on incomplete zone configs.
			if z.NumVoters != nil && numConstrainedRepls > int64(*z.NumVoters) {
				return fmt.Errorf("the number of replicas specified in voter_constraints (%d) cannot be greater "+
					"than the number of voters configured for the zone (%d)",
					numConstrainedRepls, *z.NumVoters)
			}
		}
	}

	//  Validate that `constraints` aren't incompatible with `voter_constraints`.
	if err := validateVoterConstraintsCompatibility(z.VoterConstraints, z.Constraints); err != nil {
		return err
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

// validateVoterConstraintsCompatibility cross-validates `voter_constraints`
// against `constraints` and ensures that nothing that is prohibited at the
// overall `constraints` level is required at the `voter_constraints` level,
// since this sort of incongruity will lead to an unsatisfiable zone
// configuration.
func validateVoterConstraintsCompatibility(
	voterConstraints, overallConstraints []ConstraintsConjunction,
) error {
	// We know that prohibitive constraints are not allowed under
	// `voter_constraints`. Walk through overallConstraints to ensure that none of
	// the prohibitive constraints conflict with the `required` constraints in
	// voterConstraints.
	for _, constraints := range overallConstraints {
		for _, constraint := range constraints.Constraints {
			if constraint.Type == Constraint_PROHIBITED {
				for _, otherConstraints := range voterConstraints {
					for _, otherConstraint := range otherConstraints.Constraints {
						conflicting := otherConstraint.Value == constraint.Value && otherConstraint.Key == constraint.Key
						if conflicting {
							return fmt.Errorf("prohibitive constraint %s conflicts with voter_constraint %s", constraint, otherConstraint)
						}
					}
				}
			}
		}
	}
	return nil
}

// InheritFromParent hydrates a zones missing fields from its parent.
func (z *ZoneConfig) InheritFromParent(parent *ZoneConfig) {
	// Allow for subzonePlaceholders to inherit fields from parents if needed.
	if z.NumReplicas == nil || (z.NumReplicas != nil && *z.NumReplicas == 0) {
		if parent.NumReplicas != nil {
			z.NumReplicas = proto.Int32(*parent.NumReplicas)
		}
	}
	if z.NumVoters == nil || (z.NumVoters != nil && *z.NumVoters == 0) {
		if parent.NumVoters != nil {
			z.NumVoters = proto.Int32(*parent.NumVoters)
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
	if z.InheritedVoterConstraints() {
		if !parent.InheritedVoterConstraints() {
			z.VoterConstraints = parent.VoterConstraints
			z.NullVoterConstraintsIsEmpty = parent.NullVoterConstraintsIsEmpty
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
		switch fieldName {
		case "num_replicas":
			z.NumReplicas = nil
			if other.NumReplicas != nil {
				z.NumReplicas = proto.Int32(*other.NumReplicas)
			}
		case "num_voters":
			z.NumVoters = nil
			if other.NumVoters != nil {
				z.NumVoters = proto.Int32(*other.NumVoters)
			}
		case "range_min_bytes":
			z.RangeMinBytes = nil
			if other.RangeMinBytes != nil {
				z.RangeMinBytes = proto.Int64(*other.RangeMinBytes)
			}
		case "range_max_bytes":
			z.RangeMaxBytes = nil
			if other.RangeMaxBytes != nil {
				z.RangeMaxBytes = proto.Int64(*other.RangeMaxBytes)
			}
		case "global_reads":
			z.GlobalReads = nil
			if other.GlobalReads != nil {
				z.GlobalReads = proto.Bool(*other.GlobalReads)
			}
		case "gc.ttlseconds":
			z.GC = nil
			if other.GC != nil {
				tempGC := *other.GC
				z.GC = &tempGC
			}
		case "constraints":
			z.Constraints = other.Constraints
			z.InheritedConstraints = other.InheritedConstraints
		case "voter_constraints":
			z.VoterConstraints = other.VoterConstraints
			z.NullVoterConstraintsIsEmpty = other.NullVoterConstraintsIsEmpty
		case "lease_preferences":
			z.LeasePreferences = other.LeasePreferences
			z.InheritedLeasePreferences = other.InheritedLeasePreferences
		}
	}
}

// DiffWithZoneMismatch indicates a mismatch between zone configurations.
type DiffWithZoneMismatch struct {
	// NOTE: the below fields are only set if there is a subzone in the
	// zone configuration which is mismatching.

	// IndexID represents a subzone with a mismatching index ID.
	IndexID uint32
	// PartitionName represents a subzone with a mismatching partitionName.
	PartitionName string

	// IsMissingSubzone indicates a subzone is missing.
	IsMissingSubzone bool
	// IsExtraSubzone indicates we have an extraneous subzone.
	IsExtraSubzone bool
	// Field indicates the field which is wrong.
	Field string
}

// DiffWithZone diffs all specified fields of the supplied ZoneConfig, with the
// receiver ZoneConfig. Returns true if all are equal, and false if there is a
// difference (along with a DiffWithZoneMismatch which represents the first
// difference found).
func (z *ZoneConfig) DiffWithZone(
	other ZoneConfig, fieldList []tree.Name,
) (bool, DiffWithZoneMismatch, error) {
	mismatchingNumReplicas := false
	for _, fieldName := range fieldList {
		switch fieldName {
		case "num_replicas":
			if other.NumReplicas == nil && z.NumReplicas == nil {
				continue
			}
			if z.NumReplicas == nil || other.NumReplicas == nil ||
				*z.NumReplicas != *other.NumReplicas {
				// In cases where one of the zone configs are placeholders,
				// defer the error reporting to below so that we can correctly
				// report on a subzone difference, should one exist.
				if z.IsSubzonePlaceholder() || other.IsSubzonePlaceholder() {
					mismatchingNumReplicas = true
					continue
				}
				return false, DiffWithZoneMismatch{
					Field: "num_replicas",
				}, nil
			}
		case "num_voters":
			if other.NumVoters == nil && z.NumVoters == nil {
				continue
			}
			if z.NumVoters == nil || other.NumVoters == nil ||
				*z.NumVoters != *other.NumVoters {
				return false, DiffWithZoneMismatch{
					Field: "num_voters",
				}, nil
			}
		case "range_min_bytes":
			if other.RangeMinBytes == nil && z.RangeMinBytes == nil {
				continue
			}
			if z.RangeMinBytes == nil || other.RangeMinBytes == nil ||
				*z.RangeMinBytes != *other.RangeMinBytes {
				return false, DiffWithZoneMismatch{
					Field: "range_min_bytes",
				}, nil
			}
		case "range_max_bytes":
			if other.RangeMaxBytes == nil && z.RangeMaxBytes == nil {
				continue
			}
			if z.RangeMaxBytes == nil || other.RangeMaxBytes == nil ||
				*z.RangeMaxBytes != *other.RangeMaxBytes {
				return false, DiffWithZoneMismatch{
					Field: "range_max_bytes",
				}, nil
			}
		case "global_reads":
			if other.GlobalReads == nil && z.GlobalReads == nil {
				continue
			}
			if z.GlobalReads == nil || other.GlobalReads == nil ||
				*z.GlobalReads != *other.GlobalReads {
				return false, DiffWithZoneMismatch{
					Field: "global_reads",
				}, nil
			}
		case "gc.ttlseconds":
			if other.GC == nil && z.GC == nil {
				continue
			}
			if z.GC == nil || other.GC == nil || *z.GC != *other.GC {
				return false, DiffWithZoneMismatch{
					Field: "gc.ttlseconds",
				}, nil
			}
		case "constraints":
			if other.Constraints == nil && z.Constraints == nil {
				continue
			}
			if z.Constraints == nil || other.Constraints == nil {
				return false, DiffWithZoneMismatch{
					Field: "constraints",
				}, nil
			}
			for i, c := range z.Constraints {
				for j, constraint := range c.Constraints {
					if len(other.Constraints) <= i ||
						len(other.Constraints[i].Constraints) <= j ||
						constraint != other.Constraints[i].Constraints[j] {
						return false, DiffWithZoneMismatch{
							Field: "constraints",
						}, nil
					}
				}
			}
		case "voter_constraints":
			if other.VoterConstraints == nil && z.VoterConstraints == nil {
				continue
			}
			if z.VoterConstraints == nil || other.VoterConstraints == nil {
				return false, DiffWithZoneMismatch{
					Field: "voter_constraints",
				}, nil
			}
			for i, c := range z.VoterConstraints {
				for j, constraint := range c.Constraints {
					if len(other.VoterConstraints) <= i ||
						len(other.VoterConstraints[i].Constraints) <= j ||
						constraint != other.VoterConstraints[i].Constraints[j] {
						return false, DiffWithZoneMismatch{
							Field: "voter_constraints",
						}, nil
					}
				}
			}
		case "lease_preferences":
			if other.LeasePreferences == nil && z.LeasePreferences == nil {
				continue
			}
			if z.LeasePreferences == nil || other.LeasePreferences == nil {
				return false, DiffWithZoneMismatch{
					Field: "voter_constraints",
				}, nil
			}
			for i, c := range z.LeasePreferences {
				for j, constraint := range c.Constraints {
					if len(other.LeasePreferences) <= i ||
						len(other.LeasePreferences[i].Constraints) <= j ||
						constraint != other.LeasePreferences[i].Constraints[j] {
						return false, DiffWithZoneMismatch{
							Field: "lease_preferences",
						}, nil
					}
				}
			}
		default:
			return false, DiffWithZoneMismatch{}, errors.AssertionFailedf("unknown zone configuration field %q", fieldName)
		}
	}

	// Look into all subzones and ensure they're equal across both zone
	// configs.
	// These need to be read in as a map as subzones can be added out-of-order.
	type subzoneKey struct {
		indexID       uint32
		partitionName string
	}
	otherSubzonesBySubzoneKey := make(map[subzoneKey]Subzone, len(other.Subzones))
	for _, o := range other.Subzones {
		k := subzoneKey{indexID: o.IndexID, partitionName: o.PartitionName}
		otherSubzonesBySubzoneKey[k] = o
	}
	for _, s := range z.Subzones {
		k := subzoneKey{indexID: s.IndexID, partitionName: s.PartitionName}
		o, found := otherSubzonesBySubzoneKey[k]
		if !found {
			// There can be an extra zone config defined so long as
			// it doesn't have any fields in the fieldList set.
			if b, subzoneMismatch, err := s.Config.DiffWithZone(
				*NewZoneConfig(),
				fieldList,
			); err != nil {
				return b, subzoneMismatch, err
			} else if !b {
				return false, DiffWithZoneMismatch{
					IndexID:        s.IndexID,
					PartitionName:  s.PartitionName,
					IsExtraSubzone: true,
					Field:          subzoneMismatch.Field,
				}, nil
			}
			continue
		}
		if b, subzoneMismatch, err := s.Config.DiffWithZone(
			o.Config,
			fieldList,
		); err != nil {
			return b, subzoneMismatch, err
		} else if !b {
			// We should never have subzones nested within subzones.
			if subzoneMismatch.IndexID > 0 {
				return false, DiffWithZoneMismatch{}, errors.AssertionFailedf(
					"unexpected subzone index id %d",
					subzoneMismatch.IndexID,
				)
			}
			return b, DiffWithZoneMismatch{
				IndexID:       o.IndexID,
				PartitionName: o.PartitionName,
				Field:         subzoneMismatch.Field,
			}, nil
		}
		delete(otherSubzonesBySubzoneKey, k)
	}

	// Anything remaining in the map can be presumed to be missing.
	// This is permitted provided that everything in the field list
	// still matches on an empty zone configuration.
	for _, o := range otherSubzonesBySubzoneKey {
		if b, subzoneMismatch, err := NewZoneConfig().DiffWithZone(
			o.Config,
			fieldList,
		); err != nil {
			return b, subzoneMismatch, err
		} else if !b {
			return false, DiffWithZoneMismatch{
				IndexID:          o.IndexID,
				PartitionName:    o.PartitionName,
				IsMissingSubzone: true,
				Field:            subzoneMismatch.Field,
			}, nil
		}
	}
	// If we've got a mismatch in the num_replicas field and we haven't found
	// any other mismatch, report on num_replicas.
	if mismatchingNumReplicas {
		return false, DiffWithZoneMismatch{
			Field: "num_replicas",
		}, nil
	}
	return true, DiffWithZoneMismatch{}, nil
}

// ClearFieldsOfAllSubzones uses the supplied fieldList and clears those fields
// from all of the zone config's subzones.
func (z *ZoneConfig) ClearFieldsOfAllSubzones(fieldList []tree.Name) {
	newSubzones := z.Subzones[:0]
	emptyZone := NewZoneConfig()
	for _, sz := range z.Subzones {
		// By copying from an empty zone, we'll end up clearing out all of the
		// fields in the fieldList.
		sz.Config.CopyFromZone(*emptyZone, fieldList)
		// If we haven't emptied out the subzone, append it to the new slice.
		if !sz.Config.Equal(emptyZone) {
			newSubzones = append(newSubzones, sz)
		}
	}
	z.Subzones = newSubzones
}

// StoreSatisfiesConstraint checks whether a store satisfies the given constraint.
// If the constraint is of the PROHIBITED type, satisfying it means the store
// not matching the constraint's spec.
func StoreSatisfiesConstraint(store roachpb.StoreDescriptor, constraint Constraint) bool {
	hasConstraint := StoreMatchesConstraint(store, constraint)
	if (constraint.Type == Constraint_REQUIRED && !hasConstraint) ||
		(constraint.Type == Constraint_PROHIBITED && hasConstraint) {
		return false
	}
	return true
}

// StoreMatchesConstraint returns whether a store's attributes or node's
// locality match the constraint's spec. It notably ignores whether the
// constraint is required, prohibited, positive, or otherwise.
// Also see StoreSatisfiesConstraint().
func StoreMatchesConstraint(store roachpb.StoreDescriptor, c Constraint) bool {
	if c.Key == "" {
		for _, attrs := range []roachpb.Attributes{store.Attrs, store.Node.Attrs} {
			for _, attr := range attrs.Attrs {
				if attr == c.Value {
					return true
				}
			}
		}
		return false
	}
	for _, tier := range store.Node.Locality.Tiers {
		if c.Key == tier.Key && c.Value == tier.Value {
			return true
		}
	}
	return false
}

// DeleteTableConfig removes any configuration that applies to the table
// targeted by this ZoneConfig, leaving only its subzone configs, if any. After
// calling DeleteTableConfig, IsSubzonePlaceholder will return true.
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
	// TODO(aayush): Decide whether its worth introducing a isPlaceholder flag to
	// clean this up after num_voters is introduced.
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

// GetSubzoneExact is similar to GetSubzone but does not find the most specific
// subzone that applies to a specified index and partition, as it finds either the
// exact config that applies, or returns nil.
func (z *ZoneConfig) GetSubzoneExact(indexID uint32, partition string) *Subzone {
	for _, s := range z.Subzones {
		if s.IndexID == indexID && s.PartitionName == partition {
			copySubzone := s
			return &copySubzone
		}
	}
	return nil
}

// GetSubzoneForKeySuffix returns the ZoneConfig for the subzone that contains
// keySuffix, if it exists and its position in the subzones slice.
func (z ZoneConfig) GetSubzoneForKeySuffix(keySuffix []byte) (*Subzone, int32) {
	// TODO(benesch): Use binary search instead.
	for _, s := range z.SubzoneSpans {
		// The span's Key is stored with the prefix removed, so we can compare
		// directly to keySuffix. An unset EndKey implies Key.PrefixEnd().
		if (s.Key.Compare(keySuffix) <= 0) &&
			((s.EndKey == nil && bytes.HasPrefix(keySuffix, s.Key)) || s.EndKey.Compare(keySuffix) > 0) {
			copySubzone := z.Subzones[s.SubzoneIndex]
			return &copySubzone, s.SubzoneIndex
		}
	}
	return nil, -1
}

// GetNumVoters returns the number of voting replicas for the given zone config.
//
// This method will panic if called on a ZoneConfig with an uninitialized
// NumReplicas attribute.
func (z *ZoneConfig) GetNumVoters() int32 {
	if z.NumReplicas == nil {
		panic("NumReplicas must not be nil")
	}
	if z.NumVoters != nil && *z.NumVoters != 0 {
		return *z.NumVoters
	}
	return *z.NumReplicas
}

// GetNumNonVoters returns the number of non-voting replicas as defined in the
// zone config.
//
// This method will panic if called on a ZoneConfig with an uninitialized
// NumReplicas attribute.
func (z *ZoneConfig) GetNumNonVoters() int32 {
	if z.NumReplicas == nil {
		panic("NumReplicas must not be nil")
	}
	if z.NumVoters != nil && *z.NumVoters != 0 {
		return *z.NumReplicas - *z.NumVoters
	}
	// `num_voters` hasn't been explicitly configured. Every replica should be a
	// voting replica.
	return 0
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

// SubzoneSplits returns the split points determined by a ZoneConfig's subzones.
func (z ZoneConfig) SubzoneSplits() []roachpb.RKey {
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

// VoterConstraintsCount is part of the cat.Zone interface.
func (z *ZoneConfig) VoterConstraintsCount() int {
	return len(z.VoterConstraints)
}

// VoterConstraint is part of the cat.Zone interface.
func (z *ZoneConfig) VoterConstraint(i int) cat.ReplicaConstraints {
	return &z.VoterConstraints[i]
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

func (c ConstraintsConjunction) String() string {
	var sb strings.Builder
	for i, cons := range c.Constraints {
		if i > 0 {
			sb.WriteRune(',')
		}
		sb.WriteString(cons.String())
	}
	if c.NumReplicas != 0 {
		fmt.Fprintf(&sb, ":%d", c.NumReplicas)
	}
	return sb.String()
}

// ReplicaCount is part of the cat.ReplicaConstraints interface.
func (c *ConstraintsConjunction) ReplicaCount() int32 {
	return c.NumReplicas
}

// ConstraintCount is part of the cat.ReplicaConstraints interface.
func (c *ConstraintsConjunction) ConstraintCount() int {
	return len(c.Constraints)
}

// Constraint is part of the cat.ReplicaConstraints interface.
func (c *ConstraintsConjunction) Constraint(i int) cat.Constraint {
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

// TTL returns the implies TTL as a time.Duration.
func (m *GCPolicy) TTL() time.Duration {
	return time.Duration(m.TTLSeconds) * time.Second
}
