// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedbase

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

// Target provides a version-agnostic wrapper around jobspb.ChangefeedTargetSpecification.
type Target struct {
	Type              jobspb.ChangefeedTargetSpecification_TargetType
	TableID           descpb.ID
	FamilyName        string
	StatementTimeName StatementTimeName
}

// StatementTimeName is the original way a table was referred to when it was added to
// the changefeed, possibly modified by WITH options.
type StatementTimeName string

type targetsByTable struct {
	// wholeTable is a single target set when the target has no specified column
	// families
	wholeTable *Target
	// byFamilyName is populated only if there are multiple column family targets
	// for the table
	byFamilyName map[string]Target
}

func (tbt targetsByTable) add(t Target) targetsByTable {
	if t.FamilyName == "" {
		tbt.wholeTable = &t
	} else {
		if tbt.byFamilyName == nil {
			tbt.byFamilyName = make(map[string]Target)
		}
		tbt.byFamilyName[t.FamilyName] = t
	}
	return tbt
}

func (tbt targetsByTable) each(f func(t Target) error) error {
	if tbt.wholeTable != nil {
		if err := f(*tbt.wholeTable); err != nil {
			return err
		}
	}
	for _, t := range tbt.byFamilyName {
		if err := f(t); err != nil {
			return err
		}
	}
	return nil
}

// Targets is the complete list of target specifications for a changefeed.
// This is stored as a map of TableID -> Family Name -> Target in order
// to support all current ways we need to iterate over it.
type Targets struct {
	Size uint
	m    map[descpb.ID]targetsByTable
}

// Add adds a target to the list.
func (ts *Targets) Add(t Target) {
	if ts.m == nil {
		ts.m = make(map[descpb.ID]targetsByTable)
	}
	ts.m[t.TableID] = ts.m[t.TableID].add(t)
	ts.Size++
}

// EachTarget iterates over Targets.
func (ts *Targets) EachTarget(f func(Target) error) error {
	for _, l := range ts.m {
		if err := l.each(f); err != nil {
			return err
		}
	}
	return nil
}

// GetSpecifiedColumnFamilies returns a set of watched families
// belonging to the table.
func (ts *Targets) GetSpecifiedColumnFamilies(tableID descpb.ID) map[string]struct{} {
	target, exists := ts.m[tableID]
	if !exists {
		return make(map[string]struct{})
	}

	families := make(map[string]struct{}, len(target.byFamilyName))
	for family := range target.byFamilyName {
		families[family] = struct{}{}
	}
	return families
}

// EachTableID iterates over unique TableIDs referenced in Targets.
func (ts *Targets) EachTableID(f func(descpb.ID) error) error {
	for id := range ts.m {
		if err := f(id); err != nil {
			return iterutil.Map(err)
		}
	}
	return nil
}

// EachTableIDWithBool is similar to EachTableID but avoids using
// iterutil.Map(err) to eliminate the overhead of errors.Is. Thus, f should not
// return iterutil.StopIteration(). It returns false with error when the
// callback f returns false or true when the iteration completes.
func (ts *Targets) EachTableIDWithBool(f func(descpb.ID) (bool, error)) (bool, error) {
	for id := range ts.m {
		if b, err := f(id); !b {
			return false, err
		}
	}
	return true, nil
}

// EachHavingTableID iterates over each Target with the given id, returning
// false if there were none.
func (ts *Targets) EachHavingTableID(id descpb.ID, f func(Target) error) (bool, error) {
	targets, ok := ts.m[id]
	return ok, targets.each(f)
}

// NumUniqueTables gives the number of unique TableIDs referenced in Targets.
func (ts *Targets) NumUniqueTables() int {
	return len(ts.m)
}

// FindByTableIDAndFamilyName returns a target matching the given table id and family name,
// or false if none were found. If no target matches the family name but a target covers
// the whole table, that target will be returned.
func (ts *Targets) FindByTableIDAndFamilyName(id descpb.ID, family string) (Target, bool) {
	tbt, ok := ts.m[id]
	if !ok {
		return Target{}, false
	}
	if tbt.byFamilyName != nil {
		t, ok := tbt.byFamilyName[family]
		if ok {
			return t, true
		}
	}
	if tbt.wholeTable != nil {
		return *tbt.wholeTable, true
	}
	return Target{}, false
}
