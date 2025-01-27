// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package blobfixture

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
)

// FixtureMetadata is the metadata stored as in object storage for each
// fixture. Its serialized to object storage as json. So be mindful of
// backwards compatability when changing or removing fields.
type FixtureMetadata struct {
	// Kind is a user defined string that is used to group fixtures together.
	Kind string `json:"kind"`

	// DataPath is the path to the fixture data in object storage.
	DataPath string `json:"path"`

	// MetadataPath is the path to the metadata for this fixture in object storage.
	MetadataPath string `json:"metadata_path"`

	// CreatedAt is the time the fixture was created.
	CreatedAt time.Time `json:"created_at"`

	// ReadyAt is the time the fixture was made ready for use.
	ReadyAt *time.Time `json:"ready_at,omitempty"`
}

func (f *FixtureMetadata) MarshalJson() ([]byte, error) {
	// Indent the metadata json so the blobs are easier to read when viewing
	// objects in the cloud consoles.
	return json.MarshalIndent(f, "", "    ")
}

func (f *FixtureMetadata) UnmarshalJson(data []byte) error {
	if err := json.Unmarshal(data, f); err != nil {
		return errors.Wrap(err, "unmarshal fixture metadata")
	}
	if f.Kind == "" {
		return errors.New("missing kind")
	}
	if f.DataPath == "" {
		return errors.New("missing data path")
	}
	if f.MetadataPath == "" {
		return errors.New("missing metadata path")
	}
	if f.CreatedAt.IsZero() {
		return errors.New("missing created at")
	}
	return nil
}

func mostRecent(fixture []FixtureMetadata) *FixtureMetadata {
	var mostRecent *FixtureMetadata
	for _, f := range fixture {
		if f.ReadyAt == nil || f.ReadyAt.IsZero() {
			continue
		}
		if mostRecent == nil || f.ReadyAt.After(*mostRecent.ReadyAt) {
			mostRecent = &f
		}
	}
	return mostRecent
}

type fixtureToDelete struct {
	metadata FixtureMetadata
	reason   string
}

// fixturesToGc returns a list of fixtures to delete. The policy is as follows:
//
// If a fixture is not ready within 48 hours, assume creation failed and it
// was leaked.
//
// A fixture has a successor if there is another fixture of the same kind
// that was made ready after it. A fixture is eligible for gc if it has a
// successor and the successor was made ready more than 24 hours ago. The 24
// hour wait is to ensure no tests are in the middle of using the fixture.
//
// GC decisions are made soly based on the metadata. There is no attempt to
// examine actual live data in object storage. This ensures the GC will only
// delete data that is managed by the fixture registry, so its safe to mix
// manually managed and non-managed fixtures. This decision may be worth
// revisiting if the registry is given its own bucket and is guaranteed to own
// all data in it.
func fixturesToGc(gcAt time.Time, allFixtures []FixtureMetadata) []fixtureToDelete {
	// If a fixtures is not ready within 48 hours, assume creation failed and it
	// was leaked.
	leakedAtThreshold := gcAt.Add(-48 * time.Hour)

	// A fixture is eligible for gc if it has a successor and the successor was
	// made ready more than 24 hours ago.
	obsoleteThreshold := gcAt.Add(-24 * time.Hour)

	toDelete := []fixtureToDelete{}

	byKind := make(map[string][]FixtureMetadata)
	for _, f := range allFixtures {
		if f.ReadyAt == nil || f.ReadyAt.IsZero() {
			if f.CreatedAt.Before(leakedAtThreshold) {
				toDelete = append(toDelete, fixtureToDelete{
					metadata: f,
					reason:   "fixture was not made ready within 48 hours",
				})
				continue
			} else {
				// fixtures is being created and is not eligible for gc
				continue
			}
		}
		byKind[f.Kind] = append(byKind[f.Kind], f)
	}

	for kind := range byKind {
		// Sort by ReadyAt in descending order so that index 0 is the most recent
		// fixture.
		slices.SortFunc(byKind[kind], func(a, b FixtureMetadata) int {
			return -a.ReadyAt.Compare(*b.ReadyAt)
		})
	}

	for _, fixtures := range byKind {
		// NOTE: starting at 1 because index 0 is the most recent fixture and is
		// not eligible for garbage collection.
		for i := 1; i < len(fixtures); i++ {
			successor := fixtures[i-1]
			if successor.ReadyAt.Before(obsoleteThreshold) {
				toDelete = append(toDelete, fixtureToDelete{
					metadata: fixtures[i],
					reason:   fmt.Sprintf("fixture '%s' is was mode obsolete by '%s' at '%s'", fixtures[i].DataPath, successor.DataPath, successor.ReadyAt),
				})
			}
		}
	}

	return toDelete
}
