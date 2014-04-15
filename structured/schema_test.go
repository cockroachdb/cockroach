// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package structured

import (
	"testing"

	"github.com/golang/glog"
)

// User is a top-level table. User IDs are scattered, meaning a two
// byte hash of the ID from the UserID sequence is prepended to yield
// a randomly distributed keyspace.
type User struct {
	ID   int64  `roach:"id,pk,auto,scatter"`
	Name string `roach:"na"`
}

// Identity is a top-level table as identities must be queried by key
// at login.
type Identity struct {
	Key    string `roach:"ke,pk,scatter"` // (e.g. email:spencer.kimball@gmail.com, phone:6464174337)
	UserID int64  `roach:"ui,fk=User.ID,ondelete=setnull"`
}

// Photo is a top-level table. While photos can only be contributed by
// a single user, they are then shared and accessible to any number of
// other users. See notes on scatter option for User.ID.
type Photo struct {
	ID       int64   `roach:"id,pk,auto=10000,scatter"`
	UserID   int64   `roach:"ui,fk=User.ID,ondelete=setnull"`
	Location LatLong `roach:"lo,locationindex"`
}

// PhotoStream is a top-level table as streams are shared by multiple
// users. The user ID for a photo stream has an index so that all
// photo streams for a user may be easily queried. Photo stream titles
// are full-text indexed for public searching.
type PhotoStream struct {
	ID     int64  `roach:"id,pk,auto,scatter"`
	UserID int64  `roach:"ui,fk=User.ID"`
	Title  string `roach:"ti,fulltextindex"`
}

// StreamPost is an interleaved join table (on PhotoStream), as the
// posts which link a Photo to a PhotoStream are scoped to a single
// PhotoStream. With interleaved tables, it's not necessary to specify
// the "ondelete" roach option, as it is always set to "cascade".
type StreamPost struct {
	PhotoStreamID int64 `roach:"si,pk,fk=PhotoStream.ID,interleave"`
	PhotoID       int64 `roach:"pi,pk,fk=Photo.ID"`
	Timestamp     int64 `roach:"ti"`
}

// Comment is an interleaved table (on PhotoStream), as comments are
// scoped to a single PhotoStream.
type Comment struct {
	PhotoStreamID int64  `roach:"si,pk,fk=PhotoStream.ID,interleave"`
	ID            int64  `roach:"id,pk,auto"`
	UserID        int64  `roach:"ui,fk=User.ID"`
	Message       string `roach:"me,fulltextindex"`
	Timestamp     int64  `roach:"ti"`
}

func createTestSchema() (*Schema, error) {
	sm := map[string]interface{}{
		"us": User{},
		"id": Identity{},
		"ph": Photo{},
		"ps": PhotoStream{},
		"sp": StreamPost{},
		"co": Comment{},
	}
	s, err := NewGoSchema("PhotoDB", "pdb", sm)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// TestNoPrimaryKey verifies a missing primary key is an error.
func TestNoPrimaryKey(t *testing.T) {
	yaml := `db: Test
db_key: t
tables:
- table: A
  table_key: a`
	if _, err := NewYAMLSchema([]byte(yaml)); err == nil {
		t.Errorf("expected failure on missing primary key")
	}
}

// TestDuplicateTables verifies that duplicate table names and table
// keys are disallowed within a single schema.
func TestDuplicateTables(t *testing.T) {
	badYAML := []string{
		`db: Test
db_key: t
tables:
- table: A
  table_key: a
  columns:
  - column: A
    column_key: a
    primary_key: true
- table: A
  table_key: b
  columns:
  - column: A
    column_key: a
    primary_key: true`,

		`db: Test
db_key: t
tables:
- table: A
  table_key: a
  columns:
  - column: A
    column_key: a
    primary_key: true
- table: B
  table_key: a
  columns:
  - column: A
    column_key: a
    primary_key: true`,
	}

	for i, yaml := range badYAML {
		if _, err := NewYAMLSchema([]byte(yaml)); err == nil {
			t.Errorf("%d: expected failure on duplicate table names", i)
		}
	}
}

// TestDuplicateColumns verifies that duplicate column names and column
// keys are disallowed within a single schema.
func TestDuplicateColumns(t *testing.T) {
	badYAML := []string{
		`db: Test
db_key: t
tables:
- table: A
  table_key: a
  columns:
  - column: A
    column_key: a
    primary_key: true
  - column: A
    column_key: b`,

		`db: Test
db_key: t
tables:
- table: A
  table_key: a
  columns:
  - column: A
    column_key: a
    primary_key: true
  - column: B
    column_key: a`,
	}

	for i, yaml := range badYAML {
		if _, err := NewYAMLSchema([]byte(yaml)); err == nil {
			t.Errorf("%d: expected failure on duplicate column names", i)
		}
	}
}

// TestForeignKeys verifies correct foreign keys.
func TestForeignKeys(t *testing.T) {
	s, err := createTestSchema()
	if err != nil {
		glog.Fatalf("failed building schema: %s", err)
	}
	spT := s.byName["StreamPost"]
	if spT.foreignKeys["PhotoStream"]["ID"] != spT.byName["PhotoStreamID"] {
		t.Errorf("missing expected foreign key from StreamPost.PhotoStreamID to PhotoStream.ID")
	}
	if spT.foreignKeys["Photo"]["ID"] != spT.byName["PhotoID"] {
		t.Errorf("missing expected foreign key from StreamPost.PhotoID to Photo.ID")
	}

	// Test incoming foreign keys.
	psT := s.byName["PhotoStream"]
	if psT.incomingForeignKeys["StreamPost"]["PhotoStreamID"] != spT.byName["PhotoStreamID"] {
		t.Errorf("PhotoStream table missing expected incoming foreign key from StreamPost.PhotoStreamID")
	}
	phT := s.byName["Photo"]
	if phT.incomingForeignKeys["StreamPost"]["PhotoID"] != spT.byName["PhotoID"] {
		t.Errorf("Photo table missing expected incoming foreign key from StreamPost.PhotoID")
	}

	// Modify Identity.UserID's foreign key specification to be just "User"
	// to verify the default is to use the referenced table's primary key.
	s.byName["Identity"].byName["UserID"].ForeignKey = "User"
	if err := s.Validate(); err != nil {
		t.Errorf("error validating default foreign key specification: %v", err)
	}
}

// TestInvalidForeignKeys verifies error conditions in foreign keys.
func TestBadForeignKeys(t *testing.T) {
	s, err := createTestSchema()
	if err != nil {
		glog.Fatalf("failed building schema: %s", err)
	}

	badForeignKeys := []string{
		"Foo",
		"User.Name",
		"User.NOID",
	}
	for i, badFK := range badForeignKeys {
		s.byName["Identity"].byName["UserID"].ForeignKey = badFK
		if err := s.Validate(); err == nil {
			t.Errorf("%d: expected error validating bad foreign key %s", i, badFK)
		}
	}
}

// TestColumnOptions verifies settings of trivial column options
// (e.g. "scatter").
func TestColumnOptions(t *testing.T) {
	s, err := createTestSchema()
	if err != nil {
		glog.Fatalf("failed building schema: %s", err)
	}
	if !s.byName["User"].byName["ID"].PrimaryKey {
		t.Errorf("expected User.ID to be primary key")
	}
	if !s.byName["User"].byName["ID"].Scatter {
		t.Errorf("expected scatter option set on User.ID")
	}
	if *s.byName["Photo"].byName["ID"].Auto != 10000 {
		t.Errorf("expected auto option starting at 10000 on Photo.ID")
	}
	if s.byName["Identity"].byName["UserID"].OnDelete != "setnull" {
		t.Errorf("expected ondelete=setnull for Identity.UserID")
	}
	if !s.byName["Comment"].byName["PhotoStreamID"].Interleave {
		t.Errorf("expected interleave for Comment.PhotoStreamID")
	}
	if s.byName["Comment"].byName["PhotoStreamID"].OnDelete != "cascade" {
		t.Errorf("expected ondelete=cascade for Comment.PhotoStreamID")
	}
	if s.byName["Photo"].byName["Location"].Index != "location" {
		t.Errorf("expected location index on Photo.Location")
	}
	if s.byName["Photo"].byName["Location"].Index != "location" {
		t.Errorf("expected location index on Photo.Location")
	}
	if s.byName["PhotoStream"].byName["Title"].Index != "fulltext" {
		t.Errorf("expected full text index on PhotoStream.Title")
	}
}
