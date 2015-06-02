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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)

package structured_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/structured"
	"github.com/cockroachdb/cockroach/util"
)

func TestPutGetDeleteSchema(t *testing.T) {
	s, err := createTestSchema()
	if err != nil {
		t.Fatalf("could not create test schema: %v", err)
	}
	stopper := util.NewStopper()
	defer stopper.Stop()
	e := engine.NewInMem(proto.Attributes{}, 1<<20)
	localKV, err := server.BootstrapCluster("test-cluster", []engine.Engine{e}, stopper)
	if err != nil {
		t.Fatalf("unable to boostrap cluster: %v", err)
	}
	db := structured.NewDB(localKV.NewDB())
	if err := db.PutSchema(s); err != nil {
		t.Fatalf("could not register schema: %v", err)
	}
	if s, err = db.GetSchema(s.Key); err != nil {
		t.Errorf("could not get schema with key %q: %v", s.Key, err)
	}
	expectedName := "PhotoDB"
	if s.Name != expectedName {
		t.Errorf("expected schema to be named %q; got %q", expectedName, s.Name)
	}
	if err := db.DeleteSchema(s); err != nil {
		t.Errorf("could not delete schema: %v", err)
	}
	if s, err = db.GetSchema(s.Key); err != nil {
		t.Errorf("could not get schema with key %q: %v", s.Key, err)
	}
	if s != nil {
		t.Errorf("expected schema to be nil; got %+v", s)
	}
}

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
	ID       int64              `roach:"id,pk,auto=10000,scatter"`
	UserID   int64              `roach:"ui,fk=User.ID,ondelete=setnull"`
	Location structured.LatLong `roach:"lo,locationindex"`
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

func createTestSchema() (*structured.Schema, error) {
	sm := map[string]interface{}{
		"us": User{},
		"id": Identity{},
		"ph": Photo{},
		"ps": PhotoStream{},
		"sp": StreamPost{},
		"co": Comment{},
	}
	s, err := structured.NewGoSchema("PhotoDB", "pdb", sm)
	if err != nil {
		return nil, err
	}
	return s, nil
}
