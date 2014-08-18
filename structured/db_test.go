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
// Author: Andrew Bonventre (andybons@gmail.com)

package structured

import (
	"testing"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
)

func TestPutGetDeleteSchema(t *testing.T) {
	s, err := createTestSchema()
	if err != nil {
		t.Fatalf("could not create test schema: %v", err)
	}
	e := engine.NewInMem(engine.Attributes{}, 1<<20)
	localDB, err := server.BootstrapCluster("test-cluster", e)
	if err != nil {
		t.Fatalf("unable to boostrap cluster: %v", err)
	}
	db := NewDB(localDB)
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
