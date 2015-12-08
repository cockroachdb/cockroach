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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestSchemaChangeLease(t *testing.T) {
	defer leaktest.AfterTest(t)
	server, sqlDB, _ := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	var lease sql.TableDescriptor_SchemaChangeLease
	var id = sql.ID(keys.MaxReservedDescID + 2)
	var node = roachpb.NodeID(2)
	db := server.DB()
	changer := sql.NewSchemaChangerForTesting(id, node, *db)

	// Acquire a lease.
	lease, err := changer.AcquireLease()
	if err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(lease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(lease.ExpirationTime, 0))
	}

	// Acquiring another lease will fail.
	var newLease sql.TableDescriptor_SchemaChangeLease
	newLease, err = changer.AcquireLease()
	if err == nil {
		t.Fatalf("acquired new lease: %v, while unexpired lease exists: %v", newLease, lease)
	}

	// Extend the lease.
	newLease, err = changer.ExtendLease(lease)
	if err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(newLease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(newLease.ExpirationTime, 0))
	}

	// Extending an old lease fails.
	_, err = changer.ExtendLease(lease)
	if err == nil {
		t.Fatal("extending an old lease succeeded")
	}

	// Releasing an old lease fails.
	err = changer.ReleaseLease(lease)
	if err == nil {
		t.Fatal("releasing a old lease succeeded")
	}

	// Release lease.
	err = changer.ReleaseLease(newLease)
	if err != nil {
		t.Fatal(err)
	}

	// Extending the lease fails.
	_, err = changer.ExtendLease(newLease)
	if err == nil {
		t.Fatalf("was able to extend an already released lease: %d, %v", id, lease)
	}

	// acquiring the lease succeeds
	lease, err = changer.AcquireLease()
	if err != nil {
		t.Fatal(err)
	}
}

func validExpirationTime(expirationTime int64) bool {
	now := time.Now()
	return expirationTime > now.Add(sql.LeaseDuration/2).UnixNano() && expirationTime < now.Add(sql.LeaseDuration*3/2).UnixNano()
}
