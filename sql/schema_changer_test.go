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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
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
	db := server.DB()

	// Acquire a lease.
	if err := db.Txn(func(txn *client.Txn) error {
		t := sql.NewSchemaChangeLeaserForTesting(txn)
		var err error
		lease, err = t.Acquire(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(lease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(lease.ExpirationTime, 0))
	}

	// Acquiring another lease will fail.
	var newLease sql.TableDescriptor_SchemaChangeLease
	if err := db.Txn(func(txn *client.Txn) error {
		t := sql.NewSchemaChangeLeaserForTesting(txn)
		var err error
		_, err = t.Acquire(id)
		return err
	}); err == nil {
		t.Fatalf("acquired new lease: %v, while unexpired lease exists: %v", newLease, lease)
	}

	// Extend the lease.
	if err := db.Txn(func(txn *client.Txn) error {
		t := sql.NewSchemaChangeLeaserForTesting(txn)
		var err error
		newLease, err = t.Extend(id, lease)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	if !validExpirationTime(newLease.ExpirationTime) {
		t.Fatalf("invalid expiration time: %s", time.Unix(newLease.ExpirationTime, 0))
	}

	// Extending an old lease fails.
	if err := db.Txn(func(txn *client.Txn) error {
		t := sql.NewSchemaChangeLeaserForTesting(txn)
		var err error
		_, err = t.Extend(id, lease)
		return err
	}); err == nil {
		t.Fatal("extending an old lease succeeded")
	}

	// Releasing an old lease fails.
	if err := db.Txn(func(txn *client.Txn) error {
		t := sql.NewSchemaChangeLeaserForTesting(txn)
		return t.Release(id, lease)
	}); err == nil {
		t.Fatal("releasing a old lease succeeded")
	}

	// Release lease.
	if err := db.Txn(func(txn *client.Txn) error {
		t := sql.NewSchemaChangeLeaserForTesting(txn)
		return t.Release(id, newLease)
	}); err != nil {
		t.Fatal(err)
	}

	// Extending the lease fails.
	if err := db.Txn(func(txn *client.Txn) error {
		t := sql.NewSchemaChangeLeaserForTesting(txn)
		var err error
		_, err = t.Extend(id, newLease)
		return err
	}); err == nil {
		t.Fatalf("was able to extend an already released lease: %d, %v", id, lease)
	}

	// acquiring the lease succeeds
	if err := db.Txn(func(txn *client.Txn) error {
		t := sql.NewSchemaChangeLeaserForTesting(txn)
		var err error
		lease, err = t.Acquire(id)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func validExpirationTime(expirationTime int64) bool {
	now := time.Now()
	return expirationTime > now.Add(sql.LeaseDuration/2).Unix() && expirationTime < now.Add(sql.LeaseDuration*3/2).Unix()
}
