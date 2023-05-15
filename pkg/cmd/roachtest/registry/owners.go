// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package registry

// Owner is a valid entry for the Owners field of a roachtest. They should be
// teams, not individuals.
type Owner string

// The allowable values of Owner.
const (
	OwnerCDC              Owner = `cdc`
	OwnerDisasterRecovery Owner = `disaster-recovery`
	OwnerKV               Owner = `kv`
	OwnerReplication      Owner = `replication`
	OwnerAdmissionControl Owner = `admission-control`
	OwnerObsInf           Owner = `obs-inf-prs`
	OwnerServer           Owner = `server` // not currently staffed
	OwnerSQLFoundations   Owner = `sql-foundations`
	OwnerSQLQueries       Owner = `sql-queries`
	OwnerStorage          Owner = `storage`
	OwnerTestEng          Owner = `test-eng`
	OwnerDevInf           Owner = `dev-inf`
	OwnerMultiTenant      Owner = `multi-tenant`
	OwnerClusterObs       Owner = `cluster-observability`
)
