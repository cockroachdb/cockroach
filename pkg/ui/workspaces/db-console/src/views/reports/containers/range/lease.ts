// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";

export function IsLeaseEpoch(lease: protos.cockroach.roachpb.ILease) {
  return !FixLong(lease.epoch).eq(0);
}

export function IsLeaseLeader(lease: protos.cockroach.roachpb.ILease) {
  return !FixLong(lease.term).eq(0);
}

export default {
  IsEpoch: IsLeaseEpoch,
  IsLeader: IsLeaseLeader,
};
