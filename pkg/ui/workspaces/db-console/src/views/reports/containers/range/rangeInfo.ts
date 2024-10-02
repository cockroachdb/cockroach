// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import find from "lodash/find";
import isNil from "lodash/isNil";
import Long from "long";

import * as protos from "src/js/protos";
import { FixLong } from "src/util/fixLong";

export function GetLocalReplica(
  info: protos.cockroach.server.serverpb.IRangeInfo,
): protos.cockroach.roachpb.IReplicaDescriptor {
  return find(
    info.state.state.desc.internal_replicas,
    rep => rep.store_id === info.source_store_id,
  );
}

export function IsLeader(info: protos.cockroach.server.serverpb.IRangeInfo) {
  const localRep = GetLocalReplica(info);
  if (isNil(localRep)) {
    return false;
  }
  return Long.fromInt(localRep.replica_id).eq(FixLong(info.raft_state.lead));
}

export default {
  GetLocalReplica: GetLocalReplica,
  IsLeader: IsLeader,
};
