// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
