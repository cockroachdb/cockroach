// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { LivenessStatus } from "src/redux/nodes";
import { cockroach } from "src/js/protos";

import liveIcon from "!!raw-loader!assets/livenessIcons/live.svg";
import suspectIcon from "!!raw-loader!assets/livenessIcons/suspect.svg";
import deadIcon from "!!raw-loader!assets/livenessIcons/dead.svg";
import decommissioningIcon from "!!raw-loader!assets/livenessIcons/decommissioning.svg";

import NodeLivenessStatus = cockroach.storage.NodeLivenessStatus;

export const getLivenessStatusDescription = (status: LivenessStatus) => {
  switch (status) {
    case LivenessStatus.LIVE:
      return "This node is currently healthy.";
    case LivenessStatus.DECOMMISSIONING:
      return  "This node is in the process of being decommissioned. It may take some time to transfer" +
        " the data to other nodes. When finished, it will appear below as a decommissioned node.";
    case LivenessStatus.DEAD:
      return "This node has not reported as live for a significant period and is considered dead. " +
        "The cut-off period for dead nodes is configurable as cluster setting " +
        "'server.time_until_store_dead'";
    default:
      return "This node has not recently reported as being live. " +
        "It may not be functioning correctly, but no automatic action has yet been taken.";
  }
};

export const getLivenessIcon = (livenessStatus: NodeLivenessStatus) => {
  switch (livenessStatus) {
    case NodeLivenessStatus.LIVE:
      return liveIcon;
    case NodeLivenessStatus.DECOMMISSIONING:
      return decommissioningIcon;
    case NodeLivenessStatus.DEAD:
      return deadIcon;
    default:
      return suspectIcon;
  }
};
