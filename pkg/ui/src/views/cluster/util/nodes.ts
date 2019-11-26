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

export const getLivenessStatusDescription = (status: LivenessStatus) => {
  switch (status) {
    case LivenessStatus.LIVE:
      return "This node is currently healthy.";
    case LivenessStatus.DECOMMISSIONING:
      return  "This node is in the process of being decommissioned. It may take some time to transfer" +
        " the data to other nodes. When finished, it will appear below as a decommissioned node.";
    default:
      return "This node has not recently reported as being live. " +
        "It may not be functioning correctly, but no automatic action has yet been taken.";
  }
};
