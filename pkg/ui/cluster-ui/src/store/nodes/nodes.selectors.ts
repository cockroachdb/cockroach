// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "@reduxjs/toolkit";
import _ from "lodash";
import { AppState } from "../reducers";
import { getDisplayName } from "../../nodes";
import { livenessStatusByNodeIDSelector } from "../liveness";
import { accumulateMetrics } from "../../util";

export const nodeStatusesSelector = (state: AppState) =>
  state.adminUI.nodes.data || [];

export const nodesSelector = createSelector(
  nodeStatusesSelector,
  accumulateMetrics,
);

export const nodeDisplayNameByIDSelector = createSelector(
  nodesSelector,
  livenessStatusByNodeIDSelector,
  (nodeStatuses, livenessStatusByNodeID) => {
    const result: { [key: string]: string } = {};
    if (!_.isEmpty(nodeStatuses)) {
      nodeStatuses.forEach(ns => {
        result[ns.desc.node_id] = getDisplayName(
          ns,
          livenessStatusByNodeID[ns.desc.node_id],
        );
      });
    }
    return result;
  },
);
