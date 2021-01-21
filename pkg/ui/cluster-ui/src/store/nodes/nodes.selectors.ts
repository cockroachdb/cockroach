import { createSelector } from "@reduxjs/toolkit";
import _ from "lodash";
import { AppState } from "../reducers";
import { getDisplayName } from "../../nodes";
import { livenessStatusByNodeIDSelector } from "../liveness";

export const nodeStatusesSelector = (state: AppState) =>
  state.adminUI.nodes.data;

export const nodeDisplayNameByIDSelector = createSelector(
  nodeStatusesSelector,
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
