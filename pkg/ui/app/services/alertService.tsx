/** The Alert Service is a  */

import * as React from "react";
import { Store } from "redux";

import { createSelector } from "reselect";
import _ from "lodash";

import { AdminUIState } from "../redux/state";
import * as uiData from "../redux/uiData";
import { refreshNodes, refreshCluster, refreshVersion } from "../redux/apiReducers";
import { VERSION_DISMISSED_KEY, loadUIData /*, saveUIData */ } from "../redux/uiData";
import { VersionList } from "../interfaces/cockroachlabs";
import { NodeStatus } from "../util/proto";

// import { isReactElement } from "../util/find";

// const MILLISECONDS_IN_DAY = moment.duration(1, "day").asMilliseconds();
export const ERROR_LIMIT = 3;

let errors = 0;

/**
 * shouldRun is a function that returns true if no relevant requests are in
 * flight and false otherwise.
 */
export function shouldRun(state: AdminUIState): boolean {
  return !uiData.isInFlight(state, VERSION_DISMISSED_KEY) &&
    !state.cachedData.nodes.inFlight &&
    !state.cachedData.cluster.inFlight &&
    !state.cachedData.version.inFlight &&
    errors < ERROR_LIMIT;
}

let nodeStatusesSelector = (state: AdminUIState): NodeStatus[] => state.cachedData.nodes.data;

let versionsSelector = createSelector(
  nodeStatusesSelector,
  (statuses: NodeStatus[]): string[] => statuses && _.uniq(_.map(statuses, (s: NodeStatus) => s.build_info && s.build_info.tag)),
);

let nodeCountSelector = createSelector(
  nodeStatusesSelector,
  (statuses: NodeStatus[]) => statuses && statuses.length,
);

let versionCountSelector = createSelector(
  versionsSelector,
  (v: string[]) => v && v.length,
);

// // Selector that returns true if all the versions match.
// let versionsMatch = createSelector(
//   versions,
//   (v: string[]) => v && v.length === 1,
// );

let buildtagSelector = createSelector(
  versionsSelector,
  (v: string[]): string => v && v.length === 1 ? v[0] : null,
);

let newerVersionsSelector = (state: AdminUIState): VersionList => state.cachedData.version.valid ? state.cachedData.version.data : null;
let clusterIDSelector = (state: AdminUIState): string => state.cachedData.cluster.data && state.cachedData.cluster.data.cluster_id;

export function loadNecessaryInfo(state: AdminUIState, dispatch: (a: any) => any) {
  let validClusterID: string, buildtag: string;

  if (!state.cachedData.nodes.valid || !state.cachedData.nodes.data) {
    dispatch(refreshNodes());
  } else {
    buildtag = buildtagSelector(state);
  }

  if (!uiData.isValid(state, VERSION_DISMISSED_KEY)) {
    dispatch(loadUIData(VERSION_DISMISSED_KEY));
  }

  if (!state.cachedData.cluster.valid || !state.cachedData.cluster.data || !state.cachedData.cluster.data.cluster_id) {
    dispatch(refreshCluster());
  } else {
    validClusterID = clusterIDSelector(state);
  }

  if (validClusterID && buildtag && !newerVersionsSelector(state)) {
    dispatch(refreshVersion({ clusterID: validClusterID, buildtag }));
  }
}

// TODO: persist alert dismissals in the alert service

enum AlertType {
  VERSION_OUTDATED_ALERT,
  VERSION_MISMATCH_ALERT,
}

export enum AlertIcon {
  WARNING,
}

export class Alert {
  constructor(
    public id: string,
    public type: AlertType,
    public icon: AlertIcon,
    public title: string,
    public content: React.ReactNode,
    public dismissed = false,
    public persisteDismissed = false,
  ) { }
}

// TODO: USE UI REDUCER INSTEAD
let dismissedIds: { [id: string]: boolean } = {};

function getDismissed(id: string) {
  return dismissedIds[id];
}

export function setDismissed(id: string, dismissed: boolean) {
  dismissedIds[id] = dismissed;
}

let makeNewerVersionId = (newerVersions: VersionList) => [newerVersions.details[0].version, newerVersions.details[0].detail, _.last(newerVersions.details).version, _.last(newerVersions.details).detail].join("###");

let alerts: ((state: AdminUIState) => Alert[])[] = [
  // version outdated alert
  createSelector(
    newerVersionsSelector,
    (newerVersions: VersionList) => (newerVersions && newerVersions.details && newerVersions.details.length > 0) ? [new Alert(
      makeNewerVersionId(newerVersions),
      AlertType.VERSION_OUTDATED_ALERT,
      AlertIcon.WARNING,
      "Newer Version",
      <span>There is a newer version of CockroachDB available.</span>,
      getDismissed(makeNewerVersionId(newerVersions)),
    )] : null,
  ),
  // version outdated alert
  createSelector(
    versionCountSelector,
    nodeCountSelector,
    (versionCount: number, nodeCount: number) => versionCount > 1 ? [new Alert(
      `${versionCount} ${nodeCount}`,
      AlertType.VERSION_MISMATCH_ALERT,
      AlertIcon.WARNING,
      "Mismatched Versions",
      <span>Node versions are mismatched. {versionCount} versions were detected across {nodeCount} nodes.</span>,
      getDismissed(`${versionCount} ${nodeCount}`),
    )] : null,
  ),
];

let allAlerts = (state: AdminUIState): Alert[][] => _.map(alerts, (alert) => alert(state));

export let alertsSelector = createSelector(
  allAlerts,
  (a: Alert[][]) => _.compact(_.flatten(a)),
);

export default function refreshAlertData(store: Store<AdminUIState>) {
  return () => {
    let dispatch: (a: any) => any = store.dispatch;
    let state: AdminUIState = store.getState();

    if (shouldRun(state)) {
      loadNecessaryInfo(state, dispatch);
    }
  };
}
