// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/**
 * Alerts is a collection of selectors which determine if there are any Alerts
 * to display based on the current redux state.
 */

import _ from "lodash";
import moment from "moment";
import { createSelector } from "reselect";
import { Store, Dispatch, Action } from "redux";
import { ThunkAction } from "redux-thunk";

import { LocalSetting } from "./localsettings";
import {
  VERSION_DISMISSED_KEY, INSTRUCTIONS_BOX_COLLAPSED_KEY,
  saveUIData, loadUIData, isInFlight, UIDataState, UIDataStatus,
} from "./uiData";
import { refreshCluster, refreshNodes, refreshVersion, refreshHealth } from "./apiReducers";
import { singleVersionSelector, versionsSelector } from "src/redux/nodes";
import { AdminUIState } from "./state";
import * as docsURL from "src/util/docs";

export enum AlertLevel {
  NOTIFICATION,
  WARNING,
  CRITICAL,
  SUCCESS,
}

export interface AlertInfo {
  // Alert Level, which determines visual qualities such as icon and coloring.
  level: AlertLevel;
  // Title to display with the alert.
  title: string;
  // The text of this alert.
  text?: string;
  // Optional hypertext link to be followed when clicking alert.
  link?: string;
}

export interface Alert extends AlertInfo {
  // ThunkAction which will result in this alert being dismissed. This
  // function will be dispatched to the redux store when the alert is dismissed.
  dismiss: ThunkAction<Promise<void>, AdminUIState, void>;
  // Makes alert to be positioned in the top right corner of the screen instead of
  // stretching to full width.
  showAsAlert?: boolean;
  autoClose?: boolean;
  closable?: boolean;
  autoCloseTimeout?: number;
}

const localSettingsSelector = (state: AdminUIState) => state.localSettings;

// Clusterviz Instruction Box collapsed

export const instructionsBoxCollapsedSetting = new LocalSetting(
  INSTRUCTIONS_BOX_COLLAPSED_KEY, localSettingsSelector, false,
);

const instructionsBoxCollapsedPersistentLoadedSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  (uiData): boolean => (
    uiData
      && _.has(uiData, INSTRUCTIONS_BOX_COLLAPSED_KEY)
      && uiData[INSTRUCTIONS_BOX_COLLAPSED_KEY].status === UIDataStatus.VALID
  ),
);

const instructionsBoxCollapsedPersistentSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  (uiData): boolean => (
    uiData
      && _.has(uiData, INSTRUCTIONS_BOX_COLLAPSED_KEY)
      && uiData[INSTRUCTIONS_BOX_COLLAPSED_KEY].status === UIDataStatus.VALID
      && uiData[INSTRUCTIONS_BOX_COLLAPSED_KEY].data
  ),
);

export const instructionsBoxCollapsedSelector = createSelector(
  instructionsBoxCollapsedPersistentLoadedSelector,
  instructionsBoxCollapsedPersistentSelector,
  instructionsBoxCollapsedSetting.selector,
  (persistentLoaded, persistentCollapsed, localSettingCollapsed): boolean => {
    if (persistentLoaded) {
      return persistentCollapsed;
    }
    return localSettingCollapsed;
  },
);

export function setInstructionsBoxCollapsed(collapsed: boolean) {
  return (dispatch: Dispatch<Action, AdminUIState>) => {
    dispatch(instructionsBoxCollapsedSetting.set(collapsed));
    dispatch(saveUIData({
      key: INSTRUCTIONS_BOX_COLLAPSED_KEY,
      value: collapsed,
    }));
  };
}

////////////////////////////////////////
// Version mismatch.
////////////////////////////////////////
export const staggeredVersionDismissedSetting = new LocalSetting(
  "staggered_version_dismissed", localSettingsSelector, false,
);

/**
 * Warning when multiple versions of CockroachDB are detected on the cluster.
 * This excludes decommissioned nodes.
 */
export const staggeredVersionWarningSelector = createSelector(
  versionsSelector,
  staggeredVersionDismissedSetting.selector,
  (versions, versionMismatchDismissed): Alert => {
    if (versionMismatchDismissed) {
      return undefined;
    }

    if (!versions || versions.length <= 1) {
      return undefined;
    }

    return {
      level: AlertLevel.WARNING,
      title: "Staggered Version",
      text: `We have detected that multiple versions of CockroachDB are running
      in this cluster. This may be part of a normal rolling upgrade process, but
      should be investigated if this is unexpected.`,
      dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
        dispatch(staggeredVersionDismissedSetting.set(true));
        return Promise.resolve();
      },
    };
  });

// A boolean that indicates whether the server has yet been checked for a
// persistent dismissal of this notification.
// TODO(mrtracy): Refactor so that we can distinguish "never loaded" from
// "loaded, doesn't exist on server" without a separate selector.
const newVersionDismissedPersistentLoadedSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  (uiData) => uiData && _.has(uiData, VERSION_DISMISSED_KEY),
);

const newVersionDismissedPersistentSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  (uiData) => {
    return (uiData
            && uiData[VERSION_DISMISSED_KEY]
            && uiData[VERSION_DISMISSED_KEY].data
            && moment(uiData[VERSION_DISMISSED_KEY].data)
            ) || moment(0);
  },
);

export const newVersionDismissedLocalSetting = new LocalSetting(
  "new_version_dismissed", localSettingsSelector, moment(0),
);

export const newerVersionsSelector = (state: AdminUIState) => state.cachedData.version.valid ? state.cachedData.version.data : null;

/**
 * Notification when a new version of CockroachDB is available.
 */
export const newVersionNotificationSelector = createSelector(
  newerVersionsSelector,
  newVersionDismissedPersistentLoadedSelector,
  newVersionDismissedPersistentSelector,
  newVersionDismissedLocalSetting.selector,
  (newerVersions, newVersionDismissedPersistentLoaded, newVersionDismissedPersistent, newVersionDismissedLocal): Alert => {
    // Check if there are new versions available.
    if (!newerVersions || !newerVersions.details || newerVersions.details.length === 0) {
      return undefined;
    }

    // Check local dismissal. Local dismissal is valid for one day.
    const yesterday = moment().subtract(1, "day");
    if (newVersionDismissedLocal.isAfter && newVersionDismissedLocal.isAfter(yesterday)) {
      return undefined;
    }

    // Check persistent dismissal, also valid for one day.
    if (!newVersionDismissedPersistentLoaded
        || !newVersionDismissedPersistent
        || newVersionDismissedPersistent.isAfter(yesterday)) {
      return undefined;
    }

    return {
      level: AlertLevel.NOTIFICATION,
      title: "New Version Available",
      text: "A new version of CockroachDB is available.",
      link: docsURL.upgradeCockroachVersion,
      dismiss: (dispatch: any) => {
        const dismissedAt = moment();
        // Dismiss locally.
        dispatch(newVersionDismissedLocalSetting.set(dismissedAt));
        // Dismiss persistently.
        return dispatch(saveUIData({
          key: VERSION_DISMISSED_KEY,
          value: dismissedAt.valueOf(),
        }));
      },
    };
  });

export const disconnectedDismissedLocalSetting = new LocalSetting(
  "disconnected_dismissed", localSettingsSelector, moment(0),
);

/**
 * Notification when the Admin UI is disconnected from the cluster.
 */
export const disconnectedAlertSelector = createSelector(
  (state: AdminUIState) => state.cachedData.health,
  disconnectedDismissedLocalSetting.selector,
  (health, disconnectedDismissed): Alert => {
    if (!health || !health.lastError) {
      return undefined;
    }

    // Allow local dismissal for one minute.
    const dismissedMaxTime = moment().subtract(1, "m");
    if (disconnectedDismissed.isAfter(dismissedMaxTime)) {
      return undefined;
    }

    return {
      level: AlertLevel.CRITICAL,
      title: "We're currently having some trouble fetching updated data. If this persists, it might be a good idea to check your network connection to the CockroachDB cluster.",
      dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
        dispatch(disconnectedDismissedLocalSetting.set(moment()));
        return Promise.resolve();
      },
    };
  },
);

export const emailSubscriptionAlertLocalSetting = new LocalSetting(
  "email_subscription_alert", localSettingsSelector, false,
);

export const emailSubscriptionAlertSelector = createSelector(
  emailSubscriptionAlertLocalSetting.selector,
  ( emailSubscriptionAlert): Alert => {
    if (!emailSubscriptionAlert) {
      return undefined;
    }
    return {
      level: AlertLevel.SUCCESS,
      title: "You successfully signed up for CockroachDB release notes",
      showAsAlert: true,
      autoClose: true,
      closable: false,
      dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
        dispatch(emailSubscriptionAlertLocalSetting.set(false));
        return Promise.resolve();
      },
    };
  },
);

type CreateStatementDiagnosticsAlertPayload = {
  show: boolean;
  status?: "SUCCESS" | "FAILED";
};

export const createStatementDiagnosticsAlertLocalSetting = new LocalSetting<AdminUIState, CreateStatementDiagnosticsAlertPayload>(
  "create_stmnt_diagnostics_alert", localSettingsSelector, { show: false },
);

export const createStatementDiagnosticsAlertSelector = createSelector(
  createStatementDiagnosticsAlertLocalSetting.selector,
  ( createStatementDiagnosticsAlert): Alert => {
    if (!createStatementDiagnosticsAlert || !createStatementDiagnosticsAlert.show) {
      return undefined;
    }
    const { status } = createStatementDiagnosticsAlert;

    if (status === "FAILED") {
      return {
        level: AlertLevel.CRITICAL,
        title: "There was an error activating statement diagnostics",
        text: "Please try activating again. If the problem continues please reach out to customer support.",
        showAsAlert: true,
        dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
          dispatch(createStatementDiagnosticsAlertLocalSetting.set({ show: false }));
          return Promise.resolve();
        },
      };
    }
    return {
      level: AlertLevel.SUCCESS,
      title: "Statement diagnostics were successfully activated",
      showAsAlert: true,
      autoClose: true,
      closable: false,
      dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
        dispatch(createStatementDiagnosticsAlertLocalSetting.set({ show: false }));
        return Promise.resolve();
      },
    };
  },
);

type TerminateSessionAlertPayload = {
  show: boolean;
  status?: "SUCCESS" | "FAILED";
};

export const terminateSessionAlertLocalSetting = new LocalSetting<AdminUIState, TerminateSessionAlertPayload>(
  "terminate_session_alert", localSettingsSelector, { show: false },
);

export const terminateSessionAlertSelector = createSelector(
  terminateSessionAlertLocalSetting.selector,
  (terminateSessionAlert): Alert => {
    if (!terminateSessionAlert || !terminateSessionAlert.show) {
      return undefined;
    }
    const { status } = terminateSessionAlert;

    if (status === "FAILED") {
      return {
        level: AlertLevel.CRITICAL,
        title: "There was an error terminating the session.",
        text: "Please try activating again. If the problem continues please reach out to customer support.",
        showAsAlert: true,
        dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
          dispatch(terminateSessionAlertLocalSetting.set({ show: false }));
          return Promise.resolve();
        },
      };
    }
    return {
      level: AlertLevel.SUCCESS,
      title: "Session terminated.",
      showAsAlert: true,
      autoClose: true,
      closable: false,
      dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
        dispatch(terminateSessionAlertLocalSetting.set({ show: false }));
        return Promise.resolve();
      },
    };
  },
);

type TerminateQueryAlertPayload = {
  show: boolean;
  status?: "SUCCESS" | "FAILED";
};

export const terminateQueryAlertLocalSetting = new LocalSetting<AdminUIState, TerminateQueryAlertPayload>(
  "terminate_query_alert", localSettingsSelector, { show: false },
);

export const terminateQueryAlertSelector = createSelector(
  terminateQueryAlertLocalSetting.selector,
  (terminateQueryAlert): Alert => {
    if (!terminateQueryAlert || !terminateQueryAlert.show) {
      return undefined;
    }
    const { status } = terminateQueryAlert;

    if (status === "FAILED") {
      return {
        level: AlertLevel.CRITICAL,
        title: "There was an error terminating the query.",
        text: "Please try terminating again. If the problem continues please reach out to customer support.",
        showAsAlert: true,
        dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
          dispatch(terminateQueryAlertLocalSetting.set({ show: false }));
          return Promise.resolve();
        },
      };
    }
    return {
      level: AlertLevel.SUCCESS,
      title: "Query terminated.",
      showAsAlert: true,
      autoClose: true,
      closable: false,
      dismiss: (dispatch: Dispatch<Action, AdminUIState>) => {
        dispatch(terminateQueryAlertLocalSetting.set({ show: false }));
        return Promise.resolve();
      },
    };
  },
);

/**
 * Selector which returns an array of all active alerts which should be
 * displayed in the alerts panel, which is embedded within the cluster overview
 * page; currently, this includes all non-critical alerts.
 */
export const panelAlertsSelector = createSelector(
  newVersionNotificationSelector,
  staggeredVersionWarningSelector,
  (...alerts: Alert[]): Alert[] => {
    return _.without(alerts, null, undefined);
  },
);

/**
 * Selector which returns an array of all active alerts which should be
 * displayed as a banner, which appears at the top of the page and overlaps
 * content in recognition of the severity of the alert; currently, this includes
 * all critical-level alerts.
 */
export const bannerAlertsSelector = createSelector(
  disconnectedAlertSelector,
  emailSubscriptionAlertSelector,
  createStatementDiagnosticsAlertSelector,
  terminateSessionAlertSelector,
  terminateQueryAlertSelector,
  (...alerts: Alert[]): Alert[] => {
    return _.without(alerts, null, undefined);
  },
);

/**
 * This function, when supplied with a redux store, generates a callback that
 * attempts to populate missing information that has not yet been loaded from
 * the cluster that is needed to show certain alerts. This returned function is
 * intended to be attached to the store as a subscriber.
 */
export function alertDataSync(store: Store<AdminUIState>) {
  const dispatch = store.dispatch;

  // Memoizers to prevent unnecessary dispatches of alertDataSync if store
  // hasn't changed in an interesting way.
  let lastUIData: UIDataState;

  return () => {
    const state: AdminUIState = store.getState();

    // Always refresh health.
    dispatch(refreshHealth());

    // Load persistent settings which have not yet been loaded.
    const uiData = state.uiData;
    if (uiData !== lastUIData) {
      lastUIData = uiData;
      const keysToMaybeLoad = [VERSION_DISMISSED_KEY, INSTRUCTIONS_BOX_COLLAPSED_KEY];
      const keysToLoad = _.filter(keysToMaybeLoad, (key) => {
        return !(_.has(uiData, key) || isInFlight(state, key));
      });
      if (keysToLoad) {
        dispatch(loadUIData(...keysToLoad));
      }
    }

    // Load Cluster ID once at startup.
    const cluster = state.cachedData.cluster;
    if (cluster && !cluster.data && !cluster.inFlight) {
      dispatch(refreshCluster());
    }

    // Load Nodes initially if it has not yet been loaded.
    const nodes = state.cachedData.nodes;
    if (nodes && !nodes.data && !nodes.inFlight) {
      dispatch(refreshNodes());
    }

    // Load potential new versions from CockroachDB cluster. This is the
    // complicating factor of this function, since the call requires the cluster
    // ID and node statuses being loaded first and thus cannot simply run at
    // startup.
    const currentVersion = singleVersionSelector(state);
    if (_.isNil(newerVersionsSelector(state))) {
      if (cluster.data && cluster.data.cluster_id && currentVersion) {
        dispatch(refreshVersion({
          clusterID: cluster.data.cluster_id,
          buildtag: currentVersion,
        }));
      }
    }
  };
}
