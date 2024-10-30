// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * Alerts is a collection of selectors which determine if there are any Alerts
 * to display based on the current redux state.
 */

import filter from "lodash/filter";
import has from "lodash/has";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";
import without from "lodash/without";
import moment from "moment-timezone";
import { Store, Dispatch, Action, AnyAction } from "redux";
import { ThunkAction } from "redux-thunk";
import { createSelector } from "reselect";

import {
  singleVersionSelector,
  numNodesByVersionsTagSelector,
  numNodesByVersionsSelector,
} from "src/redux/nodes";
import * as docsURL from "src/util/docs";
import { longToInt } from "src/util/fixLong";

import { getDataFromServer } from "../util/dataFromServer";

import {
  refreshCluster,
  refreshNodes,
  refreshVersion,
  refreshHealth,
  refreshSettings,
} from "./apiReducers";
import {
  selectClusterSettings,
  selectClusterSettingVersion,
} from "./clusterSettings";
import { LocalSetting } from "./localsettings";
import { AdminUIState, AppDispatch } from "./state";
import {
  VERSION_DISMISSED_KEY,
  INSTRUCTIONS_BOX_COLLAPSED_KEY,
  saveUIData,
  loadUIData,
  isInFlight,
  UIDataState,
  UIDataStatus,
} from "./uiData";

export enum AlertLevel {
  NOTIFICATION,
  WARNING,
  CRITICAL,
  SUCCESS,
  INFORMATION,
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
  dismiss: ThunkAction<Promise<void>, AdminUIState, void, AnyAction>;
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
  INSTRUCTIONS_BOX_COLLAPSED_KEY,
  localSettingsSelector,
  false,
);

const instructionsBoxCollapsedPersistentLoadedSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  (uiData): boolean =>
    uiData &&
    has(uiData, INSTRUCTIONS_BOX_COLLAPSED_KEY) &&
    uiData[INSTRUCTIONS_BOX_COLLAPSED_KEY].status === UIDataStatus.VALID,
);

const instructionsBoxCollapsedPersistentSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  (uiData): boolean =>
    uiData &&
    has(uiData, INSTRUCTIONS_BOX_COLLAPSED_KEY) &&
    uiData[INSTRUCTIONS_BOX_COLLAPSED_KEY].status === UIDataStatus.VALID &&
    uiData[INSTRUCTIONS_BOX_COLLAPSED_KEY].data,
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
  return (dispatch: AppDispatch) => {
    dispatch(instructionsBoxCollapsedSetting.set(collapsed));
    dispatch(
      saveUIData({
        key: INSTRUCTIONS_BOX_COLLAPSED_KEY,
        value: collapsed,
      }),
    );
  };
}

////////////////////////////////////////
// Version mismatch.
////////////////////////////////////////
export const staggeredVersionDismissedSetting = new LocalSetting(
  "staggered_version_dismissed",
  localSettingsSelector,
  false,
);

/**
 * Warning when multiple versions of CockroachDB are detected on the cluster.
 * This excludes decommissioned nodes.
 */
export const staggeredVersionWarningSelector = createSelector(
  numNodesByVersionsTagSelector,
  staggeredVersionDismissedSetting.selector,
  (versionsMap, versionMismatchDismissed): Alert => {
    if (versionMismatchDismissed) {
      return undefined;
    }
    if (!versionsMap || versionsMap.size < 2) {
      return undefined;
    }
    const versionsText = Array.from(versionsMap)
      .map(([k, v]) => (v === 1 ? `1 node on ${k}` : `${v} nodes on ${k}`))
      .join(", ")
      .concat(". ");
    return {
      level: AlertLevel.WARNING,
      title: "Multiple versions of CockroachDB are running on this cluster.",
      text:
        "Listed versions: " +
        versionsText +
        `You can see a list of all nodes and their versions below.
        This may be part of a normal rolling upgrade process, but should be investigated
        if unexpected.`,
      dismiss: (dispatch: AppDispatch) => {
        dispatch(staggeredVersionDismissedSetting.set(true));
        return Promise.resolve();
      },
    };
  },
);

// A boolean that indicates whether the server has yet been checked for a
// persistent dismissal of this notification.
// TODO(mrtracy): Refactor so that we can distinguish "never loaded" from
// "loaded, doesn't exist on server" without a separate selector.
const newVersionDismissedPersistentLoadedSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  uiData => uiData && has(uiData, VERSION_DISMISSED_KEY),
);

const newVersionDismissedPersistentSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  uiData => {
    return (
      (uiData &&
        uiData[VERSION_DISMISSED_KEY] &&
        uiData[VERSION_DISMISSED_KEY].data &&
        moment(uiData[VERSION_DISMISSED_KEY].data)) ||
      moment(0)
    );
  },
);

export const newVersionDismissedLocalSetting = new LocalSetting(
  "new_version_dismissed",
  localSettingsSelector,
  moment(0),
);

export const newerVersionsSelector = (state: AdminUIState) =>
  state.cachedData.version.valid ? state.cachedData.version.data : null;

/**
 * Notification when a new version of CockroachDB is available.
 */
export const newVersionNotificationSelector = createSelector(
  newerVersionsSelector,
  newVersionDismissedPersistentLoadedSelector,
  newVersionDismissedPersistentSelector,
  newVersionDismissedLocalSetting.selector,
  (
    newerVersions,
    newVersionDismissedPersistentLoaded,
    newVersionDismissedPersistent,
    newVersionDismissedLocal,
  ): Alert => {
    // Check if there are new versions available.
    if (
      !newerVersions ||
      !newerVersions.details ||
      newerVersions.details.length === 0
    ) {
      return undefined;
    }

    // Check local dismissal. Local dismissal is valid for one day.
    const yesterday = moment().subtract(1, "day");
    if (
      newVersionDismissedLocal.isAfter &&
      newVersionDismissedLocal.isAfter(yesterday)
    ) {
      return undefined;
    }

    // Check persistent dismissal, also valid for one day.
    if (
      !newVersionDismissedPersistentLoaded ||
      !newVersionDismissedPersistent ||
      newVersionDismissedPersistent.isAfter(yesterday)
    ) {
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
        return dispatch(
          saveUIData({
            key: VERSION_DISMISSED_KEY,
            value: dismissedAt.valueOf(),
          }),
        );
      },
    };
  },
);

export const disconnectedDismissedLocalSetting = new LocalSetting(
  "disconnected_dismissed",
  localSettingsSelector,
  moment(0),
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
      title:
        "We're currently having some trouble fetching updated data. If this persists, it might be a good idea to check your network connection to the CockroachDB cluster.",
      dismiss: (dispatch: Dispatch<Action>) => {
        dispatch(disconnectedDismissedLocalSetting.set(moment()));
        return Promise.resolve();
      },
    };
  },
);

export const emailSubscriptionAlertLocalSetting = new LocalSetting(
  "email_subscription_alert",
  localSettingsSelector,
  false,
);

export const emailSubscriptionAlertSelector = createSelector(
  emailSubscriptionAlertLocalSetting.selector,
  (emailSubscriptionAlert): Alert => {
    if (!emailSubscriptionAlert) {
      return undefined;
    }
    return {
      level: AlertLevel.SUCCESS,
      title: "You successfully signed up for CockroachDB release notes",
      showAsAlert: true,
      autoClose: true,
      closable: false,
      dismiss: (dispatch: Dispatch<Action>) => {
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

export const createStatementDiagnosticsAlertLocalSetting = new LocalSetting<
  AdminUIState,
  CreateStatementDiagnosticsAlertPayload
>("create_stmnt_diagnostics_alert", localSettingsSelector, { show: false });

export const createStatementDiagnosticsAlertSelector = createSelector(
  createStatementDiagnosticsAlertLocalSetting.selector,
  (createStatementDiagnosticsAlert): Alert => {
    if (
      !createStatementDiagnosticsAlert ||
      !createStatementDiagnosticsAlert.show
    ) {
      return undefined;
    }
    const { status } = createStatementDiagnosticsAlert;

    if (status === "FAILED") {
      return {
        level: AlertLevel.CRITICAL,
        title: "There was an error activating statement diagnostics",
        text: "Please try activating again. If the problem continues please reach out to customer support.",
        showAsAlert: true,
        dismiss: (dispatch: Dispatch<Action>) => {
          dispatch(
            createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
          );
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
      dismiss: (dispatch: Dispatch<Action>) => {
        dispatch(
          createStatementDiagnosticsAlertLocalSetting.set({ show: false }),
        );
        return Promise.resolve();
      },
    };
  },
);

type CancelStatementDiagnosticsAlertPayload = {
  show: boolean;
  status?: "SUCCESS" | "FAILED";
};

export const cancelStatementDiagnosticsAlertLocalSetting = new LocalSetting<
  AdminUIState,
  CancelStatementDiagnosticsAlertPayload
>("cancel_stmnt_diagnostics_alert", localSettingsSelector, { show: false });

export const cancelStatementDiagnosticsAlertSelector = createSelector(
  cancelStatementDiagnosticsAlertLocalSetting.selector,
  (cancelStatementDiagnosticsAlert): Alert => {
    if (
      !cancelStatementDiagnosticsAlert ||
      !cancelStatementDiagnosticsAlert.show
    ) {
      return undefined;
    }
    const { status } = cancelStatementDiagnosticsAlert;

    if (status === "FAILED") {
      return {
        level: AlertLevel.CRITICAL,
        title: "There was an error cancelling statement diagnostics",
        text: "Please try cancelling the statement diagnostic again. If the problem continues please reach out to customer support.",
        showAsAlert: true,
        dismiss: (dispatch: Dispatch<Action>) => {
          dispatch(
            cancelStatementDiagnosticsAlertLocalSetting.set({ show: false }),
          );
          return Promise.resolve();
        },
      };
    }
    return {
      level: AlertLevel.SUCCESS,
      title: "Statement diagnostics were successfully cancelled",
      showAsAlert: true,
      autoClose: true,
      closable: false,
      dismiss: (dispatch: Dispatch<Action>) => {
        dispatch(
          cancelStatementDiagnosticsAlertLocalSetting.set({ show: false }),
        );
        return Promise.resolve();
      },
    };
  },
);

type TerminateSessionAlertPayload = {
  show: boolean;
  status?: "SUCCESS" | "FAILED";
};

export const terminateSessionAlertLocalSetting = new LocalSetting<
  AdminUIState,
  TerminateSessionAlertPayload
>("terminate_session_alert", localSettingsSelector, { show: false });

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
        title: "There was an error cancelling the session.",
        text: "Please try cancelling again. If the problem continues please reach out to customer support.",
        showAsAlert: true,
        dismiss: (dispatch: Dispatch<Action>) => {
          dispatch(terminateSessionAlertLocalSetting.set({ show: false }));
          return Promise.resolve();
        },
      };
    }
    return {
      level: AlertLevel.SUCCESS,
      title: "Session cancelled.",
      showAsAlert: true,
      autoClose: true,
      closable: false,
      dismiss: (dispatch: Dispatch<Action>) => {
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

export const terminateQueryAlertLocalSetting = new LocalSetting<
  AdminUIState,
  TerminateQueryAlertPayload
>("terminate_query_alert", localSettingsSelector, { show: false });

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
        title: "There was an error cancelling the statement.",
        text: "Please try cancelling again. If the problem continues please reach out to customer support.",
        showAsAlert: true,
        dismiss: (dispatch: Dispatch<Action>) => {
          dispatch(terminateQueryAlertLocalSetting.set({ show: false }));
          return Promise.resolve();
        },
      };
    }
    return {
      level: AlertLevel.SUCCESS,
      title: "Statement cancelled.",
      showAsAlert: true,
      autoClose: true,
      closable: false,
      dismiss: (dispatch: Dispatch<Action>) => {
        dispatch(terminateQueryAlertLocalSetting.set({ show: false }));
        return Promise.resolve();
      },
    };
  },
);

/**
 * Notification for when the cluster.preserve_downgrade_option has been set for
 * too long of a duration (48hrs) as part of a version upgrade.
 */
export const clusterPreserveDowngradeOptionDismissedSetting = new LocalSetting(
  "cluster_preserve_downgrade_option_dismissed",
  localSettingsSelector,
  false,
);

export const clusterPreserveDowngradeOptionOvertimeSelector = createSelector(
  selectClusterSettings,
  clusterPreserveDowngradeOptionDismissedSetting.selector,
  (settings, notificationDismissed): Alert => {
    if (notificationDismissed || !settings) {
      return undefined;
    }
    const clusterPreserveDowngradeOption =
      settings["cluster.preserve_downgrade_option"];
    const value = clusterPreserveDowngradeOption?.value;
    const lastUpdated = clusterPreserveDowngradeOption?.last_updated;
    if (!value || !lastUpdated) {
      return undefined;
    }
    const lastUpdatedTime = moment.unix(longToInt(lastUpdated.seconds));
    const diff = moment.duration(moment().diff(lastUpdatedTime)).asHours();
    if (diff <= 0) {
      return undefined;
    }
    return {
      level: AlertLevel.WARNING,
      title: `Cluster setting cluster.preserve_downgrade_option has been set for ${diff.toFixed(
        1,
      )} hours`,
      text: `You can see a list of all nodes and their versions below.
        Once all cluster nodes have been upgraded, and you have validated the stability and performance of
        your workload on the new version, you must reset the cluster.preserve_downgrade_option cluster
        setting with the following command:
        RESET CLUSTER SETTING cluster.preserve_downgrade_option;`,
      dismiss: (dispatch: AppDispatch) => {
        dispatch(clusterPreserveDowngradeOptionDismissedSetting.set(true));
        return Promise.resolve();
      },
    };
  },
);

////////////////////////////////////////
// Upgrade not finalized.
////////////////////////////////////////
export const upgradeNotFinalizedDismissedSetting = new LocalSetting(
  "upgrade_not_finalized_dismissed",
  localSettingsSelector,
  false,
);

/**
 * Warning when all the nodes are running on the new version, but the cluster is not finalized.
 */
export const upgradeNotFinalizedWarningSelector = createSelector(
  selectClusterSettings,
  numNodesByVersionsSelector,
  selectClusterSettingVersion,
  upgradeNotFinalizedDismissedSetting.selector,
  (
    settings,
    versionsMap,
    clusterVersion,
    upgradeNotFinalizedDismissed,
  ): Alert => {
    if (upgradeNotFinalizedDismissed || !settings) {
      return undefined;
    }
    // Don't show this warning if nodes are on different versions, since there is
    // already an alert for that (staggeredVersionWarningSelector).
    if (!versionsMap || versionsMap.size !== 1 || !clusterVersion) {
      return undefined;
    }
    // Don't show this warning if cluster.preserve_downgrade_option is set,
    // because it's expected for the upgrade not be finalized on that case and there is
    // an alert for that (clusterPreserveDowngradeOptionOvertimeSelector)
    const clusterPreserveDowngradeOption =
      settings["cluster.preserve_downgrade_option"];
    const value = clusterPreserveDowngradeOption?.value;
    const lastUpdated = clusterPreserveDowngradeOption?.last_updated;
    if (value && lastUpdated) {
      return undefined;
    }

    const nodesVersion = versionsMap.keys().next().value;
    // Prod: node version is 23.1 and cluster version is 23.1.
    // Dev: node version is 23.1 and cluster version is 23.1-2.
    if (clusterVersion.startsWith(nodesVersion)) {
      return undefined;
    }

    return {
      level: AlertLevel.WARNING,
      title: "Upgrade not finalized.",
      text: `All nodes are running on version ${nodesVersion}, but the cluster is on version ${clusterVersion}. 
      Features might not be available in this state.`,
      link: docsURL.upgradeTroubleshooting,
      dismiss: (dispatch: AppDispatch) => {
        dispatch(upgradeNotFinalizedDismissedSetting.set(true));
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
    return without(alerts, null, undefined);
  },
);

/**
 * dataFromServerAlertSelector returns an alert when the
 * `dataFromServer` window variable is unset. This variable is retrieved
 * prior to page render and contains some base metadata about the
 * backing CRDB node.
 */
export const dataFromServerAlertSelector = createSelector(
  () => {},
  (): Alert => {
    if (isEmpty(getDataFromServer())) {
      return {
        level: AlertLevel.CRITICAL,
        title: "There was an error retrieving base DB Console configuration.",
        text: "Please try refreshing the page. If the problem continues please reach out to customer support.",
        showAsAlert: true,
        dismiss: (_: Dispatch<Action>) => {
          // Dismiss is a no-op here because the alert component itself
          // is dismissable by default and because we do want to keep
          // showing it if the metadata continues to be missing.
          return Promise.resolve();
        },
      };
    } else {
      return null;
    }
  },
);

export type LicenseType =
  | "Evaluation"
  | "Trial"
  | "Enterprise"
  | "Non-Commercial"
  | "None"
  | "Free";

const licenseTypeNames = new Map<string, LicenseType>([
  ["Evaluation", "Evaluation"],
  ["Enterprise", "Enterprise"],
  ["NonCommercial", "Non-Commercial"],
  ["OSS", "None"],
  ["BSD", "None"],
  ["Free", "Free"],
  ["Trial", "Trial"],
]);

// licenseTypeSelector returns user-friendly names of license types.
export const licenseTypeSelector = createSelector(
  getDataFromServer,
  data => licenseTypeNames.get(data.LicenseType) || "None",
);

export const licenseUpdateDismissedLocalSetting = new LocalSetting(
  "license_update_dismissed",
  localSettingsSelector,
  moment(0),
);

/**
 * Selector which returns an array of all active alerts which should be
 * displayed in the overview list page, these should be non-critical alerts.
 */

export const overviewListAlertsSelector = createSelector(
  staggeredVersionWarningSelector,
  clusterPreserveDowngradeOptionOvertimeSelector,
  upgradeNotFinalizedWarningSelector,
  (...alerts: Alert[]): Alert[] => {
    return without(alerts, null, undefined);
  },
);

// daysUntilLicenseExpiresSelector returns number of days remaining before license expires.
export const daysUntilLicenseExpiresSelector = createSelector(
  getDataFromServer,
  data => {
    return Math.ceil(data.SecondsUntilLicenseExpiry / 86400); // seconds in 1 day
  },
);

export const isManagedClusterSelector = createSelector(
  getDataFromServer,
  data => data.IsManaged,
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
  cancelStatementDiagnosticsAlertSelector,
  terminateSessionAlertSelector,
  terminateQueryAlertSelector,
  dataFromServerAlertSelector,
  (...alerts: Alert[]): Alert[] => {
    return without(alerts, null, undefined);
  },
);

/**
 * This function, when supplied with a redux store, generates a callback that
 * attempts to populate missing information that has not yet been loaded from
 * the cluster that is needed to show certain alerts. This returned function is
 * intended to be attached to the store as a subscriber.
 */
export function alertDataSync(store: Store<AdminUIState>) {
  const dispatch = store.dispatch as AppDispatch;

  // Memoizers to prevent unnecessary dispatches of alertDataSync if store
  // hasn't changed in an interesting way.
  let lastUIData: UIDataState;

  return () => {
    const state: AdminUIState = store.getState();

    // Always refresh health.
    dispatch(refreshHealth());

    const { Insecure } = getDataFromServer();
    // We should not send out requests to the endpoints below if
    // the user has not successfully logged in since the requests
    // will always return with a 401 error.
    // Insecure mode is an exception, where login state is irrelevant.
    if (!Insecure) {
      if (
        !state.login ||
        !state.login.loggedInUser ||
        state.login.loggedInUser === ``
      ) {
        return;
      }
    }

    // Load persistent settings which have not yet been loaded.
    const uiData = state.uiData;
    if (uiData !== lastUIData) {
      lastUIData = uiData;
      const keysToMaybeLoad = [
        VERSION_DISMISSED_KEY,
        INSTRUCTIONS_BOX_COLLAPSED_KEY,
      ];
      const keysToLoad = filter(keysToMaybeLoad, key => {
        return !(has(uiData, key) || isInFlight(state, key));
      });
      if (keysToLoad) {
        dispatch(loadUIData(...keysToLoad));
      }
    }

    // Load Cluster ID once at startup.
    const cluster = state.cachedData.cluster;
    if (cluster && !cluster.data && !cluster.inFlight && !cluster.valid) {
      dispatch(refreshCluster());
    }

    // Load Nodes initially if it has not yet been loaded.
    const nodes = state.cachedData.nodes;
    if (nodes && !nodes.data && !nodes.inFlight && !nodes.valid) {
      dispatch(refreshNodes());
    }

    // Load settings if not loaded
    const settings = state.cachedData.settings;
    if (settings && !settings.data && !settings.inFlight && !settings.valid) {
      dispatch(refreshSettings());
    }

    // Load potential new versions from CockroachDB cluster. This is the
    // complicating factor of this function, since the call requires the cluster
    // ID and node statuses being loaded first and thus cannot simply run at
    // startup.
    const currentVersion = singleVersionSelector(state);
    if (isNil(newerVersionsSelector(state))) {
      if (cluster.data && cluster.data.cluster_id && currentVersion) {
        dispatch(
          refreshVersion({
            clusterID: cluster.data.cluster_id,
            buildtag: currentVersion,
          }),
        );
      }
    }
  };
}
