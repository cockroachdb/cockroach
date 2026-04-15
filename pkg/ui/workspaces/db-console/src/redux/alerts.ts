// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * Alerts is a collection of selectors which determine if there are any Alerts
 * to display based on the current redux state.
 */

import { ClusterSetting } from "@cockroachlabs/cluster-ui";
import has from "lodash/has";
import isEmpty from "lodash/isEmpty";
import without from "lodash/without";
import moment from "moment-timezone";
import { Dispatch, Action } from "redux";
import { createSelector } from "reselect";

import { VersionList } from "src/interfaces/cockroachlabs";
import * as docsURL from "src/util/docs";

import { getDataFromServer } from "../util/dataFromServer";

import { LocalSetting } from "./localsettings";
import { AdminUIState } from "./state";
import {
  VERSION_DISMISSED_KEY,
  INSTRUCTIONS_BOX_COLLAPSED_KEY,
  saveUIData,
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
  // Function that will be called when the alert is dismissed.
  dismiss: (dispatch: Dispatch<Action>, getState: () => AdminUIState) => Promise<void>;
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

export function setInstructionsBoxCollapsed(
  dispatch: Dispatch<Action>,
  getState: () => AdminUIState,
  collapsed: boolean,
): void {
  dispatch(instructionsBoxCollapsedSetting.set(collapsed));
  saveUIData(dispatch, getState, {
    key: INSTRUCTIONS_BOX_COLLAPSED_KEY,
    value: collapsed,
  });
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
 * getStaggeredVersionWarning returns a warning alert when multiple versions of
 * CockroachDB are detected on the cluster, or undefined if no warning applies.
 */
export function getStaggeredVersionWarning(
  versionsMap: Map<string, number> | undefined,
  versionMismatchDismissed: boolean,
): Alert | undefined {
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
    dismiss: (dispatch: Dispatch<Action>) => {
      dispatch(staggeredVersionDismissedSetting.set(true));
      return Promise.resolve();
    },
  };
}

export const newVersionDismissedLocalSetting = new LocalSetting(
  "new_version_dismissed",
  localSettingsSelector,
  moment(0),
);

/**
 * getNewVersionNotification returns a notification alert when a newer version
 * of CockroachDB is available, or undefined if no notification applies.
 */
export function getNewVersionNotification(
  newerVersions: VersionList | null,
  newVersionDismissedPersistentLoaded: boolean,
  newVersionDismissedPersistent: moment.Moment,
  newVersionDismissedLocal: moment.Moment,
): Alert | undefined {
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
    dismiss: (dispatch: Dispatch<Action>, getState: () => AdminUIState) => {
      const dismissedAt = moment();
      // Dismiss locally.
      dispatch(newVersionDismissedLocalSetting.set(dismissedAt));
      // Dismiss persistently.
      return saveUIData(dispatch, getState, {
        key: VERSION_DISMISSED_KEY,
        value: dismissedAt.valueOf(),
      });
    },
  };
}

export const disconnectedDismissedLocalSetting = new LocalSetting(
  "disconnected_dismissed",
  localSettingsSelector,
  moment(0),
);

/**
 * Notification when the Admin UI is disconnected from the cluster.
 */
export const disconnectedAlertSelector = createSelector(
  (state: AdminUIState) => state.health,
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

/**
 * Notification for when the cluster.preserve_downgrade_option has been set for
 * too long of a duration (48hrs) as part of a version upgrade.
 */
export const clusterPreserveDowngradeOptionDismissedSetting = new LocalSetting(
  "cluster_preserve_downgrade_option_dismissed",
  localSettingsSelector,
  false,
);

/**
 * getClusterPreserveDowngradeOptionOvertime returns a warning alert when the
 * cluster.preserve_downgrade_option setting has been set for a prolonged
 * duration, or undefined if no warning applies.
 *
 * `settings` is expected to be a Record<string, ClusterSetting> as returned
 * by the useClusterSettings SWR hook.
 */
export function getClusterPreserveDowngradeOptionOvertime(
  settings: Record<string, ClusterSetting> | null | undefined,
  notificationDismissed: boolean,
): Alert | undefined {
  if (notificationDismissed || !settings) {
    return undefined;
  }
  const clusterPreserveDowngradeOption =
    settings["cluster.preserve_downgrade_option"];
  const value = clusterPreserveDowngradeOption?.value;
  const lastUpdated = clusterPreserveDowngradeOption?.lastUpdated;
  if (!value || !lastUpdated) {
    return undefined;
  }
  const diff = moment.duration(moment().diff(lastUpdated)).asHours();
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
    dismiss: (dispatch: Dispatch<Action>) => {
      dispatch(clusterPreserveDowngradeOptionDismissedSetting.set(true));
      return Promise.resolve();
    },
  };
}


////////////////////////////////////////
// Upgrade not finalized.
////////////////////////////////////////
export const upgradeNotFinalizedDismissedSetting = new LocalSetting(
  "upgrade_not_finalized_dismissed",
  localSettingsSelector,
  false,
);

/**
 * getUpgradeNotFinalizedWarning returns a warning alert when all nodes are on
 * the same (new) version but the cluster has not yet finalized the upgrade.
 *
 * `settings` is expected to be a Record<string, ClusterSetting> as returned
 * by the useClusterSettings SWR hook.
 */
export function getUpgradeNotFinalizedWarning(
  settings: Record<string, ClusterSetting> | null | undefined,
  versionsMap: Map<string, number> | undefined,
  clusterVersion: string,
  upgradeNotFinalizedDismissed: boolean,
): Alert | undefined {
  if (upgradeNotFinalizedDismissed || !settings) {
    return undefined;
  }
  // Don't show this warning if nodes are on different versions, since there is
  // already an alert for that (staggeredVersionWarningSelector).
  if (!versionsMap || versionsMap.size !== 1 || !clusterVersion) {
    return undefined;
  }
  // Don't show this warning if cluster.preserve_downgrade_option is set,
  // because it's expected for the upgrade not be finalized on that case and
  // there is an alert for that (clusterPreserveDowngradeOptionOvertimeSelector).
  const clusterPreserveDowngradeOption =
    settings["cluster.preserve_downgrade_option"];
  const value = clusterPreserveDowngradeOption?.value;
  const lastUpdated = clusterPreserveDowngradeOption?.lastUpdated;
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
    dismiss: (dispatch: Dispatch<Action>) => {
      dispatch(upgradeNotFinalizedDismissedSetting.set(true));
      return Promise.resolve();
    },
  };
}


// panelAlertsSelector has been removed. Alerts are now computed directly in
// components using SWR hooks and the helper functions above.

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

// overviewListAlertsSelector has been removed. Alerts are now computed directly
// in components using SWR hooks and the helper functions above.

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
  dataFromServerAlertSelector,
  (...alerts: Alert[]): Alert[] => {
    return without(alerts, null, undefined);
  },
);
