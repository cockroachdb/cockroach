/**
 * Alerts is a collection of selectors which determine if there are any Alerts
 * to display based on the current redux state.
 */

import _ from "lodash";
import moment from "moment";
import { createSelector } from "reselect";
import { Store } from "redux";
import { ThunkAction } from "redux-thunk";

import { LocalSetting } from "./localsettings";
import {
  OptInAttributes, saveUIData, KEY_HELPUS, VERSION_DISMISSED_KEY, loadUIData, isInFlight,
  UIDataSet,
} from "./uiData";
import { refreshCluster, refreshNodes, refreshVersion, refreshHealth } from "./apiReducers";
import { nodeStatusesSelector } from "./nodes";
import { AdminUIState } from "./state";

export enum AlertLevel {
  NOTIFICATION,
  WARNING,
  CRITICAL,
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
}

const localSettingsSelector = (state: AdminUIState) => state.localSettings;

export const helpusBannerDismissedSetting = new LocalSetting(
  "helpus_alert_dismissed", localSettingsSelector, false,
);

// optinAttributes are the saved attributes that indicate whether the user has
// opted in to usage reporting.
export const optinAttributesSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  (uiData) => uiData && uiData[KEY_HELPUS] && uiData[KEY_HELPUS].data as OptInAttributes,
);

// optinAttributesLoaded is a boolean that indicates whether the optinAttributes
// have been loaded yet.
// TODO(mrtracy): Refactor so that we can distinguish "never loaded" from
// "loaded, doesn't exist on server" without a separate selector
export const optinAttributesLoadedSelector = createSelector(
  (state: AdminUIState) => state.uiData,
  (uiData) => uiData && _.has(uiData, KEY_HELPUS),
);

/**
 * Notification if the user has not yet explicitly opted in or out of usage
 * reporting.
 */
export const helpusNotificationSelector = createSelector(
  optinAttributesSelector,
  optinAttributesLoadedSelector,
  helpusBannerDismissedSetting.selector,
  (optinAttributes, attributesLoaded, helpusBannerDismissed): Alert => {
    if (helpusBannerDismissed) {
      return undefined;
    }
    // If we haven't yet checked the server for current opt-in settings, do not
    // yet display the notification.
    if (!attributesLoaded) {
      return undefined;
    }
    // If server-side opt-in settings indicate the user has already explicitly
    // set an opt-in preference, do not display the notification.
    if (optinAttributes && _.isBoolean(optinAttributes.optin)) {
      return undefined;
    }

    return {
      level: AlertLevel.NOTIFICATION,
      title: "Help Us!",
      link: "#/help-us/reporting",
      text: "Help Cockroach DB improve: opt in to share usage statistics",
      dismiss: (dispatch) => {
        // Dismiss locally.
        dispatch(helpusBannerDismissedSetting.set(true));
        // Dismiss with persistence on server.
        const newAttributes = optinAttributes ? _.clone(optinAttributes) : new OptInAttributes();
        // "dismissed" counts the number of times this banner has been
        // dismissed. It is not currently being used anywhere to my knowledge,
        // it might be wholly replaceable by "dismissedAt".
        newAttributes.dismissed = 1;
        return dispatch(saveUIData({
          key: KEY_HELPUS,
          value: newAttributes,
        }));
      },
    };
  });

////////////////////////////////////////
// Version mismatch.
////////////////////////////////////////
export const staggeredVersionDismissedSetting = new LocalSetting(
  "staggered_version_dismissed", localSettingsSelector, false,
);

export const versionsSelector = createSelector(
  nodeStatusesSelector,
  (nodeStatuses) => nodeStatuses && _.uniq(_.map(nodeStatuses, (status) => status.build_info && status.build_info.tag)),
);

/**
 * Warning when multiple versions of CockroachDB are detected on the cluster.
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
      dismiss: (dispatch) => {
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
    if (newVersionDismissedLocal.isAfter(yesterday)) {
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
      link: "https://www.cockroachlabs.com/docs/install-cockroachdb.html",
      dismiss: (dispatch) => {
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
      title: "Connection to CockroachDB node lost.",
      dismiss: (dispatch) => {
        dispatch(disconnectedDismissedLocalSetting.set(moment()));
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
  helpusNotificationSelector,
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
  (...alerts: Alert[]): Alert[] => {
    return _.without(alerts, null, undefined);
  },
);

// Select the current build version of the cluster, returning undefined if the
// cluster's version is currently staggered.
const singleVersionSelector = createSelector(
  versionsSelector,
  (builds) => {
    if (!builds || builds.length !== 1) {
      return undefined;
    }
    return builds[0];
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
  let lastUIData: UIDataSet;

  return () => {
    const state: AdminUIState = store.getState();

    // Always refresh health.
    dispatch(refreshHealth());

    // Load persistent settings which have not yet been loaded.
    const uiData = state.uiData;
    if (uiData !== lastUIData) {
      lastUIData = uiData;
      const keysToMaybeLoad = [KEY_HELPUS, VERSION_DISMISSED_KEY];
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
