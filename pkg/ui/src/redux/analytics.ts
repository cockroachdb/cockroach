import Analytics from "analytics-node";
import { Location } from "history";
import _ from "lodash";
import { Store } from "redux";

import { store, history, AdminUIState } from "src/redux/state";

/**
 * List of current redactions needed for pages tracked by the Admin UI.
 * TODO(mrtracy): It this list becomes more extensive, it might benefit from a
 * set of tests as a double-check.
 */
const defaultRedactions = [
    // When viewing a specific database, the database name and table are part of
    // the URL path.
    {
        match: new RegExp("/databases/database/.*/table/.*"),
        replace: "/databases/database/[db]/table/[tbl]",
    },
];

/**
 * A PageTrackRedaction describes a regular expression used to identify PII
 * in strings that are being sent to analytics. If a string matches the given
 * "match" RegExp, it will be replaced with the "replace" string before being
 * sent to analytics.
 */
interface PageTrackRedaction {
    match: RegExp;
    replace: string;
}

/**
 * AnalyticsSync is used to dispatch analytics event from the Admin UI to an
 * analytics service (currently Segment). It combines information on individual
 * events with user information from the redux state in order to properly
 * identify events.
 */
export class AnalyticsSync {
    /**
     * queuedPages are used to store pages visited before the cluster ID
     * is available. Once the cluster ID is available, the next call to page()
     * will dispatch all queued locations to the underlying analytics API.
     */
     private queuedPages: Location[] = [];

     /**
      * AnalyticsSync can be called from different places in the code without
      * needing to check if the individual cluster has enabled analytics.
      * Instead,
      */
     private enableAnalytics = false;

     /**
      * Construct a new AnalyticsSync object.
      * @param analytics Underlying interface to push to the analytics service.
      * @param store The redux store for the Admin UI.
      * @param redactions A list of redaction regular expressions, used to
      * scrub any potential personally-identifying information from the data
      * being tracked.
      */
    constructor(
        private analytics: Analytics,
        private store: Store<AdminUIState>,
        private redactions: PageTrackRedaction[],
    ) {}

    /**
     * setEnabled can be used to enable analytics pushing. By default, a newly
     * initialized AnalyticsSync is not enabled.
     */
    setEnabled(enabled: boolean) {
        this.enableAnalytics = enabled;
    }

    /**
     * page should be called whenever the user moves to a new page in the
     * application.
     * @param location The location (URL information) of the page.
     */
    page(location: Location) {
        if (!this.enableAnalytics) {
            return;
        }

        // If the cluster ID is not yet available, queue the location to be
        // pushed later.
        const id = this.getCluster();
        if (id === null) {
            this.queuedPages.push(location);
            return;
        }

        // If there are any queued pages, push them.
        _.each(this.queuedPages, (l) => this.pushPage(id, l));
        this.queuedPages = [];

        // Push the page that was just accessed.
        this.pushPage(id, location);
    }

    /**
     * Return the ClusterID from the store, returning null if the clusterID
     * has not yet been fetched. We can depend on the alertdatasync component
     * to eventually retrieve this without having to request it ourselves.
     */
    private getCluster(): string | null {
        const state = this.store.getState();

        // Do nothing if cluster ID has not been loaded.
        const cluster = state.cachedData.cluster;
        if (!cluster || !cluster.data) {
            return null;
        }

        return cluster.data.cluster_id;
    }

    /**
     * pushPage pushes a single "page" event to the analytics service.
     */
    private pushPage = (userID: string, location: Location) => {
        // Loop through redactions, if any matches return the appropriate
        // redacted string.
        let path = location.pathname;
        _.each(this.redactions, (r) => {
            if (r.match.test(location.pathname)) {
                path = r.replace;
                return false;
            }
        });

        this.analytics.page({
            userId: userID,
            name: path,
            properties: {
                path,
            },
        });
    }
}

// Create a global instance of AnalyticsSync which can be used from various
// packages. If enabled, this instance will push to segment using the following
// analytics key.
const analyticsInstance = new Analytics("5Vbp8WMYDmZTfCwE0uiUqEdAcTiZWFDb", { flushAt: 1 });
export const analytics = new AnalyticsSync(analyticsInstance, store, defaultRedactions);

// Temporary placeholder; this logic should be based on the server-side setting
// "diagnostics.reporting.enabled". A mechanism to pass this value to the UI
// must be added before enabling tracking. For now, it is disabled.
analytics.setEnabled(false);

// Attach a listener to the history object which will track a 'page' event
// whenever the user navigates to a new path.
let lastPageLocation: Location;
history.listen((location) => {
  // Do not log if the pathname is the same as the previous.
  // Needed because history.listen() fires twice when using hash history, this
  // bug is "won't fix" in the version of history we are using, and upgrading
  // would imply a difficult upgrade to react-router v4.
  // (https://github.com/ReactTraining/history/issues/427).
  if (lastPageLocation && lastPageLocation.pathname === location.pathname) {
    return;
  }
  lastPageLocation = location;
  analytics.page(location);
});

// Record the initial page that was accessed; listen won't fire for the first
// page loaded.
analytics.page(history.getCurrentLocation());
