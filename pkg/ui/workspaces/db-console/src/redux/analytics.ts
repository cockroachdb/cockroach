// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import Analytics from "analytics-node";
import { Location } from "history";
import _ from "lodash";
import { Store } from "redux";

import * as protos from "src/js/protos";
import { versionsSelector } from "src/redux/nodes";
import { store, history, AdminUIState } from "src/redux/state";
import { COCKROACHLABS_ADDR } from "src/util/cockroachlabsAPI";

type ClusterResponse = protos.cockroach.server.serverpb.IClusterResponse;

interface TrackMessage {
  event: string;
  properties?: Object;
  timestamp?: Date;
  context?: Object;
}
/**
 * List of current redactions needed for pages tracked by the Admin UI.
 * TODO(mrtracy): It this list becomes more extensive, it might benefit from a
 * set of tests as a double-check.
 */
export const defaultRedactions = [
  // When viewing a specific database, the database name and table are part of
  // the URL path.
  {
    match: new RegExp("/databases/database/.*/table/.*"),
    replace: "/databases/database/[db]/table/[tbl]",
  },
  // The new URL for a database page.
  {
    match: new RegExp("/database/.*/table/.*"),
    replace: "/database/[db]/table/[tbl]",
  },
  // The clusterviz map page, which puts localities in the URL.
  {
    match: new RegExp("/overview/map((/.+)+)"),
    useFunction: true, // I hate TypeScript.
    replace: function countTiers(original: string, localities: string) {
      const tierCount = localities.match(new RegExp("/", "g")).length;
      let redactedLocalities = "";
      for (let i = 0; i < tierCount; i++) {
        redactedLocalities += "/[locality]";
      }
      return original.replace(localities, redactedLocalities);
    },
  },
  // The statement details page, with a full SQL statement in the URL.
  {
    match: new RegExp("/statement/.*"),
    replace: "/statement/[statement]",
  },
];

type PageTrackReplacementFunction = (match: string, ...args: any[]) => string;
type PageTrackReplacement = string | PageTrackReplacementFunction;

/**
 * A PageTrackRedaction describes a regular expression used to identify PII
 * in strings that are being sent to analytics. If a string matches the given
 * "match" RegExp, it will be replaced with the "replace" string before being
 * sent to analytics.
 */
interface PageTrackRedaction {
  match: RegExp;
  replace: PageTrackReplacement;
  useFunction?: boolean; // I hate Typescript.
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
   * sentIdentifyEvent tracks whether the identification event has already
   * been sent for this session. This event is not sent until all necessary
   * information has been retrieved (current version of cockroachDB,
   * cluster settings).
   */
  private identifyEventSent = false;

  /**
   * Construct a new AnalyticsSync object.
   * @param analyticsService Underlying interface to push to the analytics service.
   * @param deprecatedStore The redux store for the Admin UI. [DEPRECATED]
   * @param redactions A list of redaction regular expressions, used to
   * scrub any potential personally-identifying information from the data
   * being tracked.
   */
  constructor(
    private analyticsService: Analytics,
    private deprecatedStore: Store<AdminUIState>,
    private redactions: PageTrackRedaction[],
  ) {}

  /**
   * page should be called whenever the user moves to a new page in the
   * application.
   * @param location The location (URL information) of the page.
   */
  page(location: Location) {
    // If the cluster ID is not yet available, queue the location to be
    // pushed later.
    const cluster = this.getCluster();
    if (cluster === null) {
      this.queuedPages.push(location);
      return;
    }

    const { cluster_id, reporting_enabled } = cluster;

    // A cluster setting determines if diagnostic reporting is enabled. If
    // it is not explicitly enabled, do nothing.
    if (!reporting_enabled) {
      if (this.queuedPages.length > 0) {
        this.queuedPages = [];
      }
      return;
    }

    // If there are any queued pages, push them.
    _.each(this.queuedPages, (l) => this.pushPage(cluster_id, l));
    this.queuedPages = [];

    // Push the page that was just accessed.
    this.pushPage(cluster_id, location);
  }

  /**
   * identify attempts to send an "identify" event to the analytics service.
   * The identify event will only be sent once per session; if it has already
   * been sent, it will be a no-op whenever called afterwards.
   */
  identify() {
    if (this.identifyEventSent) {
      return;
    }

    // Do nothing if Cluster information is not yet available.
    const cluster = this.getCluster();
    if (cluster === null) {
      return;
    }

    const { cluster_id, reporting_enabled, enterprise_enabled } = cluster;
    if (!reporting_enabled) {
      return;
    }

    // Do nothing if version information is not yet available.
    const state = this.deprecatedStore.getState();
    const versions = versionsSelector(state);
    if (_.isEmpty(versions)) {
      return;
    }

    this.analyticsService.identify({
      userId: cluster_id,
      traits: {
        version: versions[0],
        userAgent: window.navigator.userAgent,
        enterprise: enterprise_enabled,
      },
    });
    this.identifyEventSent = true;
  }

  /** Analytics Track for Segment: https://segment.com/docs/connections/spec/track/ */
  track(msg: TrackMessage) {
    const cluster = this.getCluster();
    if (cluster === null) {
      return;
    }

    // get cluster_id to id the event
    const { cluster_id } = cluster;
    const pagePath = this.redact(history.location.pathname);

    // break down properties from message
    const { properties, ...rest } = msg;
    const props = {
      pagePath,
      ...properties,
    };

    const message = {
      userId: cluster_id,
      properties: { ...props },
      ...rest,
    };

    this.analyticsService.track(message);
  }

  /**
   * Return the ClusterID from the store, returning null if the clusterID
   * has not yet been fetched. We can depend on the alertdatasync component
   * to eventually retrieve this without having to request it ourselves.
   */
  private getCluster(): ClusterResponse | null {
    const state = this.deprecatedStore.getState();

    // Do nothing if cluster ID has not been loaded.
    const cluster = state.cachedData.cluster;
    if (!cluster || !cluster.data) {
      return null;
    }

    return cluster.data;
  }

  /**
   * pushPage pushes a single "page" event to the analytics service.
   */
  private pushPage = (userID: string, location: Location) => {
    // Loop through redactions, if any matches return the appropriate
    // redacted string.
    const path = this.redact(location.pathname);
    let search = "";

    if (location.search && location.search.length > 1) {
      const query = location.search.slice(1);
      const params = new URLSearchParams(query);

      params.forEach((value, key) => {
        params.set(key, this.redact(value));
      });
      search = "?" + params.toString();
    }

    this.analyticsService.page({
      userId: userID,
      name: path,
      properties: {
        path,
        search,
      },
    });
  };

  private redact(path: string): string {
    _.each(this.redactions, (r) => {
      if (r.match.test(path)) {
        // Apparently TypeScript doesn't know how to dispatch functions.
        // If there are two function overloads defined (as with
        // String.prototype.replace), it is unable to recognize that
        // a union of the two types can be successfully passed in as a
        // parameter of that function.  We have to explicitly
        // disambiguate the types for it.
        // See https://github.com/Microsoft/TypeScript/issues/14107
        if (r.useFunction) {
          path = path.replace(
            r.match,
            r.replace as PageTrackReplacementFunction,
          );
        } else {
          path = path.replace(r.match, r.replace as string);
        }
        return false;
      }
    });
    return path;
  }
}

// Create a global instance of AnalyticsSync which can be used from various
// packages. If enabled, this instance will push to segment using the following
// analytics key.
const analyticsOpts = {
  host: COCKROACHLABS_ADDR + "/api/segment",
};
const analyticsInstance = new Analytics(
  "5Vbp8WMYDmZTfCwE0uiUqEdAcTiZWFDb",
  analyticsOpts,
);
export const analytics = new AnalyticsSync(
  analyticsInstance,
  store,
  defaultRedactions,
);

// Attach a listener to the history object which will track a 'page' event
// whenever the user navigates to a new path.
let lastPageLocation: Location;
history.listen((location: Location) => {
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
  // Identify the cluster.
  analytics.identify();
});

// Record the initial page that was accessed; listen won't fire for the first
// page loaded.
analytics.page(history.location);
// Identify the cluster.
analytics.identify();
