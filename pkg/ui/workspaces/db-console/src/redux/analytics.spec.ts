// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Analytics } from "@segment/analytics-node";
import { Location, createLocation } from "history";
import each from "lodash/each";

import * as protos from "src/js/protos";

import { AnalyticsSync, defaultRedactions } from "./analytics";
import { history } from "./history";

describe("analytics listener", function () {
  const clusterID = "a49f0ced-7ada-4135-af37-8acf6b548df0";

  function makeClusterData(enabled = true, enterprise = true) {
    return new protos.cockroach.server.serverpb.ClusterResponse({
      cluster_id: clusterID,
      reporting_enabled: enabled,
      enterprise_enabled: enterprise,
    });
  }

  describe("page method", function () {
    let analyticsService: Analytics;
    const pageSpy = jest.fn();

    beforeEach(function () {
      // Analytics is a completely fake object, we don't want to call
      // segment if an unexpected method is called.
      analyticsService = {
        page: pageSpy,
      } as any;
    });

    afterEach(() => {
      pageSpy.mockRestore();
    });

    it("does nothing if cluster info is not available", function () {
      const sync = new AnalyticsSync(analyticsService, []);

      sync.page({
        pathname: "/test/path",
      } as Location);

      expect(pageSpy).not.toHaveBeenCalled();
    });

    it("does nothing if reporting is not explicitly enabled", function () {
      const sync = new AnalyticsSync(analyticsService, []);
      sync.updateCluster(makeClusterData(false));

      sync.page({
        pathname: "/test/path",
      } as Location);

      expect(pageSpy).not.toHaveBeenCalled();
    });

    it("correctly calls segment on a page call", function () {
      const sync = new AnalyticsSync(analyticsService, []);
      sync.updateCluster(makeClusterData());

      sync.page({
        pathname: "/test/path",
      } as Location);

      expect(pageSpy).toHaveBeenCalledTimes(1);
      expect(pageSpy.mock.lastCall[0]).toEqual({
        userId: clusterID,
        name: "/test/path",
        properties: {
          path: "/test/path",
          search: "",
        },
      });
    });

    it("correctly queues calls before cluster ID is available", function () {
      const sync = new AnalyticsSync(analyticsService, []);

      sync.page({
        pathname: "/test/path",
      } as Location);

      sync.updateCluster(makeClusterData());
      expect(pageSpy).not.toHaveBeenCalled();

      sync.page({
        pathname: "/test/path/2",
      } as Location);

      expect(pageSpy.mock.calls.length).toBe(2);
      expect(pageSpy.mock.calls[0][0]).toEqual({
        userId: clusterID,
        name: "/test/path",
        properties: {
          path: "/test/path",
          search: "",
        },
      });
      expect(pageSpy.mock.calls[1][0]).toEqual({
        userId: clusterID,
        name: "/test/path/2",
        properties: {
          path: "/test/path/2",
          search: "",
        },
      });
    });

    it("correctly applies redaction to matched paths", function () {
      const sync = new AnalyticsSync(analyticsService, [
        {
          match: RegExp("/test/.*/path"),
          replace: "/test/[redacted]/path",
        },
      ]);
      sync.updateCluster(makeClusterData());

      sync.page({
        pathname: "/test/username/path",
      } as Location);

      expect(pageSpy).toHaveBeenCalledTimes(1);
      expect(pageSpy.mock.lastCall[0]).toEqual({
        userId: clusterID,
        name: "/test/[redacted]/path",
        properties: {
          path: "/test/[redacted]/path",
          search: "",
        },
      });
    });

    function testRedaction(title: string, input: string, expected: string) {
      return { title, input, expected };
    }

    [
      testRedaction(
        "old database URL (redirect)",
        "/databases/database/foobar/table/baz",
        "/databases/database/[db]/table/[tbl]",
      ),
      testRedaction(
        "database URL (redirect)",
        "/database/foobar",
        "/database/[db]",
      ),
      testRedaction(
        "database tables URL",
        "/database/foobar/tables",
        "/database/[db]/tables",
      ),
      testRedaction(
        "database grants URL",
        "/database/foobar/grants",
        "/database/[db]/grants",
      ),
      testRedaction(
        "database table URL (redirect)",
        "/database/foobar/table",
        "/database/[db]/table",
      ),
      testRedaction(
        "database table URL",
        "/database/foobar/table/baz",
        "/database/[db]/table/[tbl]",
      ),
      testRedaction("clusterviz map root", "/overview/map/", "/overview/map/"),
      testRedaction(
        "clusterviz map single locality",
        "/overview/map/datacenter=us-west-1",
        "/overview/map/[locality]",
      ),
      testRedaction(
        "clusterviz map multiple localities",
        "/overview/map/datacenter=us-west-1/rack=1234",
        "/overview/map/[locality]/[locality]",
      ),
      testRedaction(
        "login redirect URL parameters",
        "/login?redirectTo=%2Fdatabase%2Ffoobar%2Ftable%2Fbaz",
        "/login?redirectTo=%2Fdatabase%2F%5Bdb%5D%2Ftable%2F%5Btbl%5D",
      ),
      testRedaction(
        "statement details page",
        "/statement/SELECT * FROM database.table",
        "/statement/[statement]",
      ),
    ].map(function ({ title, input, expected }) {
      it(`applies a redaction for ${title}`, function () {
        const sync = new AnalyticsSync(analyticsService, defaultRedactions);
        sync.updateCluster(makeClusterData());
        const expectedLocation = createLocation(expected);

        sync.page(createLocation(input));

        expect(pageSpy).toHaveBeenCalledTimes(1);

        const actualArgs = pageSpy.mock.lastCall[0];
        const expectedArgs = {
          userId: clusterID,
          name: expectedLocation.pathname,
          properties: {
            path: expectedLocation.pathname,
            search: expectedLocation.search,
          },
        };
        expect(actualArgs).toEqual(expectedArgs);
      });
    });
  });

  describe("identify method", function () {
    let analyticsService: Analytics;
    const identifySpy = jest.fn();

    beforeEach(function () {
      // Analytics is a completely fake object, we don't want to call
      // segment if an unexpected method is called.
      analyticsService = {
        identify: identifySpy,
      } as any;
    });

    afterEach(() => {
      identifySpy.mockReset();
    });

    it("does nothing if cluster info is not available", function () {
      const sync = new AnalyticsSync(analyticsService, []);
      sync.updateVersions(["0.1"]);

      sync.identify();

      expect(identifySpy).not.toHaveBeenCalled();
    });

    it("does nothing if version info is not available", function () {
      const sync = new AnalyticsSync(analyticsService, []);
      sync.updateCluster(makeClusterData(true, true));

      sync.identify();

      expect(identifySpy).not.toHaveBeenCalled();
    });

    it("does nothing if reporting is not explicitly enabled", function () {
      const sync = new AnalyticsSync(analyticsService, []);
      sync.updateCluster(makeClusterData(false, true));
      sync.updateVersions(["0.1"]);

      sync.identify();

      expect(identifySpy).not.toHaveBeenCalled();
    });

    it("sends the correct value of clusterID, version and enterprise", function () {
      each([false, true], enterpriseSetting => {
        identifySpy.mockReset();
        const sync = new AnalyticsSync(analyticsService, []);
        sync.updateCluster(makeClusterData(true, enterpriseSetting));
        sync.updateVersions(["0.1"]);
        sync.identify();

        expect(identifySpy).toHaveBeenCalledTimes(1);
        expect(identifySpy.mock.lastCall[0]).toEqual({
          userId: clusterID,
          traits: {
            version: "0.1",
            userAgent: window.navigator.userAgent,
            enterprise: enterpriseSetting,
          },
        });
      });
    });

    it("only reports once", function () {
      const sync = new AnalyticsSync(analyticsService, []);
      sync.updateCluster(makeClusterData(true, true));
      sync.updateVersions(["0.1"]);

      sync.identify();
      sync.identify();

      expect(identifySpy).toHaveBeenCalledTimes(1);
    });
  });

  describe("track method", function () {
    let analyticsService: Analytics;
    const trackSpy = jest.fn();

    beforeEach(() => {
      // Analytics is a completely fake object, we don't want to call
      // segment if an unexpected method is called.
      analyticsService = {
        track: trackSpy,
      } as any;
    });

    afterEach(() => {
      trackSpy.mockReset();
    });

    it("does nothing if cluster info is not available", () => {
      const sync = new AnalyticsSync(analyticsService, []);

      sync.track({
        event: "test",
      });

      expect(trackSpy).not.toHaveBeenCalled();
    });

    it("add userId to track calls using the cluster_id", () => {
      const sync = new AnalyticsSync(analyticsService, []);
      sync.updateCluster(makeClusterData());

      sync.track({
        event: "test",
      });

      const expected = {
        userId: clusterID,
        properties: {
          pagePath: "/",
        },
        event: "test",
      };
      const message = trackSpy.mock.lastCall[0];

      expect(trackSpy).toHaveBeenCalledTimes(1);
      expect(message).toEqual(expected);
    });

    it("add the page path to properties", () => {
      const sync = new AnalyticsSync(analyticsService, []);
      sync.updateCluster(makeClusterData());
      const testPagePath = "/test/page/path";

      history.push(testPagePath);

      sync.track({
        event: "test",
        properties: {
          testProp: "test",
        },
      });

      const expected = {
        userId: clusterID,
        properties: {
          pagePath: testPagePath,
          testProp: "test",
        },
        event: "test",
      };
      const message = trackSpy.mock.lastCall[0];

      expect(trackSpy).toHaveBeenCalledTimes(1);
      expect(message).toEqual(expected);
    });
  });
});
