import { assert } from "chai";
import * as sinon from "sinon";

import Analytics from "analytics-node";
import { Location } from "history";
import _ from "lodash";
import { Store } from "redux";

import { AnalyticsSync } from "./analytics";
import { clusterReducerObj, nodesReducerObj } from "./apiReducers";
import { AdminUIState, createAdminUIStore } from "./state";

import * as protos from "src/js/protos";

describe("analytics listener", function() {
  const clusterID = "a49f0ced-7ada-4135-af37-8acf6b548df0";
  describe("page method", function () {
    let store: Store<AdminUIState>;
    let analytics: Analytics;
    let pageSpy: sinon.SinonSpy;

    beforeEach(function () {
      store = createAdminUIStore();
      pageSpy = sinon.spy();

      // Analytics is a completely fake object, we don't want to call
      // segment if an unexpected method is called.
      analytics = {
        page: pageSpy,
      } as any;
    });

    const setClusterData = function (enabled = true) {
      store.dispatch(clusterReducerObj.receiveData(
        new protos.cockroach.server.serverpb.ClusterResponse({
          cluster_id: clusterID,
          reporting_enabled: enabled,
        }),
      ));
    };

    it("does nothing if cluster info is not available", function () {
      const sync = new AnalyticsSync(analytics, store, []);

      sync.page({
        pathname: "/test/path",
      } as Location);

      assert.isTrue(pageSpy.notCalled);
    });

    it("does nothing if reporting is not explicitly enabled", function () {
      const sync = new AnalyticsSync(analytics, store, []);
      setClusterData(false);

      sync.page({
        pathname: "/test/path",
      } as Location);

      assert.isTrue(pageSpy.notCalled);
    });

    it("correctly calls segment on a page call", function () {
      const sync = new AnalyticsSync(analytics, store, []);
      setClusterData();

      sync.page({
        pathname: "/test/path",
      } as Location);

      assert.isTrue(pageSpy.calledOnce);
      assert.deepEqual(pageSpy.args[0][0], {
        userId: clusterID,
        name: "/test/path",
        properties: {
          path: "/test/path",
        },
      });
    });

    it("correctly queues calls before cluster ID is available", function () {
      const sync = new AnalyticsSync(analytics, store, []);

      sync.page({
        pathname: "/test/path",
      } as Location);

      setClusterData();
      assert.isTrue(pageSpy.notCalled);

      sync.page({
        pathname: "/test/path/2",
      } as Location);

      assert.equal(pageSpy.callCount, 2);
      assert.deepEqual(pageSpy.args[0][0], {
        userId: clusterID,
        name: "/test/path",
        properties: {
          path: "/test/path",
        },
      });
      assert.deepEqual(pageSpy.args[1][0], {
        userId: clusterID,
        name: "/test/path/2",
        properties: {
          path: "/test/path/2",
        },
      });
    });

    it("correctly applies redaction to matched paths", function () {
      setClusterData();
      const sync = new AnalyticsSync(analytics, store, [
        {
          match: RegExp("/test/.*/path"),
          replace: "/test/[redacted]/path",
        },
      ]);

      sync.page({
        pathname: "/test/username/path",
      } as Location);

      assert.isTrue(pageSpy.calledOnce);
      assert.deepEqual(pageSpy.args[0][0], {
        userId: clusterID,
        name: "/test/[redacted]/path",
        properties: {
          path: "/test/[redacted]/path",
        },
      });
    });
  });

  describe("identify method", function () {
    let store: Store<AdminUIState>;
    let analytics: Analytics;
    let identifySpy: sinon.SinonSpy;

    beforeEach(function () {
      store = createAdminUIStore();
      identifySpy = sinon.spy();

      // Analytics is a completely fake object, we don't want to call
      // segment if an unexpected method is called.
      analytics = {
        identify: identifySpy,
      } as any;
    });

    const setClusterData = function (enabled = true, enterprise = true) {
      store.dispatch(clusterReducerObj.receiveData(
        new protos.cockroach.server.serverpb.ClusterResponse({
          cluster_id: clusterID,
          reporting_enabled: enabled,
          enterprise_enabled: enterprise,
        }),
      ));
    };

    const setVersionData = function () {
      store.dispatch(nodesReducerObj.receiveData([
        {
          build_info: {
            tag: "0.1",
          },
        },
      ]));
    };

    it("does nothing if cluster info is not available", function () {
      const sync = new AnalyticsSync(analytics, store, []);
      setVersionData();

      sync.identify();

      assert.isTrue(identifySpy.notCalled);
    });

    it("does nothing if version info is not available", function () {
      const sync = new AnalyticsSync(analytics, store, []);
      setClusterData(true, true);

      sync.identify();

      assert.isTrue(identifySpy.notCalled);
    });

    it("does nothing if reporting is not explicitly enabled", function () {
      const sync = new AnalyticsSync(analytics, store, []);
      setClusterData(false, true);
      setVersionData();

      sync.identify();

      assert.isTrue(identifySpy.notCalled);
    });

    it("sends the correct value of clusterID, version and enterprise", function () {
      setVersionData();

      _.each([false, true], (enterpriseSetting) => {
        identifySpy.reset();
        setClusterData(true, enterpriseSetting);
        const sync = new AnalyticsSync(analytics, store, []);
        sync.identify();

        assert.isTrue(identifySpy.calledOnce);
        assert.deepEqual(identifySpy.args[0][0], {
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
      const sync = new AnalyticsSync(analytics, store, []);
      setClusterData(true, true);
      setVersionData();

      sync.identify();
      sync.identify();

      assert.isTrue(identifySpy.calledOnce);
    });
  });
});
