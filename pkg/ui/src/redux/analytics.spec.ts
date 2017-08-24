import { assert } from "chai";
import * as sinon from "sinon";

import Analytics from "analytics-node";
import { Location } from "history";
import { Store } from "redux";

import { AdminUIState, createAdminUIStore } from "./state";
import { AnalyticsSync } from "./analytics";
import { clusterReducerObj } from "./apiReducers";

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

    const setClusterID = function () {
      store.dispatch(clusterReducerObj.receiveData(
        new protos.cockroach.server.serverpb.ClusterResponse({
          cluster_id: clusterID,
        }),
      ));
    };

    it("does nothing if analytics is not enabled.", function () {
      const sync = new AnalyticsSync(analytics, store, []);

      sync.page({
        pathname: "/test/path",
      } as Location);

      assert.isTrue(pageSpy.notCalled);
    });

    it("does nothing if cluster ID is not available.", function () {
      const sync = new AnalyticsSync(analytics, store, []);
      sync.setEnabled(true);

      sync.page({
        pathname: "/test/path",
      } as Location);

      assert.isTrue(pageSpy.notCalled);
    });

    it("correctly calls segment on a page call", function () {
      setClusterID();
      const sync = new AnalyticsSync(analytics, store, []);
      sync.setEnabled(true);

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
      sync.setEnabled(true);

      sync.page({
        pathname: "/test/path",
      } as Location);

      setClusterID();
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
      setClusterID();
      const sync = new AnalyticsSync(analytics, store, [
        {
          match: RegExp("/test/.*/path"),
          replace: "/test/[redacted]/path",
        },
      ]);
      sync.setEnabled(true);

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
});
