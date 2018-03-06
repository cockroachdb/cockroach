import { assert } from "chai";
import fetchMock from "src/util/fetch-mock";
import { Store } from "redux";
import moment from "moment";

import * as protos from "src/js/protos";
import { API_PREFIX } from "src/util/api";
import { AdminUIState, createAdminUIStore } from "./state";
import {
  AlertLevel,
  alertDataSync,
  versionsSelector,
  staggeredVersionWarningSelector, staggeredVersionDismissedSetting,
  newVersionNotificationSelector, newVersionDismissedLocalSetting,
  disconnectedAlertSelector, disconnectedDismissedLocalSetting,
} from "./alerts";
import {
  VERSION_DISMISSED_KEY, INSTRUCTIONS_BOX_COLLAPSED_KEY,
  setUIDataKey, isInFlight,
} from "./uiData";
import {
  livenessReducerObj, versionReducerObj, nodesReducerObj, clusterReducerObj, healthReducerObj,
} from "./apiReducers";

describe("alerts", function() {
  let store: Store<AdminUIState>;
  let dispatch: typeof store.dispatch;
  let state: typeof store.getState;

  beforeEach(function () {
    store = createAdminUIStore();
    dispatch = store.dispatch;
    state = store.getState;
  });

  afterEach(function() {
    fetchMock.restore();
  });

  describe("selectors", function() {
    describe("versions", function() {
      it("tolerates missing liveness data", function () {
        dispatch(nodesReducerObj.receiveData([
          {
            build_info: {
              tag: "0.1",
            },
          },
          {
            build_info: {
              tag: "0.2",
            },
          },
        ]));
        const versions = versionsSelector(state());
        assert.deepEqual(versions, ["0.1", "0.2"]);
      });

      it("ignores decommissioned nodes", function () {
        dispatch(nodesReducerObj.receiveData([
          {
            build_info: {
              tag: "0.1",
            },
          },
          {
            desc: {
              node_id: 2,
            },
            build_info: {
              tag: "0.2",
            },
          },
        ]));

        dispatch(livenessReducerObj.receiveData(
          new protos.cockroach.server.serverpb.LivenessResponse({
            livenesses: [{
              node_id: 2,
              decommissioning: true,
            }],
          }),
        ));

        const versions = versionsSelector(state());
        assert.deepEqual(versions, ["0.1"]);
      });
    });

    describe("version mismatch warning", function () {
      it("requires versions to be loaded before displaying", function () {
        const alert = staggeredVersionWarningSelector(state());
        assert.isUndefined(alert);
      });

      it("does not display when versions match", function () {
        dispatch(nodesReducerObj.receiveData([
          {
            build_info: {
              tag: "0.1",
            },
          },
          {
            build_info: {
              tag: "0.1",
            },
          },
        ]));
        const alert = staggeredVersionWarningSelector(state());
        assert.isUndefined(alert);
      });

      it("displays when mismatch detected and not dismissed", function () {
        dispatch(nodesReducerObj.receiveData([
          {
            // `desc` intentionally omitted (must not affect outcome).
            build_info: {
              tag: "0.1",
            },
          },
          {
            desc: {
              node_id: 1,
            },
            build_info: {
              tag: "0.2",
            },
          },
        ]));
        const alert = staggeredVersionWarningSelector(state());
        assert.isObject(alert);
        assert.equal(alert.level, AlertLevel.WARNING);
        assert.equal(alert.title, "Staggered Version");
      });

      it("does not display if dismissed locally", function () {
        dispatch(nodesReducerObj.receiveData([
          {
            build_info: {
              tag: "0.1",
            },
          },
          {
            build_info: {
              tag: "0.2",
            },
          },
        ]));
        dispatch(staggeredVersionDismissedSetting.set(true));
        const alert = staggeredVersionWarningSelector(state());
        assert.isUndefined(alert);
      });

      it("dismisses by setting local dismissal", function () {
        dispatch(nodesReducerObj.receiveData([
          {
            build_info: {
              tag: "0.1",
            },
          },
          {
            build_info: {
              tag: "0.2",
            },
          },
        ]));
        const alert = staggeredVersionWarningSelector(state());
        dispatch(alert.dismiss);
        assert.isTrue(staggeredVersionDismissedSetting.selector(state()));
      });
    });

    describe("new version available notification", function () {
      it("displays nothing when versions have not yet been loaded", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        const alert = newVersionNotificationSelector(state());
        assert.isUndefined(alert);
      });

      it("displays nothing when persistent dismissal has not been checked", function () {
        dispatch(versionReducerObj.receiveData({
          details: [
            {
              version: "0.1",
              detail: "alpha",
            },
          ],
        }));
        const alert = newVersionNotificationSelector(state());
        assert.isUndefined(alert);
      });

      it("displays nothing when no new version is available", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        dispatch(versionReducerObj.receiveData({
          details: [],
        }));
        const alert = newVersionNotificationSelector(state());
        assert.isUndefined(alert);
      });

      it("displays when new version available and not dismissed", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        dispatch(versionReducerObj.receiveData({
          details: [
            {
              version: "0.1",
              detail: "alpha",
            },
          ],
        }));
        const alert = newVersionNotificationSelector(state());
        assert.isObject(alert);
        assert.equal(alert.level, AlertLevel.NOTIFICATION);
        assert.equal(alert.title, "New Version Available");
      });

      it("respects local dismissal setting", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        dispatch(versionReducerObj.receiveData({
          details: [
            {
              version: "0.1",
              detail: "alpha",
            },
          ],
        }));
        dispatch(newVersionDismissedLocalSetting.set(moment()));
        let alert = newVersionNotificationSelector(state());
        assert.isUndefined(alert);

        // Local dismissal only lasts one day.
        dispatch(newVersionDismissedLocalSetting.set(moment().subtract(2, "days")));
        alert = newVersionNotificationSelector(state());
        assert.isDefined(alert);
      });

      it("respects persistent dismissal setting", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, moment().valueOf()));
        dispatch(versionReducerObj.receiveData({
          details: [
            {
              version: "0.1",
              detail: "alpha",
            },
          ],
        }));
        let alert = newVersionNotificationSelector(state());
        assert.isUndefined(alert);

        // Dismissal only lasts one day.
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, moment().subtract(2, "days").valueOf()));
        alert = newVersionNotificationSelector(state());
        assert.isDefined(alert);
      });

      it("dismisses by setting local and persistent dismissal", function (done) {
        fetchMock.mock({
          matcher: `${API_PREFIX}/uidata`,
          method: "POST",
          response: (_url: string) => {
            const encodedResponse = protos.cockroach.server.serverpb.SetUIDataResponse.encode({}).finish();
            return {
              body: encodedResponse,
            };
          },
        });

        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        dispatch(versionReducerObj.receiveData({
          details: [
            {
              version: "0.1",
              detail: "alpha",
            },
          ],
        }));
        const alert = newVersionNotificationSelector(state());
        const beforeDismiss = moment();

        dispatch(alert.dismiss).then(() => {
          assert.isTrue(newVersionDismissedLocalSetting.selector(state()).isSameOrAfter(beforeDismiss));
          assert.isNotNull(state().uiData[VERSION_DISMISSED_KEY]);
          assert.isNotNull(state().uiData[VERSION_DISMISSED_KEY].data);
          const dismissedMoment = moment(state().uiData[VERSION_DISMISSED_KEY].data as number);
          assert.isTrue(dismissedMoment.isSameOrAfter(beforeDismiss));
          done();
        });
      });
    });

    describe("disconnected alert", function () {
      it("requires health to be available before displaying", function () {
        const alert = disconnectedAlertSelector(state());
        assert.isUndefined(alert);
      });

      it("does not display when cluster is healthy", function () {
        dispatch(healthReducerObj.receiveData(
          new protos.cockroach.server.serverpb.ClusterResponse({})),
        );
        const alert = disconnectedAlertSelector(state());
        assert.isUndefined(alert);
      });

      it("displays when cluster health endpoint returns an error", function () {
        dispatch(healthReducerObj.errorData(new Error("error")));
        const alert = disconnectedAlertSelector(state());
        assert.isObject(alert);
        assert.equal(alert.level, AlertLevel.CRITICAL);
        assert.equal(alert.title, "Connection to CockroachDB node lost.");
      });

      it("does not display if dismissed locally", function () {
        dispatch(healthReducerObj.errorData(new Error("error")));
        dispatch(disconnectedDismissedLocalSetting.set(moment()));
        const alert = disconnectedAlertSelector(state());
        assert.isUndefined(alert);
      });

      it("dismisses by setting local dismissal", function (done) {
        dispatch(healthReducerObj.errorData(new Error("error")));
        const alert = disconnectedAlertSelector(state());
        const beforeDismiss = moment();

        dispatch(alert.dismiss).then(() => {
          assert.isTrue(
            disconnectedDismissedLocalSetting.selector(state()).isSameOrAfter(beforeDismiss),
          );
          done();
        });
      });
    });
  });

  describe("data sync listener", function() {
    let sync: () => void;
    beforeEach(function() {
      // We don't care about the responses, we only care that the sync listener
      // is making requests, which can be verified using "inFlight" settings.
      fetchMock.mock({
        matcher: "*",
        method: "GET",
        response: () => 500,
      });

      sync = alertDataSync(store);
    });

    it("dispatches requests for expected data on empty store", function() {
      sync();
      assert.isTrue(isInFlight(state(), VERSION_DISMISSED_KEY));
      assert.isTrue(state().cachedData.cluster.inFlight);
      assert.isTrue(state().cachedData.nodes.inFlight);
      assert.isFalse(state().cachedData.version.inFlight);
      assert.isTrue(state().cachedData.health.inFlight);
    });

    it("dispatches request for version data when cluster ID and nodes are available", function() {
      dispatch(nodesReducerObj.receiveData([
        {
          build_info: {
            tag: "0.1",
          },
        },
      ]));
      dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({
        cluster_id: "my-cluster",
      })));

      sync();
      assert.isTrue(state().cachedData.version.inFlight);
    });

    it("does not request version data when version is staggered", function() {
      dispatch(nodesReducerObj.receiveData([
        {
          build_info: {
            tag: "0.1",
          },
        },
        {
          build_info: {
            tag: "0.2",
          },
        },
      ]));
      dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({
        cluster_id: "my-cluster",
      })));

      sync();
      assert.isFalse(state().cachedData.version.inFlight);
    });

    it("refreshes health function whenever the last health response is no longer valid.", function() {
      dispatch(healthReducerObj.receiveData(
        new protos.cockroach.server.serverpb.ClusterResponse({})),
      );
      dispatch(healthReducerObj.invalidateData());
      sync();
      assert.isTrue(state().cachedData.health.inFlight);
    });

    it("does not do anything when all data is available.", function() {
      dispatch(nodesReducerObj.receiveData([
        {
          build_info: {
            tag: "0.1",
          },
        },
      ]));
      dispatch(clusterReducerObj.receiveData(new protos.cockroach.server.serverpb.ClusterResponse({
        cluster_id: "my-cluster",
      })));
      dispatch(setUIDataKey(VERSION_DISMISSED_KEY, "blank"));
      dispatch(setUIDataKey(INSTRUCTIONS_BOX_COLLAPSED_KEY, false));
      dispatch(versionReducerObj.receiveData({
        details: [],
      }));
      dispatch(healthReducerObj.receiveData(
        new protos.cockroach.server.serverpb.ClusterResponse({})),
      );

      const expectedState = state();
      sync();
      assert.deepEqual(state(), expectedState);
    });
  });
});
