// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createHashHistory } from "history";
import Long from "long";
import moment from "moment-timezone";
import { Store } from "redux";

import * as protos from "src/js/protos";
import { cockroach } from "src/js/protos";
import { versionsSelector } from "src/redux/nodes";
import { API_PREFIX } from "src/util/api";
import fetchMock from "src/util/fetch-mock";

import {
  AlertLevel,
  alertDataSync,
  staggeredVersionWarningSelector,
  staggeredVersionDismissedSetting,
  newVersionNotificationSelector,
  newVersionDismissedLocalSetting,
  disconnectedAlertSelector,
  disconnectedDismissedLocalSetting,
  emailSubscriptionAlertLocalSetting,
  emailSubscriptionAlertSelector,
  clusterPreserveDowngradeOptionDismissedSetting,
  clusterPreserveDowngradeOptionOvertimeSelector,
} from "./alerts";
import {
  livenessReducerObj,
  versionReducerObj,
  nodesReducerObj,
  clusterReducerObj,
  healthReducerObj,
  settingsReducerObj,
} from "./apiReducers";
import { loginSuccess } from "./login";
import { AdminUIState, AppDispatch, createAdminUIStore } from "./state";
import {
  VERSION_DISMISSED_KEY,
  INSTRUCTIONS_BOX_COLLAPSED_KEY,
  setUIDataKey,
  isInFlight,
} from "./uiData";

import MembershipStatus = cockroach.kv.kvserver.liveness.livenesspb.MembershipStatus;

describe("alerts", function () {
  let store: Store<AdminUIState>;
  let dispatch: typeof store.dispatch;
  let state: typeof store.getState;

  beforeEach(function () {
    store = createAdminUIStore(createHashHistory());
    dispatch = store.dispatch;
    state = store.getState;
  });

  afterEach(function () {
    fetchMock.restore();
    sessionStorage.clear();
  });

  describe("selectors", function () {
    describe("versions", function () {
      it("tolerates missing liveness data", function () {
        dispatch(
          nodesReducerObj.receiveData([
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
          ]),
        );
        const versions = versionsSelector(state());
        expect(versions).toEqual(["0.1", "0.2"]);
      });

      it("ignores decommissioning/decommissioned nodes", function () {
        dispatch(
          nodesReducerObj.receiveData([
            {
              desc: {
                node_id: 1,
              },
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
            {
              desc: {
                node_id: 3,
              },
              build_info: {
                tag: "0.3",
              },
            },
          ]),
        );

        dispatch(
          livenessReducerObj.receiveData(
            new protos.cockroach.server.serverpb.LivenessResponse({
              livenesses: [
                {
                  node_id: 1,
                  membership: MembershipStatus.ACTIVE,
                },
                {
                  node_id: 2,
                  membership: MembershipStatus.DECOMMISSIONING,
                },
                {
                  node_id: 3,
                  membership: MembershipStatus.DECOMMISSIONED,
                },
              ],
            }),
          ),
        );

        const versions = versionsSelector(state());
        expect(versions).toEqual(["0.1"]);
      });
    });

    describe("version mismatch warning", function () {
      it("requires versions to be loaded before displaying", function () {
        const numAlert = staggeredVersionWarningSelector(state());
        expect(numAlert).toBeUndefined();
      });

      it("does not display when versions match", function () {
        dispatch(
          nodesReducerObj.receiveData([
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
          ]),
        );
        const numAlert = staggeredVersionWarningSelector(state());
        expect(numAlert).toBeUndefined();
      });

      it("displays when mismatch detected and not dismissed", function () {
        dispatch(
          nodesReducerObj.receiveData([
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
          ]),
        );
        const numAlert = staggeredVersionWarningSelector(state());
        expect(typeof numAlert).toBe("object");
        expect(numAlert.level).toEqual(AlertLevel.WARNING);
        expect(numAlert.title).toEqual(
          "Multiple versions of CockroachDB are running on this cluster.",
        );
      });

      it("does not display if dismissed locally", function () {
        dispatch(
          nodesReducerObj.receiveData([
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
          ]),
        );
        dispatch(staggeredVersionDismissedSetting.set(true));
        const numAlert = staggeredVersionWarningSelector(state());
        expect(numAlert).toBeUndefined();
      });

      it("dismisses by setting local dismissal", function () {
        dispatch(
          nodesReducerObj.receiveData([
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
          ]),
        );
        const numAlert = staggeredVersionWarningSelector(state());
        numAlert.dismiss(dispatch, state);
        expect(staggeredVersionDismissedSetting.selector(state())).toBe(true);
      });

      it("num alert dismisses by setting local dismissal", function () {
        dispatch(
          nodesReducerObj.receiveData([
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
          ]),
        );
        const numAlert = staggeredVersionWarningSelector(state());
        numAlert.dismiss(dispatch, state);
        expect(staggeredVersionDismissedSetting.selector(state())).toBe(true);
      });
    });

    describe("new version available notification", function () {
      it("displays nothing when versions have not yet been loaded", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        const alert = newVersionNotificationSelector(state());
        expect(alert).toBeUndefined();
      });

      it("displays nothing when persistent dismissal has not been checked", function () {
        dispatch(
          versionReducerObj.receiveData({
            details: [
              {
                version: "0.1",
                detail: "alpha",
              },
            ],
          }),
        );
        const alert = newVersionNotificationSelector(state());
        expect(alert).toBeUndefined();
      });

      it("displays nothing when no new version is available", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        dispatch(
          versionReducerObj.receiveData({
            details: [],
          }),
        );
        const alert = newVersionNotificationSelector(state());
        expect(alert).toBeUndefined();
      });

      it("displays when new version available and not dismissed", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        dispatch(
          versionReducerObj.receiveData({
            details: [
              {
                version: "0.1",
                detail: "alpha",
              },
            ],
          }),
        );
        const alert = newVersionNotificationSelector(state());
        expect(typeof alert).toBe("object");
        expect(alert.level).toEqual(AlertLevel.NOTIFICATION);
        expect(alert.title).toEqual("New Version Available");
      });

      it("respects local dismissal setting", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        dispatch(
          versionReducerObj.receiveData({
            details: [
              {
                version: "0.1",
                detail: "alpha",
              },
            ],
          }),
        );
        dispatch(newVersionDismissedLocalSetting.set(moment()));
        let alert = newVersionNotificationSelector(state());
        expect(alert).toBeUndefined();

        // Local dismissal only lasts one day.
        dispatch(
          newVersionDismissedLocalSetting.set(moment().subtract(2, "days")),
        );
        alert = newVersionNotificationSelector(state());
        expect(alert).toBeDefined();
      });

      it("respects persistent dismissal setting", function () {
        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, moment().valueOf()));
        dispatch(
          versionReducerObj.receiveData({
            details: [
              {
                version: "0.1",
                detail: "alpha",
              },
            ],
          }),
        );
        let alert = newVersionNotificationSelector(state());
        expect(alert).toBeUndefined();

        // Dismissal only lasts one day.
        dispatch(
          setUIDataKey(
            VERSION_DISMISSED_KEY,
            moment().subtract(2, "days").valueOf(),
          ),
        );
        alert = newVersionNotificationSelector(state());
        expect(alert).toBeDefined();
      });

      it("dismisses by setting local and persistent dismissal", function (done) {
        fetchMock.mock({
          matcher: `${API_PREFIX}/uidata`,
          method: "POST",
          response: (_url: string) => {
            const encodedResponse =
              protos.cockroach.server.serverpb.SetUIDataResponse.encode(
                {},
              ).finish();
            return {
              body: encodedResponse,
            };
          },
        });

        dispatch(setUIDataKey(VERSION_DISMISSED_KEY, null));
        dispatch(
          versionReducerObj.receiveData({
            details: [
              {
                version: "0.1",
                detail: "alpha",
              },
            ],
          }),
        );
        const alert = newVersionNotificationSelector(state());
        const beforeDismiss = moment();

        alert.dismiss(dispatch, state).then(() => {
          expect(
            newVersionDismissedLocalSetting
              .selector(state())
              .isSameOrAfter(beforeDismiss),
          ).toBe(true);
          expect(state().uiData[VERSION_DISMISSED_KEY]).not.toBeNull();
          expect(state().uiData[VERSION_DISMISSED_KEY].data).not.toBeNull();
          const dismissedMoment = moment(
            state().uiData[VERSION_DISMISSED_KEY].data as number,
          );
          expect(dismissedMoment.isSameOrAfter(beforeDismiss)).toBe(true);
          done();
        });
      });
    });

    describe("disconnected alert", function () {
      it("requires health to be available before displaying", function () {
        const alert = disconnectedAlertSelector(state());
        expect(alert).toBeUndefined();
      });

      it("does not display when cluster is healthy", function () {
        dispatch(
          healthReducerObj.receiveData(
            new protos.cockroach.server.serverpb.ClusterResponse({}),
          ),
        );
        const alert = disconnectedAlertSelector(state());
        expect(alert).toBeUndefined();
      });

      it("displays when cluster health endpoint returns an error", function () {
        dispatch(healthReducerObj.errorData(new Error("error")));
        const alert = disconnectedAlertSelector(state());
        expect(typeof alert).toBe("object");
        expect(alert.level).toEqual(AlertLevel.CRITICAL);
        expect(alert.title).toEqual(
          "We're currently having some trouble fetching updated data. If this persists, it might be a good idea to check your network connection to the CockroachDB cluster.",
        );
      });

      it("does not display if dismissed locally", function () {
        dispatch(healthReducerObj.errorData(new Error("error")));
        dispatch(disconnectedDismissedLocalSetting.set(moment()));
        const alert = disconnectedAlertSelector(state());
        expect(alert).toBeUndefined();
      });

      it("dismisses by setting local dismissal", function (done) {
        dispatch(healthReducerObj.errorData(new Error("error")));
        const alert = disconnectedAlertSelector(state());
        const beforeDismiss = moment();

        alert.dismiss(dispatch, state).then(() => {
          expect(
            disconnectedDismissedLocalSetting
              .selector(state())
              .isSameOrAfter(beforeDismiss),
          ).toBe(true);
          done();
        });
      });
    });

    describe("email signup for release notes alert", () => {
      it("initialized with default 'false' (hidden) state", () => {
        const settingState =
          emailSubscriptionAlertLocalSetting.selector(state());
        expect(settingState).toBe(false);
      });

      it("dismissed by alert#dismiss", async () => {
        // set alert to open state
        dispatch(emailSubscriptionAlertLocalSetting.set(true));
        let openState = emailSubscriptionAlertLocalSetting.selector(state());
        expect(openState).toBe(true);

        // dismiss alert
        const alert = emailSubscriptionAlertSelector(state());
        await alert.dismiss(dispatch, state);
        openState = emailSubscriptionAlertLocalSetting.selector(state());
        expect(openState).toBe(false);
      });
    });

    describe("cluster.preserve_downgrade_option overtime alert", () => {
      it("initialized with default false state", () => {
        const settingState =
          clusterPreserveDowngradeOptionDismissedSetting.selector(state());
        expect(settingState).toBe(false);
      });
      it("returns an alert if cluster.preserve_downgrad_option is lastUpdated >48 hours ago", () => {
        dispatch(
          settingsReducerObj.receiveData(
            new protos.cockroach.server.serverpb.SettingsResponse({
              key_values: {
                "cluster.preserve_downgrade_option": {
                  last_updated: {
                    seconds: Long.fromInt(165000000),
                    nanos: 165000000,
                  },
                  value: "22.1",
                },
              },
            }),
          ),
        );
        const alert = clusterPreserveDowngradeOptionOvertimeSelector(state());
        expect(alert).toBeDefined();
      });
      it("does not display alert once dismissed", async () => {
        dispatch(
          settingsReducerObj.receiveData(
            new protos.cockroach.server.serverpb.SettingsResponse({
              key_values: {
                "cluster.preserve_downgrade_option": {
                  last_updated: {
                    seconds: Long.fromInt(165000000),
                    nanos: 165000000,
                  },
                  value: "22.1",
                },
              },
            }),
          ),
        );

        // dismiss alert
        const alert = clusterPreserveDowngradeOptionOvertimeSelector(state());
        await alert.dismiss(dispatch, state);
        const openState =
          clusterPreserveDowngradeOptionDismissedSetting.selector(state());
        expect(openState).toBe(true);
      });
    });
  });

  describe("data sync listener", function () {
    let sync: () => void;
    beforeEach(function () {
      // We don't care about the responses, we only care that the sync listener
      // is making requests, which can be verified using "inFlight" settings.
      fetchMock.mock({
        matcher: "*",
        method: "GET",
        response: () => 500,
      });
      const loginDispatch = dispatch as AppDispatch;
      loginDispatch(loginSuccess("test"));
      sync = alertDataSync(store);
    });

    it("dispatches requests for expected data on empty store", function () {
      sync();
      expect(isInFlight(state(), VERSION_DISMISSED_KEY)).toBe(true);
      expect(state().cachedData.cluster.inFlight).toBe(true);
      expect(state().cachedData.nodes.inFlight).toBe(true);
      expect(state().cachedData.version.inFlight).toBe(false);
      expect(state().cachedData.health.inFlight).toBe(true);
    });

    it("dispatches request for version data when cluster ID and nodes are available", function () {
      dispatch(
        nodesReducerObj.receiveData([
          {
            build_info: {
              tag: "0.1",
            },
          },
        ]),
      );
      dispatch(
        clusterReducerObj.receiveData(
          new protos.cockroach.server.serverpb.ClusterResponse({
            cluster_id: "my-cluster",
          }),
        ),
      );

      sync();
      expect(state().cachedData.version.inFlight).toBe(true);
    });

    it("does not request version data when version is staggered", function () {
      dispatch(
        nodesReducerObj.receiveData([
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
        ]),
      );
      dispatch(
        clusterReducerObj.receiveData(
          new protos.cockroach.server.serverpb.ClusterResponse({
            cluster_id: "my-cluster",
          }),
        ),
      );

      sync();
      expect(state().cachedData.version.inFlight).toBe(false);
    });

    it("refreshes health function whenever the last health response is no longer valid.", function () {
      dispatch(
        healthReducerObj.receiveData(
          new protos.cockroach.server.serverpb.ClusterResponse({}),
        ),
      );
      dispatch(healthReducerObj.invalidateData());
      sync();
      expect(state().cachedData.health.inFlight).toBe(true);
    });

    it("does not do anything when all data is available.", function () {
      dispatch(
        nodesReducerObj.receiveData([
          {
            build_info: {
              tag: "0.1",
            },
          },
        ]),
      );
      dispatch(
        clusterReducerObj.receiveData(
          new protos.cockroach.server.serverpb.ClusterResponse({
            cluster_id: "my-cluster",
          }),
        ),
      );
      dispatch(setUIDataKey(VERSION_DISMISSED_KEY, "blank"));
      dispatch(setUIDataKey(INSTRUCTIONS_BOX_COLLAPSED_KEY, false));
      dispatch(
        versionReducerObj.receiveData({
          details: [],
        }),
      );
      dispatch(
        healthReducerObj.receiveData(
          new protos.cockroach.server.serverpb.ClusterResponse({}),
        ),
      );
      dispatch(
        settingsReducerObj.receiveData(
          new protos.cockroach.server.serverpb.SettingsResponse({}),
        ),
      );
      const expectedState = state();
      sync();
      expect(state()).toEqual(expectedState);
    });
  });
});
