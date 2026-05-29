// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createHashHistory } from "history";
import moment from "moment-timezone";
import { Store } from "redux";

import {
  AlertLevel,
  disconnectedAlertSelector,
  disconnectedDismissedLocalSetting,
  emailSubscriptionAlertLocalSetting,
  emailSubscriptionAlertSelector,
  clusterPreserveDowngradeOptionDismissedSetting,
  getClusterPreserveDowngradeOptionOvertime,
  getStaggeredVersionWarning,
  getNewVersionNotification,
} from "./alerts";
import { setHealthError } from "./health";
import { AdminUIState, createAdminUIStore } from "./state";

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
    sessionStorage.clear();
  });

  describe("pure helper functions", function () {
    describe("getStaggeredVersionWarning", function () {
      it("returns undefined when versions map is empty", function () {
        const alert = getStaggeredVersionWarning(new Map(), false);
        expect(alert).toBeUndefined();
      });

      it("returns undefined when only one version", function () {
        const alert = getStaggeredVersionWarning(
          new Map([["v22.1", 3]]),
          false,
        );
        expect(alert).toBeUndefined();
      });

      it("returns warning when multiple versions detected", function () {
        const alert = getStaggeredVersionWarning(
          new Map([
            ["v22.1", 2],
            ["v21.1.7", 1],
          ]),
          false,
        );
        expect(alert).toBeDefined();
        expect(alert.level).toEqual(AlertLevel.WARNING);
      });

      it("returns undefined when dismissed", function () {
        const alert = getStaggeredVersionWarning(
          new Map([
            ["v22.1", 2],
            ["v21.1.7", 1],
          ]),
          true,
        );
        expect(alert).toBeUndefined();
      });
    });

    describe("getNewVersionNotification", function () {
      it("returns undefined when no newer versions", function () {
        const alert = getNewVersionNotification(
          null,
          true,
          moment(0),
          moment(0),
        );
        expect(alert).toBeUndefined();
      });

      it("returns notification when newer version available", function () {
        const alert = getNewVersionNotification(
          { details: [{ version: "0.1", detail: "alpha" }] },
          true,
          moment(0),
          moment(0),
        );
        expect(alert).toBeDefined();
        expect(alert.level).toEqual(AlertLevel.NOTIFICATION);
        expect(alert.title).toEqual("New Version Available");
      });

      it("respects local dismissal", function () {
        const alert = getNewVersionNotification(
          { details: [{ version: "0.1", detail: "alpha" }] },
          true,
          moment(0),
          moment(), // dismissed just now
        );
        expect(alert).toBeUndefined();
      });
    });

    describe("getClusterPreserveDowngradeOptionOvertime", function () {
      it("returns undefined when dismissed", function () {
        const alert = getClusterPreserveDowngradeOptionOvertime(
          {
            "cluster.preserve_downgrade_option": {
              name: "cluster.preserve_downgrade_option",
              value: "22.1",
              type: "s" as any,
              description: "",
              lastUpdated: moment().subtract(3, "days"),
              public: true,
            },
          },
          true,
        );
        expect(alert).toBeUndefined();
      });

      it("returns undefined when setting not present", function () {
        const alert = getClusterPreserveDowngradeOptionOvertime({}, false);
        expect(alert).toBeUndefined();
      });
    });
  });

  describe("selectors", function () {
    describe("disconnected alert", function () {
      it("does not display when no health error is set", function () {
        const alert = disconnectedAlertSelector(state());
        expect(alert).toBeUndefined();
      });

      it("does not display when health error is cleared", function () {
        dispatch(setHealthError(new Error("error")));
        dispatch(setHealthError(null));
        const alert = disconnectedAlertSelector(state());
        expect(alert).toBeUndefined();
      });

      it("displays when health error is set", function () {
        dispatch(setHealthError(new Error("error")));
        const alert = disconnectedAlertSelector(state());
        expect(typeof alert).toBe("object");
        expect(alert.level).toEqual(AlertLevel.CRITICAL);
        expect(alert.title).toEqual(
          "We're currently having some trouble fetching updated data. If this persists, it might be a good idea to check your network connection to the CockroachDB cluster.",
        );
      });

      it("does not display if dismissed locally", function () {
        dispatch(setHealthError(new Error("error")));
        dispatch(disconnectedDismissedLocalSetting.set(moment()));
        const alert = disconnectedAlertSelector(state());
        expect(alert).toBeUndefined();
      });

      it("dismisses by setting local dismissal", function (done) {
        dispatch(setHealthError(new Error("error")));
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
        dispatch(emailSubscriptionAlertLocalSetting.set(true));
        let openState = emailSubscriptionAlertLocalSetting.selector(state());
        expect(openState).toBe(true);

        const alert = emailSubscriptionAlertSelector(state());
        await alert.dismiss(dispatch, state);
        openState = emailSubscriptionAlertLocalSetting.selector(state());
        expect(openState).toBe(false);
      });
    });

    describe("cluster.preserve_downgrade_option dismissed setting", () => {
      it("initialized with default false state", () => {
        const settingState =
          clusterPreserveDowngradeOptionDismissedSetting.selector(state());
        expect(settingState).toBe(false);
      });
    });
  });
});
