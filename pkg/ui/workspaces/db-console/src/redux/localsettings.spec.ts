// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Action } from "redux";
import {
  LocalSettingData,
  LocalSetting,
  setLocalSetting,
  LocalSettingsState,
  localSettingsReducer,
} from "./localsettings";
import { assert } from "chai";

describe("Local Settings", function () {
  describe("actions", function () {
    it("should create the correct action to set a ui setting", function () {
      const settingName = "test-setting";
      const settingValue = { val: "arbitrary-value" };
      const expectedSetting: LocalSettingData = {
        key: settingName,
        value: settingValue,
      };
      assert.deepEqual(
        setLocalSetting(settingName, settingValue).payload,
        expectedSetting,
      );
    });
  });

  describe("reducer", function () {
    it("should have the correct default value.", function () {
      assert.deepEqual(
        localSettingsReducer(undefined, { type: "unknown" }),
        {},
      );
    });

    describe("SET_UI_VALUE", function () {
      it("should correctly set UI values by key.", function () {
        const key = "test-setting";
        const value = "test-value";
        const expected: LocalSettingsState = {
          [key]: value,
        };
        let actual = localSettingsReducer(
          undefined,
          setLocalSetting(key, value),
        );
        assert.deepEqual(actual, expected);

        const key2 = "another-setting";
        expected[key2] = value;
        actual = localSettingsReducer(actual, setLocalSetting(key2, value));
        assert.deepEqual(actual, expected);
      });

      it("should correctly overwrite previous values.", function () {
        const key = "test-setting";
        const value = "test-value";
        const expected: LocalSettingsState = {
          [key]: value,
        };
        const initial: LocalSettingsState = {
          [key]: "oldvalue",
        };
        assert.deepEqual(
          localSettingsReducer(initial, setLocalSetting(key, value)),
          expected,
        );
      });
    });
  });

  describe("LocalSetting helper class", function () {
    let topLevelState: { localSettings: LocalSettingsState };
    const dispatch = function (action: Action) {
      topLevelState = {
        localSettings: localSettingsReducer(
          topLevelState.localSettings,
          action,
        ),
      };
    };

    beforeEach(function () {
      topLevelState = {
        localSettings: {},
      };
    });

    const settingName = "test-setting";
    const settingName2 = "test-setting-2";

    it("returns default values correctly.", function () {
      const numberSetting = new LocalSetting(
        settingName,
        (s: typeof topLevelState) => s.localSettings,
        99,
      );
      assert.equal(numberSetting.selector(topLevelState), 99);
    });

    it("sets values correctly.", function () {
      const numberSetting = new LocalSetting(
        settingName,
        (s: typeof topLevelState) => s.localSettings,
        99,
      );
      dispatch(numberSetting.set(20));
      assert.deepEqual(topLevelState, {
        localSettings: {
          [settingName]: 20,
        },
      });
    });

    it("works with multiple values correctly.", function () {
      const numberSetting = new LocalSetting(
        settingName,
        (s: typeof topLevelState) => s.localSettings,
        99,
      );
      const stringSetting = new LocalSetting<typeof topLevelState, string>(
        settingName2,
        (s: typeof topLevelState) => s.localSettings,
      );
      dispatch(numberSetting.set(20));
      dispatch(stringSetting.set("hello"));
      assert.deepEqual(topLevelState, {
        localSettings: {
          [settingName]: 20,
          [settingName2]: "hello",
        },
      });
    });

    it("should select values correctly.", function () {
      const numberSetting = new LocalSetting(
        settingName,
        (s: typeof topLevelState) => s.localSettings,
        99,
      );
      assert.equal(numberSetting.selector(topLevelState), 99);
      dispatch(numberSetting.set(5));
      assert.equal(numberSetting.selector(topLevelState), 5);
    });
  });
});
