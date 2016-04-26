import reducer, { UISetting, setUISetting, UISettingsDict } from "./ui";
import { assert } from "chai";

describe("ui reducer", () => {
  describe("actions", () => {
    it("should create the correct action to set a ui setting", () => {
      const settingName = "test-setting";
      const settingValue = { val: "arbitrary-value" };
      const expectedSetting: UISetting = {
        key: settingName,
        value: settingValue,
      };
      assert.deepEqual(
        setUISetting(settingName, settingValue).payload,
        expectedSetting);
    });
  });

  describe("reducer", () => {
    it("should have the correct default value.", () => {
      assert.deepEqual(
        reducer(undefined, { type: "unknown" }),
        new UISettingsDict()
      );
    });

    describe("SET_UI_VALUE", () => {
      it("should correctly set UI values by key.", () => {
        const key = "test-setting";
        const value = "test-value";
        let expected: UISettingsDict = {
          [key]: value,
        };
        let actual = reducer(undefined, setUISetting(key, value));
        assert.deepEqual(actual, expected);

        const key2 = "another-setting";
        expected[key2] = value;
        actual = reducer(actual, setUISetting(key2, value));
        assert.deepEqual(actual, expected);
      });

      it("should correctly overwrite previous values.", () => {
        const key = "test-setting";
        const value = "test-value";
        let expected: UISettingsDict = {
          [key]: value,
        };
        let initial: UISettingsDict = {
          [key]: "oldvalue",
        };
        assert.deepEqual(
          reducer(initial, setUISetting(key, value)),
          expected
        );
      });
    });
  });
});
