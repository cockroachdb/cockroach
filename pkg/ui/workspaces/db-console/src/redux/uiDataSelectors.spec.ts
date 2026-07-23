// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { UIData, UIDataStatus } from "src/redux/uiData";

import { dismissReleaseNotesSignupForm } from "./uiDataSelectors";

describe("uiDataSelectors", () => {
  describe("dismissReleaseNotesSignupForm selector", () => {
    const selector = dismissReleaseNotesSignupForm.resultFunc;

    it("returns `false` if uiData status is VALID and has no data", () => {
      const uiData: UIData = {
        status: UIDataStatus.VALID,
        error: null,
        data: undefined,
      };
      expect(selector(uiData)).toBe(false);
    });

    it("returns `true` if uiData status is VALID and data = true", () => {
      const uiData: UIData = {
        status: UIDataStatus.VALID,
        error: null,
        data: true,
      };
      expect(selector(uiData)).toBe(true);
    });

    it("returns `true` if uiData status is UNINITIALIZED", () => {
      const uiData: UIData = {
        status: UIDataStatus.UNINITIALIZED,
        error: null,
        data: undefined,
      };
      expect(selector(uiData)).toBe(true);
    });

    it("returns `true` if uiData state is undefined", () => {
      expect(selector(undefined)).toBe(true);
    });
  });
});
