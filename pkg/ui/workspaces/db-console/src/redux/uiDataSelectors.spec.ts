// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import { dismissReleaseNotesSignupForm } from "./uiDataSelectors";
import { UIData, UIDataStatus } from "src/redux/uiData";

describe("uiDataSelectors", () => {
  describe("dismissReleaseNotesSignupForm selector", () => {
    const selector = dismissReleaseNotesSignupForm.resultFunc;

    it("returns `false` if uiData status is VALID and has no data", () => {
      const uiData: UIData = {
        status: UIDataStatus.VALID,
        error: null,
        data: undefined,
      };
      assert.isFalse(selector(uiData));
    });

    it("returns `true` if uiData status is VALID and data = true", () => {
      const uiData: UIData = {
        status: UIDataStatus.VALID,
        error: null,
        data: true,
      };
      assert.isTrue(selector(uiData));
    });

    it("returns `true` if uiData status is UNINITIALIZED", () => {
      const uiData: UIData = {
        status: UIDataStatus.UNINITIALIZED,
        error: null,
        data: undefined,
      };
      assert.isTrue(selector(uiData));
    });

    it("returns `true` if uiData state is undefined", () => {
      assert.isTrue(selector(undefined));
    });
  });
});
