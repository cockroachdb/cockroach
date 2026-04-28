// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Analytics } from "@segment/analytics-node";

import { signUpEmailSubscription } from "./customAnalyticsSagas";

describe("customAnalytics", () => {
  describe("signUpEmailSubscription", () => {
    it("calls analytics#identify with user email and dispatches local setting", async () => {
      const analyticsIdentifyFn = jest
        .spyOn(Analytics.prototype, "identify")
        .mockImplementation(() => new Analytics({ writeKey: "test" }));
      const clusterId = "cluster-1";
      const email = "foo@bar.com";
      const dispatch = jest.fn();

      await signUpEmailSubscription(clusterId, email, dispatch);

      expect(analyticsIdentifyFn).toHaveBeenCalledWith(
        expect.objectContaining({
          userId: clusterId,
          traits: expect.objectContaining({
            email,
          }),
        }),
      );
      expect(dispatch).toHaveBeenCalled();

      analyticsIdentifyFn.mockRestore();
    });
  });
});
