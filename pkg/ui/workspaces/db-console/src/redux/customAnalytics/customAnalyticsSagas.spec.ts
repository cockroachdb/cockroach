// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import Analytics from "analytics-node";
import { expectSaga } from "redux-saga-test-plan";

import { signUpEmailSubscription } from "./customAnalyticsSagas";
import { signUpForEmailSubscription } from "./customAnanlyticsActions";

describe("customAnalyticsSagas", () => {
  describe("signUpEmailSubscription generator", () => {
    it("calls analytics#identify with user email in args ", () => {
      const analyticsIdentifyFn = jest.spyOn(Analytics.prototype, "identify");
      const clusterId = "cluster-1";
      const email = "foo@bar.com";
      const action = signUpForEmailSubscription(clusterId, email);

      return expectSaga(signUpEmailSubscription, action)
        .dispatch(action)
        .run()
        .then(() => {
          expect(analyticsIdentifyFn).toHaveBeenCalledWith(
            expect.objectContaining({
              userId: clusterId,
              traits: expect.objectContaining({
                email,
              }),
            }),
          );
        });
    });
  });
});
