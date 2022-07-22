// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { expectSaga } from "redux-saga-test-plan";
import Analytics from "analytics-node";

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
