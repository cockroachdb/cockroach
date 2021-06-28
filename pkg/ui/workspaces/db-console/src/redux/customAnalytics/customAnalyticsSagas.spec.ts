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
import sinon from "sinon";
import Analytics from "analytics-node";

import { signUpEmailSubscription } from "./customAnalyticsSagas";
import { signUpForEmailSubscription } from "./customAnanlyticsActions";

const sandbox = sinon.createSandbox();

describe("customAnalyticsSagas", () => {
  describe("signUpEmailSubscription generator", () => {
    afterEach(() => {
      sandbox.reset();
    });

    it("calls analytics#identify with user email in args ", () => {
      const analyticsIdentifyFn = sandbox.stub(Analytics.prototype, "identify");
      const clusterId = "cluster-1";
      const email = "foo@bar.com";
      const action = signUpForEmailSubscription(clusterId, email);

      return expectSaga(signUpEmailSubscription, action)
        .dispatch(action)
        .run()
        .then(() => {
          const expectedAnalyticsMessage = {
            userId: clusterId,
            traits: {
              email,
            },
          };
          analyticsIdentifyFn.calledOnceWith(expectedAnalyticsMessage);
        });
    });
  });
});
