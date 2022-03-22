// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { PayloadAction } from "src/interfaces/action";

export const EMAIL_SUBSCRIPTION_SIGN_UP =
  "cockroachui/customanalytics/EMAIL_SUBSCRIPTION_SIGN_UP";

export type EmailSubscriptionSignUpPayload = {
  email: string;
  clusterId: string;
};

export function signUpForEmailSubscription(
  clusterId: string,
  email: string,
): PayloadAction<EmailSubscriptionSignUpPayload> {
  return {
    type: EMAIL_SUBSCRIPTION_SIGN_UP,
    payload: {
      email,
      clusterId,
    },
  };
}
