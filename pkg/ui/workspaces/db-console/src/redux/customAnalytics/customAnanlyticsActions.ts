// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
