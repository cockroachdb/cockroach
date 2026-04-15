// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Analytics } from "@segment/analytics-node";

import { emailSubscriptionAlertLocalSetting } from "src/redux/alerts";
import { COCKROACHLABS_ADDR } from "src/util/cockroachlabsAPI";

export type AnalyticsClientTarget = "email_sign_up";

// TODO (koorosh): has to be moved out from code base
const EMAIL_SIGN_UP_CLIENT_KEY = "72EEC0nqQKfoLWq0ZcGoTkJFIG9G9SII";

export function getAnalyticsClientFor(
  target: AnalyticsClientTarget,
): Analytics {
  switch (target) {
    case "email_sign_up":
      return new Analytics({
        writeKey: EMAIL_SIGN_UP_CLIENT_KEY,
        host: COCKROACHLABS_ADDR + "/api/segment",
      });
    default:
      throw new Error("Unrecognized Analytics Client target.");
  }
}

export async function signUpEmailSubscription(
  clusterId: string,
  email: string,
  dispatch: (action: any) => void,
): Promise<void> {
  const client = getAnalyticsClientFor("email_sign_up");
  await client.identify({
    userId: clusterId,
    traits: {
      email,
      release_notes_sign_up_from_admin_ui: "true",
      product_updates: "true",
    },
  });
  dispatch(emailSubscriptionAlertLocalSetting.set(true));
}
