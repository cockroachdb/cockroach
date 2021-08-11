// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import { LocalSetting } from "./localsettings";
import { AdminUIState } from "./state";

export type CombinedStatementsDateRangePayload = {
  start: number;
  end: number;
};

const localSettingsSelector = (state: AdminUIState) => state.localSettings;

// The default range for statements to display is one hour ago.
const oneHourAgo = {
  start: moment
    .utc()
    .subtract(1, "hours")
    .unix(),
  end: moment.utc().unix() + 60, // Add 1 minute to account for potential lag
};

export const statementsDateRangeLocalSetting = new LocalSetting<
  AdminUIState,
  CombinedStatementsDateRangePayload
>("statements_date_range", localSettingsSelector, oneHourAgo);
