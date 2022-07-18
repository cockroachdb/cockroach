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
import { TimeScale, defaultTimeScaleSelected } from "@cockroachlabs/cluster-ui";

const localSettingsSelector = (state: AdminUIState) => state.localSettings;

const setting = new LocalSetting<AdminUIState, TimeScale>(
  "timeScale/SQLActivity",
  localSettingsSelector,
  defaultTimeScaleSelected,
);

export const globalTimeScaleLocalSetting = {
  ...setting,
  selector: (state: AdminUIState): TimeScale => {
    const val = setting.selector(state);
    if (typeof val.windowSize !== "string") {
      return val;
    }
    return {
      key: val.key,
      windowSize: val.windowSize && moment.duration(val.windowSize),
      windowValid: val.windowValid && moment.duration(val.windowValid),
      sampleSize: val.sampleSize && moment.duration(val.sampleSize),
      fixedWindowEnd: val.fixedWindowEnd,
    };
  },
};
