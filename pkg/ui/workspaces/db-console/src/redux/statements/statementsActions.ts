// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { TimeScale } from "@cockroachlabs/cluster-ui";

import { PayloadAction } from "src/interfaces/action";
import { setTimeScale } from "src/redux/timeScale";

// setGlobalTimeScaleAction sets the global time scale. Previously this went
// through a saga; now it directly dispatches the timeScale reducer action.
export const setGlobalTimeScaleAction = (
  ts: TimeScale,
): PayloadAction<TimeScale> => setTimeScale(ts);
