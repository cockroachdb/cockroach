// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { TimeScaleOptions } from "@cockroachlabs/cluster-ui";
import TimeScaleDropdown from "src/views/cluster/containers/timeScaleDropdownWithSearchParams";
import moment from "moment";

export const KeyVizualizerTimeWindow = () => {
  const keyVizualizerTimeScaleOptions: TimeScaleOptions = {
    "Past 10 Minutes": {
      windowSize: moment.duration(10, "minutes"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(10, "seconds"),
    },
    "Past 30 Minutes": {
      windowSize: moment.duration(30, "minutes"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(30, "seconds"),
    },
    "Past Hour": {
      windowSize: moment.duration(1, "hour"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(30, "seconds"),
    },
    "Past 6 Hours": {
      windowSize: moment.duration(6, "hours"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(1, "minutes"),
    },
    "Past Day": {
      windowSize: moment.duration(1, "day"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(5, "minutes"),
    },
    "Past 2 Days": {
      windowSize: moment.duration(2, "day"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(5, "minutes"),
    },
    "Past 3 Days": {
      windowSize: moment.duration(3, "day"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(5, "minutes"),
    },
    "Past Week": {
      windowSize: moment.duration(7, "days"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(30, "minutes"),
    },
    "Past 2 Weeks": {
      windowSize: moment.duration(14, "days"),
      windowValid: moment.duration(15, "minutes"),
      sampleSize: moment.duration(30, "minutes"),
    },
  };

  return (
    <TimeScaleDropdown
      options={keyVizualizerTimeScaleOptions}
      hasCustomOption={false}
    />
  );
};
