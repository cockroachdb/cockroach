// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  TimezoneContext,
  CoordinatedUniversalTime,
  useClusterSettings,
} from "@cockroachlabs/cluster-ui";
import React from "react";

export const TimezoneProvider = (props: any) => {
  const { settingValues } = useClusterSettings({
    names: ["ui.default_timezone", "ui.display_timezone"],
  });
  const timezone =
    settingValues["ui.default_timezone"]?.value ||
    settingValues["ui.display_timezone"]?.value ||
    CoordinatedUniversalTime;
  return (
    <TimezoneContext.Provider value={timezone}>
      {props.children}
    </TimezoneContext.Provider>
  );
};
