// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { useDispatch } from "react-redux";

import { actions as localStorageActions } from "src/store/localStorage";
import { TimeScale } from "src/timeScaleDropdown";

import { SessionDetails } from ".";

export const SessionDetailsPageConnected: React.FC = () => {
  const dispatch = useDispatch();
  return (
    <SessionDetails
      setTimeScale={(ts: TimeScale) =>
        dispatch(localStorageActions.updateTimeScale({ value: ts }))
      }
    />
  );
};
