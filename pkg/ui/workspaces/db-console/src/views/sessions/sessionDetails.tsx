// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { SessionDetails, TimeScale } from "@cockroachlabs/cluster-ui";
import React from "react";
import { useDispatch } from "react-redux";

import { setTimeScale } from "src/redux/timeScale";

const SessionDetailsPage: React.FC = () => {
  const dispatch = useDispatch();
  return (
    <SessionDetails
      setTimeScale={(ts: TimeScale) => dispatch(setTimeScale(ts))}
    />
  );
};

export default SessionDetailsPage;
