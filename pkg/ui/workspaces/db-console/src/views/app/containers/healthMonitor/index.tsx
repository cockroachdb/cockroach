// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useHealth } from "@cockroachlabs/cluster-ui";
import { useEffect } from "react";
import { useDispatch } from "react-redux";

import { setHealthError } from "src/redux/health";

/**
 * HealthMonitor bridges the SWR-based health polling into Redux.
 * It renders nothing; its only job is to dispatch health error
 * state so that disconnectedAlertSelector can pick it up.
 */
export function HealthMonitor(): null {
  const { error } = useHealth();
  const dispatch = useDispatch();

  useEffect(() => {
    dispatch(setHealthError(error ?? null));
  }, [error, dispatch]);

  return null;
}
