// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useEffect, useRef } from "react";
import { useSelector, useStore } from "react-redux";

import { AdminUIState } from "src/redux/state";
import {
  VERSION_DISMISSED_KEY,
  INSTRUCTIONS_BOX_COLLAPSED_KEY,
  loadUIData,
  isInFlight,
} from "src/redux/uiData";
import { getDataFromServer } from "src/util/dataFromServer";

/**
 * AlertDataProvider loads persistent UI data needed by the alert system.
 *
 * Previously this component also bootstrapped Redux cached data (cluster,
 * nodes, settings, version) for legacy selectors. All data fetching is
 * now handled by SWR hooks in the consuming components.
 */
export function AlertDataProvider(): React.ReactElement {
  const store = useStore<AdminUIState>();

  // Check login state.
  const loginState = useSelector((state: AdminUIState) => state.login);
  const { Insecure } = getDataFromServer();
  const isLoggedIn =
    Insecure || (loginState?.loggedInUser && loginState.loggedInUser !== "");

  // Load persistent UI data once on login.
  const uiData = useSelector((state: AdminUIState) => state.uiData);
  const lastUIDataRef = useRef<typeof uiData>(undefined);
  useEffect(() => {
    if (!isLoggedIn) return;
    if (uiData === lastUIDataRef.current) return;
    lastUIDataRef.current = uiData;

    const keysToMaybeLoad = [
      VERSION_DISMISSED_KEY,
      INSTRUCTIONS_BOX_COLLAPSED_KEY,
    ];
    const state = { uiData } as AdminUIState;
    const keysToLoad = keysToMaybeLoad.filter(key => {
      return !(key in uiData) && !isInFlight(state, key);
    });
    if (keysToLoad.length > 0) {
      loadUIData(store.dispatch, store.getState, ...keysToLoad);
    }
  }, [store, isLoggedIn, uiData]);

  return null;
}
