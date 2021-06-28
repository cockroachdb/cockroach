// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/**
 * Monitors the currently hovered chart and point in time.
 */

import moment from "moment";
import { Action } from "redux";

import { PayloadAction } from "src/interfaces/action";
import { AdminUIState } from "src/redux/state";

export const HOVER_ON = "cockroachui/hover/HOVER_ON";
export const HOVER_OFF = "cockroachui/hover/HOVER_OFF";

/**
 * HoverInfo is conveys the current hover position to the state.
 */
export interface HoverInfo {
  hoverChart: string;
  hoverTime: moment.Moment;
}

export class HoverState {
  // Are we currently hovering over a chart?
  currentlyHovering = false;
  // Which chart are we hovering over?
  hoverChart: string;
  // What point in time are we hovering over?
  hoverTime: moment.Moment;
}

export function hoverReducer(
  state = new HoverState(),
  action: Action,
): HoverState {
  switch (action.type) {
    case HOVER_ON: {
      const { payload: hi } = action as PayloadAction<HoverInfo>;
      return {
        currentlyHovering: true,
        hoverChart: hi.hoverChart,
        hoverTime: hi.hoverTime,
      };
    }
    case HOVER_OFF:
      return new HoverState();
    default:
      return state;
  }
}

export function hoverOn(hi: HoverInfo): PayloadAction<HoverInfo> {
  return {
    type: HOVER_ON,
    payload: hi,
  };
}

export function hoverOff(): Action {
  return {
    type: HOVER_OFF,
  };
}

/**
 * Are we currently hovering, and if so, which chart and when?
 */
export const hoverStateSelector = (state: AdminUIState) => state.hover;
