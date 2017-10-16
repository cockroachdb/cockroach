/**
 * Monitors the currently hovered chart and point in time.
 */

import _ from "lodash";
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
  currentlyHovering: boolean;
  // Which chart are we hovering over?
  hoverChart: string;
  // What point in time are we hovering over?
  hoverTime: moment.Moment;

  constructor() {
    this.currentlyHovering = false;
  }
}

export function hoverReducer(state = new HoverState(), action: Action): HoverState {
  switch (action.type) {
    case HOVER_ON:
      const { payload: hi } = action as PayloadAction<HoverInfo>;
      state = _.clone(state);
      state.currentlyHovering = true;
      state.hoverChart = hi.hoverChart;
      state.hoverTime = hi.hoverTime;
      return state;
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
