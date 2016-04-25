/// <reference path="../../typings/main.d.ts" />

/**
 * This module maintains various ephemeral UI settings. These settings should be
 * maintained within a session, but not saved between sessions.
 */

import assign = require("object-assign");
import { Action, PayloadAction } from "../interfaces/action";

const SET_UI_VALUE = "cockroachui/ui/SET_UI_VALUE";

export class UISetting {
  key: string;
  value: any;
}

export class UISettingsDict {
  [key: string]: any;
}

export default function reducer(state: UISettingsDict, action: Action): UISettingsDict {
  if (action === undefined) {
    return;
  }
  if (state === undefined) {
    state = {};
  }

  switch (action.type) {
    case SET_UI_VALUE:
      let { payload } = action as PayloadAction<UISetting>;
      let newObj = assign({}, state);
      newObj[payload.key] = payload.value;
      return newObj;
    default:
      return state;
  }
}

/**
 * Set an ephemeral UI setting.
 */
export function setUISetting(key: string, value: any): PayloadAction<UISetting> {
  return {
    type: SET_UI_VALUE,
    payload: {
      key: key,
      value: value,
    },
  };
}
