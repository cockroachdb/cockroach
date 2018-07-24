import { push } from "react-router-redux";

export const SET_ROUTE_PARAM = "cockroachui/navigator/SET_ROUTE_PARAM";

export function setRouteParam(param, value) {
  return {
    type: SET_ROUTE_PARAM,
    payload: { param, value },
  };
}

export function navigatorMiddleware(store) {
  return function (next) {
    return function (action) {
      if (action.type !== SET_ROUTE_PARAM) {
        return next(action);
      }

      const state = store.getState();
      const oldLocation = state.routing.locationBeforeTransitions;

      const newLocation = Object.assign({}, oldLocation, {
        query: Object.assign({}, oldLocation.query, {
          [action.payload.param]: action.payload.value,
        }),
      });

      return next(push(newLocation));
    };
  };
}
