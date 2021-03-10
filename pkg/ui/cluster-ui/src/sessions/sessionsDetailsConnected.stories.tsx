import React from "react";
import { storiesOf } from "@storybook/react";
import { createMemoryHistory } from "history";
import createSagaMiddleware from "redux-saga";
import { Provider } from "react-redux";
import {
  ConnectedRouter,
  connectRouter,
  routerMiddleware,
} from "connected-react-router";
import {
  applyMiddleware,
  combineReducers,
  compose,
  createStore,
  Store,
} from "redux";
import { SessionDetailsPageConnected } from "./sessionDetailsConnected";
import { AppState, sagas, rootReducer } from "../store";
import { Route } from "react-router-dom";

const TEST_ID = "165f9819-2feb-40f0-0000-000000000001";

const history = createMemoryHistory({
  initialEntries: [`/session/${TEST_ID}`],
});

const routerReducer = connectRouter(history);
const sagaMiddleware = createSagaMiddleware();

const store: Store<AppState> = createStore(
  combineReducers({
    router: routerReducer,
    adminUI: rootReducer,
  }),
  compose(
    applyMiddleware(sagaMiddleware, routerMiddleware(history)),
    (window as any).__REDUX_DEVTOOLS_EXTENSION__ &&
      (window as any).__REDUX_DEVTOOLS_EXTENSION__(),
  ),
);

sagaMiddleware.run(sagas);

storiesOf("Sessions Deateils Page Connected", module)
  .addDecorator(storyFn => (
    <Provider store={store}>
      <ConnectedRouter history={history}>
        <Route path={"/session/:session"}>{storyFn()}</Route>
      </ConnectedRouter>
    </Provider>
  ))
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => <SessionDetailsPageConnected />);
