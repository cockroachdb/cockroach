// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import React from "react";
import * as ReactDOM from "react-dom";

import "src/polyfills";
import "src/protobufInit";
import { alertDataSync } from "src/redux/alerts";
import { App } from "src/app";
import { store, history } from "src/redux/state";
import "src/redux/analytics";
import {
  fetchDataFromServer,
  setDataFromServer,
} from "src/util/dataFromServer";
import ErrorBoundary from "src/views/app/components/errorMessage/errorBoundary";

fetchDataFromServer()
  .then(d => {
    setDataFromServer(d);

    ReactDOM.render(
      <App history={history} store={store} />,
      document.getElementById("react-layout"),
    );

    store.subscribe(alertDataSync(store));
  })
  .catch(e => {
    window.errorFromServer = "Failed to load init data from CRDB.";
    console.log(window.errorFromServer, e);
    ReactDOM.render(
      <ErrorBoundary></ErrorBoundary>,
      document.getElementById("react-layout"),
    );
  });
