// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useEffect } from "react";
import { useSelector } from "react-redux";
import { useHistory, useLocation } from "react-router-dom";

import { selectLoginState, getLoginPage } from "src/redux/login";
import { AdminUIState } from "src/redux/state";

const RequireLogin: React.FC<{ children?: React.ReactNode }> = ({
  children,
}) => {
  const loginState = useSelector((state: AdminUIState) =>
    selectLoginState(state),
  );
  const history = useHistory();
  const location = useLocation();

  const shouldHideLoginPage = loginState.hideLoginPage();

  useEffect(() => {
    if (!shouldHideLoginPage) {
      history.push(getLoginPage(location));
    }
  }, [shouldHideLoginPage, history, location]);

  if (!shouldHideLoginPage) {
    return null;
  }

  return <>{children}</>;
};

export default RequireLogin;
