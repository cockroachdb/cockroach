// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { LoginAPIState } from "oss/src/redux/login";
import { Button } from "src/components";

const OIDC_LOGIN_PATH = "oidc/v1/login";
const OIDC_LOGIN_PATH_WITH_JWT = "oidc/v1/login?jwt";

const OIDCLoginButton = ({ loginState }: { loginState: LoginAPIState }) => {
  return (
    <a href={OIDC_LOGIN_PATH}>
      <Button
        type="secondary"
        className="submit-button-oidc"
        disabled={loginState.inProgress}
        textAlign={"center"}
      >
        {loginState.oidcButtonText}
      </Button>
    </a>
  );
};

const OIDCLogin: React.FC<
  {
    loginState: LoginAPIState;
  } & RouteComponentProps
> = props => {
  const oidcAutoLoginQuery = new URLSearchParams(props.location.search).get(
    "oidc_auto_login",
  );
  if (props.loginState.oidcLoginEnabled) {
    if (props.loginState.oidcAutoLogin && !(oidcAutoLoginQuery === "false")) {
      window.location.replace(OIDC_LOGIN_PATH);
    }
    return <OIDCLoginButton loginState={props.loginState} />;
  }
  return null;
};

const OIDCGenerateJWTAuthToken: React.FC<
  {
    loginState: LoginAPIState;
  } & RouteComponentProps
> = props => {
  if (props.loginState.oidcGenerateJWTAuthTokenEnabled) {
    return (
      <a href={OIDC_LOGIN_PATH_WITH_JWT}>
        <Button
          type="secondary"
          className="submit-button-oidc"
          disabled={props.loginState.inProgress}
          textAlign={"center"}
        >
          Generate JWT auth token for cluster SSO
        </Button>
      </a>
    );
  }
  return null;
};

export const OIDCLoginConnected = withRouter(OIDCLogin);
export const OIDCGenerateJWTAuthTokenConnected = withRouter(
  OIDCGenerateJWTAuthToken,
);
