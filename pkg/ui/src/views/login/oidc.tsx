// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

import { LoginAPIState } from "oss/src/redux/login";
import { Button, InlineAlert } from "src/components";
import { RouteComponentProps, withRouter } from "react-router-dom";

const OIDC_LOGIN_PATH = "/oidc/v1/login";

const OIDCLoginButton = ({loginState}: {loginState: LoginAPIState}) => {
  return (
    <a href={OIDC_LOGIN_PATH} >
      <Button type="secondary" className="submit-button-oidc" disabled={loginState.inProgress} textAlign={"center"}>
        {loginState.oidcButtonText}
      </Button>
    </a>
  );
};

const OIDCError = ({errorMessage, status}: {errorMessage: string, status: string}) => {
  if (errorMessage) {
    return <InlineAlert
      title={errorMessage}
      intent={parseInt(status, 10) < 500 ? "info" : "error"}
    />;
  }
  return null;
};

const OIDCLogin: React.FC<{loginState: LoginAPIState} & RouteComponentProps> = (props) => {
  const params = new URLSearchParams(props.location.search);
  const oidcAutoLoginQuery = params.get("oidc_auto_login");
  const oidcError = params.get("error");
  const oidcErrorStatus = params.get("status");
  if (props.loginState.oidcLoginEnabled) {
    if (props.loginState.oidcAutoLogin && !(oidcAutoLoginQuery === "false")) {
      window.location.replace(OIDC_LOGIN_PATH);
    }
    return (
      <>
        <OIDCLoginButton loginState={props.loginState} />
        <OIDCError errorMessage={oidcError} status={oidcErrorStatus}/>
      </>
    );
  }
  return null;
};

export const OIDCLoginConnected = withRouter(OIDCLogin);
