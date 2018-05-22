import React from "react";
import { Route } from "react-router";

import LoginPage from "src/views/login/loginPage";

export const LOGIN_PAGE = "/login";

export default function(): JSX.Element {
  return (
    <Route path={LOGIN_PAGE} component={ LoginPage } />
  );
}
