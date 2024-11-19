// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import Helmet from "react-helmet";

import "./errorMessage.styl";
import NotFoundImg from "assets/not-found.svg";

function NotFound() {
  return (
    <main className="error-message-page">
      <Helmet title="Not Found" />
      <div className="error-message-page__content">
        <img
          className="error-message-page__img"
          src={NotFoundImg}
          alt="404 Error"
        />
        <div className="error-message-page__body">
          <div className="error-message-page__message">Whoops!</div>
          <p>
            We can&apos;t find the page you are looking for. You may have typed
            the wrong address or found a broken link.
          </p>
        </div>
      </div>
    </main>
  );
}

export default NotFound;
