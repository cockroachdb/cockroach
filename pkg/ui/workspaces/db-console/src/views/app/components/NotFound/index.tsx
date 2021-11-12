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
import Helmet from "react-helmet";
import "./notFound.styl";
import NotFoundImg from "assets/not-found.svg";

function NotFound() {
  return (
    <main className="not-found-page">
      <Helmet title="Not Found" />
      <div className="not-found-page__content">
        <img
          className="not-found-page__img"
          src={NotFoundImg}
          alt="404 Error"
        />
        <div className="not-found-page__body">
          <div className="not-found-page__message">Whoops!</div>
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
