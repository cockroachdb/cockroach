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

import spinner from "assets/spinner.gif";
import "./index.styl";

interface LoadingProps {
  loading: boolean;
  error?: Error | Error[] | null;
  className?: string;
  image?: string;
  render: () => React.ReactNode;
}

/**
 * getValidErrorsList eliminates any null Error values, and returns either
 * null or a non-empty list of Errors.
 */
function getValidErrorsList (errors?: Error | Error[] | null): Error[] | null {
  if (errors) {
    if (!Array.isArray(errors)) {
      // Put single Error into a list to simplify logic in main Loading component.
      return [errors];
    } else {
      // Remove null values from Error[].
      const validErrors = errors.filter(e => !!e);
      if (validErrors.length === 0) {
        return null;
      }
      return validErrors;
    }
  }
  return null;
}

/**
 * Loading will display a background image instead of the content if the
 * loading prop is true.
 */
export default function Loading(props: LoadingProps) {
  const className = props.className || "loading-image loading-image__spinner";
  const imageURL = props.image || spinner;
  const image = {
    "backgroundImage": `url(${imageURL})`,
  };

  const errors = getValidErrorsList(props.error);

  // Check for `error` before `loading`, since tests for `loading` often return
  // true even if CachedDataReducer has an error and is no longer really "loading".
  if (errors) {
    const errorCountMessage = (errors.length > 1) ? "Multiple errors occurred" : "An error was encountered";
    return (
      <div className="loading-error">
        <p>{errorCountMessage} while loading this data:</p>
        <ul>
          {errors.map((error, idx) => (
            <li key={idx}>
              <pre>{error.message}</pre>
            </li>
          ))}
        </ul>
      </div>
    );
  }
  if (props.loading) {
    return <div className={className} style={image} />;
  }
  return (
    <React.Fragment>
      {props.render()}
    </React.Fragment>
  );
}
