// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
