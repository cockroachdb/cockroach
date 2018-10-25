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
  error?: Error | null;
  className?: string;
  image?: string;
  render: () => React.ReactNode;
}

/**
 * Loading will display a background image instead of the content if the
 * loading prop is true.
 */
export default function Loading(props: LoadingProps) {
  const className = props.className || "loading-image loading-image__spinner-left";
  const imageURL = props.image || spinner;
  const image = {
    "backgroundImage": `url(${imageURL})`,
  };
  // Check for `error` before `loading`, since tests for `loading` often return
  // true even if CachedDataReducer has an error and is no longer really "loading".
  if (props.error) {
    return (
      <div className="loading-error">
        <p>An error was encountered while loading this data:</p>
        <pre>{props.error.message}</pre>
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
