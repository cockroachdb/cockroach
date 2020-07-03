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
import { chain } from "lodash";
import { RequestError } from "src/util/api";
import spinner from "assets/spinner.gif";
import { adminUIAccess } from "src/util/docs";
import "./index.styl";
import { InlineAlert, InlineAlertProps, InlineAlertIntent } from "src/components/inlineAlert/inlineAlert";
import { Anchor } from "src/components";

interface LoadingProps {
  loading: boolean;
  error?: Error | Error[] | null;
  className?: string;
  image?: string;
  render: () => any;
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

function isRestrictedPermissionsError(error: Error) {
  return error instanceof RequestError && error.status === 403;
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
    // - map Error to InlineAlert props. RestrictedPermissions handled as "info" message;
    // - group errors by intend to show separate alerts per intent.
    return chain(errors)
      .map<Omit<InlineAlertProps, "title">>(error => {
        const isRestrictedPermissionError = isRestrictedPermissionsError(error);

        if (isRestrictedPermissionError) {
          return {
            intent: "info",
            message: (
              <span>
                You do not have permissions to view this information. <Anchor href={adminUIAccess}>Learn more</Anchor>
              </span>
            ),
          };
        } else {
          return {
            intent: "error",
            message: <span>{error.message}: no details available</span>,
          };
        }
      })
      .groupBy(alert => alert.intent)
      .map((alerts, intent: InlineAlertIntent) => {
        if (alerts.length === 1) {
          return (
            <InlineAlert
              intent={intent}
              title={alerts[0].message}
            />
          );
        } else {
          return (
            <InlineAlert
              intent={intent}
              title={
                <p>Multiple errors occurred while loading this data:</p>
              }
              message={
                <div>
                  { alerts.map((alert, idx) => (<p key={idx}>{alert.message}</p>)) }
                </div>
              }
            />
          );
        }
      })
      .value();
  }
  if (props.loading) {
    return <div className={className} style={image} />;
  }
  return props.render();
}
