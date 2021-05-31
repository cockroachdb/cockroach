// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";
import { chain } from "lodash";
import {
  InlineAlert,
  InlineAlertProps,
  Spinner,
  IconIntent,
} from "@cockroachlabs/ui-components";
import { adminUIAccess, isForbiddenRequestError } from "src/util";
import styles from "./loading.module.scss";
import { Anchor } from "../anchor";

interface LoadingProps {
  loading: boolean;
  error?: Error | Error[] | null;
  className?: string;
  image?: string;
  render: () => any;
  errorClassName?: string;
  loadingClassName?: string;
}

const cx = classNames.bind(styles);

/**
 * getValidErrorsList eliminates any null Error values, and returns either
 * null or a non-empty list of Errors.
 */
function getValidErrorsList(errors?: Error | Error[] | null): Error[] | null {
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
export const Loading: React.FC<LoadingProps> = props => {
  const errors = getValidErrorsList(props.error);

  // Check for `error` before `loading`, since tests for `loading` often return
  // true even if CachedDataReducer has an error and is no longer really "loading".
  if (errors) {
    // - map Error to InlineAlert props. RestrictedPermissions handled as "info" message;
    // - group errors by intend to show separate alerts per intent.
    const errorAlerts = chain(errors)
      .map<Omit<InlineAlertProps, "title">>(error => {
        if (isForbiddenRequestError(error)) {
          return {
            intent: "info",
            description: (
              <span>
                {`${error.name}: ${error.message}`}{" "}
                <Anchor href={adminUIAccess}>Learn more</Anchor>
              </span>
            ),
          };
        } else {
          return {
            intent: "danger",
            description: <span>{error.message}</span>,
          };
        }
      })
      .groupBy(alert => alert.intent)
      .map((alerts, intent: IconIntent) => {
        if (alerts.length === 1) {
          return <InlineAlert intent={intent} title={alerts[0].description} />;
        } else {
          return (
            <InlineAlert
              intent={intent}
              title={<p>Multiple errors occurred while loading this data:</p>}
              description={
                <div>
                  {alerts.map((alert, idx) => (
                    <p key={idx}>{alert.description}</p>
                  ))}
                </div>
              }
            />
          );
        }
      })
      .value();

    return (
      <div className={cx("alerts-container", props.errorClassName)}>
        {React.Children.toArray(errorAlerts)}
      </div>
    );
  }
  if (props.loading) {
    return (
      <Spinner className={cx("loading-indicator", props.loadingClassName)} />
    );
  }
  return props.render();
};
