// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { ComponentType } from "react";
import { Location } from "history";
import { StaticContext } from "react-router";
import { RouteComponentProps } from "react-router-dom";

export interface WithNavigationBackProps {
  navigateBack: () => void;
}

export interface LocationState {
  from?: Location;
}

export const withNavigationBack = (fallbackPath?: string) =>
  <P extends RouteComponentProps<{}, StaticContext, LocationState>>(WrappedComponent: ComponentType<P & WithNavigationBackProps>) => {
    return class extends React.Component<P> {
      componentWillUnmount() {
        const { location } = this.props.history;
        if (location.state?.from) {
          location.state.from = undefined;
        }
      }

      navigateBack = () => {
        const { push, location, goBack } = this.props.history;
        if (location.state?.from) {
          push(location.state.from);
        } else if (fallbackPath) {
          push(fallbackPath);
        } else {
          goBack();
        }
      }

      render() {
        return <WrappedComponent {...this.props} navigateBack={this.navigateBack} />;
      }
    };
  };
