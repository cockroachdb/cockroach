import React from "react";

import { CachedDataReducerState } from "src/redux/cachedDataReducer";

import spinner from "assets/spinner.gif";

interface LoadingProps<T> {
  data: CachedDataReducerState<T>;
  className: string;
  children: (data: T) => React.ReactNode;
}

// *
// * Loading will display a background image instead of the content if the
// * loading prop is true.
// *
export default function buildLoading<T>() {
  return class Loading extends React.Component<LoadingProps<T>> {
    render() {
      const loading = !(this.props.data && this.props.data.data);

      const image = {
        "backgroundImage": `url(${spinner})`,
      };
      if (loading) {
        return <div className={this.props.className} style={image} />;
      }

      return this.props.children(this.props.data.data);
    }
  };
}
