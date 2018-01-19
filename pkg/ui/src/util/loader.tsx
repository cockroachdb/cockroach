import React from "react";

import spinner from "assets/spinner.gif";

interface LoaderProps<T> {
  load: () => Promise<T>;
  children: (data: T, handleRefresh: () => void) => JSX.Element;
}

interface LoaderState<T> {
  state: string;
  data?: T;
  error?: any;
}

/**
 * Loader handles data loading. Give it a data loading function and a render
 * function (`load` and `children`, respectively), and it will:
 * - render a spinner while data is loading.
 * - render an error when the API request returns one.
 * - render the data using the provided function when the API call returns.
 *
 * The render function is also passsed a `handleRefresh` function, which can
 * be used as an onClick handler to make a refresh button.
 */
class Loader<T> extends React.Component<LoaderProps<T>, LoaderState<T>> {

  constructor(props: LoaderProps<T>) {
    super(props);
    this.state = {
      state: "INIT",
    };
  }

  handleRefresh = () => {
    this.setState({
      state: "LOADING",
    });
    this.props.load()
      .then((resp) => {
        this.setState({
          state: "LOADED",
          data: resp,
        });
      })
      .catch((err) => {
        this.setState({
          state: "ERROR",
          error: err,
        });
      });
  }

  componentDidMount() {
    this.handleRefresh();
  }

  render() {
    switch (this.state.state) {
      case "INIT":
      case "LOADING":
        return (
          <div>
            <img className="visualization__spinner" src={spinner} />
          </div>
        );
      case "ERROR":
        return (
          <div>
            <h2>Error</h2>
            <pre>
              {JSON.stringify(this.state.error, null, 2)}
            </pre>
          </div>
        );
      case "LOADED":
        return this.props.children(this.state.data, this.handleRefresh);
      default:
        throw new Error("unknown state: " + this.state.state);
    }
  }

}

export default Loader;
