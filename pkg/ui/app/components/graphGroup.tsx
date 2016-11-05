import _ from "lodash";
import * as React from "react";
import { IInjectedProps, IRouter } from "react-router";
import { connect } from "react-redux";

import { MetricsDataProvider } from "../containers/metricsDataProvider";
import { dashQueryString } from "../util/constants";

interface GraphGroupProps {
  groupId: string;
  childClassName?: string;
  default?: boolean; // True if this graph group is shown by default.
}

class GraphGroupState {
  visibleID: string;
}

/**
 * GraphGroup is a react component that wraps a group of graphs (the
 * children of this component) in a MetricsDataProvider and some additional tags
 * relevant to the layout of our graphs pages. The graphgroup is hidden unless
 * it is either the default graph group or its groupId appears in the URL query
 * string.
 */
class GraphGroup extends React.Component<GraphGroupProps, GraphGroupState> {
  // Magic to add react router to the context.
  // See https://github.com/ReactTraining/react-router/issues/975
  static contextTypes = {
    router: React.PropTypes.object.isRequired,
  };
  context: { router?: IRouter & IInjectedProps; };

  state = new GraphGroupState();

  componentWillMount() {
    this.propsChange();
  }

  componentWillReceiveProps(props: GraphGroupProps) {
    this.propsChange(props);
  }

  propsChange(props = this.props) {
    let query: any = this.context.router.location.query;
    // Only show if the groupID is in the query string;
    if (_.has(query, dashQueryString)) {
      this.setState({ visibleID: query.dash});
    // Otherwise use the default value or, barring that, the first value.
    } else {
      this.setState({ visibleID: null });
    }
  }

  render() {
    return <div>
    {
      React.Children.map(this.props.children, (child: any, idx: number) => {
        let key = this.props.groupId + idx.toString();
        if (this.state.visibleID ? this.state.visibleID !== this.props.groupId : !this.props.default) {
          return null;
        }

        // Special case h2 tags which are used as the graph group title.
        if (child.type === "h2") {
          return <div>{ child }</div>;
        }
        return <div style={{float:"left"}} key={key} className={ this.props.childClassName || "" }>
          <MetricsDataProvider id={key}>
            { child }
          </MetricsDataProvider>
        </div>;
      })
    }
    </div>;
  }
}

export default connect()(GraphGroup);
