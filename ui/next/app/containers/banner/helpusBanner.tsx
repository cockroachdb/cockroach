/// <reference path="../../../typings/index.d.ts" />
import * as React from "react";
import { connect } from "react-redux";
import { Link, RouterOnContext } from "react-router";
import _ = require("lodash");

import Banner from "./banner";
import { KEY_HELPUS, OptInAttributes, KeyValue, loadUIData, saveUIData } from "../../redux/uiData";

class HelpusBannerProps {
  attributes: OptInAttributes;
  attributesLoaded: boolean;
  loadUIData: (...keys: string[]) => void;
  saveUIData: (...values: KeyValue[]) => void;
}

class HelpusBannerState {
  dismissed: boolean = false;
}

interface HelpusBannerContext {
  router: RouterOnContext;
}

class HelpusBanner extends React.Component<HelpusBannerProps, HelpusBannerState> {
  static contextTypes: React.ValidationMap<any> = {
    router: React.PropTypes.object,
  };

  state = new HelpusBannerState();
  context: HelpusBannerContext;

  componentWillMount() {
    if (this.context && this.context.router && this.context.router.isActive("/help-us/reporting")) {
      this.setState({ dismissed: true });
    } else {
      this.props.loadUIData(KEY_HELPUS);
    }
  }

  componentWillReceiveProps(props: HelpusBannerProps) {
    if (this.context && this.context.router && this.context.router.isActive("/help-us/reporting")) {
      this.setState({ dismissed: true });
    }
  }

  dismiss = () => {
    this.setState({ dismissed: true });
    if (this.props.attributesLoaded) {
      let attributes = this.props.attributes || new OptInAttributes();
      attributes.dismissed = attributes.dismissed ? attributes.dismissed + 1 : 1;
      this.props.saveUIData({
        key: "helpus",
        value: attributes,
      });
    }
  }

  render() {
    let visible: boolean = this.props.attributesLoaded && !this.state.dismissed && (!this.props.attributes || (!this.props.attributes.dismissed && !this.props.attributes.optin));
    return <Banner className="helpus" visible={visible} onclose={this.dismiss}>
      <span className="checkmark">✓ </span>
      Help us improve. Opt in to share usage reporting statistics.
      <Link to="/help-us/reporting"><button onClick={this.dismiss}>Opt In</button></Link>
    </Banner>;
  }
}

let optinAttributes = (state: any): OptInAttributes => state && state.uiData && state.uiData.data && state.uiData.data[KEY_HELPUS];
let attributesLoaded = (state: any): boolean => state && state.uiData && state.uiData.data && _.has(state.uiData.data, KEY_HELPUS);

// Connect the HelpUsBanner class with our redux store.
let helpusBannerConnected = connect(
  (state: any, ownProps: any) => {
    return {
      attributes: optinAttributes(state),
      attributesLoaded: attributesLoaded(state),
    };
  },
  {
    loadUIData,
    saveUIData,
  }
)(HelpusBanner);

export default helpusBannerConnected;
