// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { connect } from "react-redux";

import { EmailSubscriptionForm } from "src/views/shared/components/emailSubscriptionForm";
import { signUpForEmailSubscription } from "src/redux/customAnalytics";
import { AdminUIState } from "src/redux/state";
import { clusterIdSelector } from "src/redux/nodes";
import { LocalSetting } from "src/redux/localsettings";

import "./emailSubscription.styl";

type EmailSubscriptionProps = MapDispatchToProps & MapStateToProps;

class EmailSubscription extends React.Component<EmailSubscriptionProps> {
  handleEmailSubscriptionSubmit = (email: string) => {
    this.props.signUpForEmailSubscription(this.props.clusterId, email);
  }

  handlePanelHide = () => {
    this.props.hidePanel();
  }

  render() {
    const { isHiddenPanel } = this.props;

    if (isHiddenPanel) {
      return null;
    }

    return (
      <section className="section">
        <div className="crl-email-subscription">
          <div className="crl-email-subscription__text">
            <div>
              Keep up-to-date with CockroachDB
              software releases and best practices.
            </div>
          </div>
          <div className="crl-email-subscription__controls">
            <EmailSubscriptionForm onSubmit={this.handleEmailSubscriptionSubmit} />
          </div>
          <div
            onClick={this.handlePanelHide}
            className="crl-email-subscription__close-button"
          >
            &times;
          </div>
        </div>
      </section>
    );
  }
}

const hidePanelLocalSetting = new LocalSetting<AdminUIState, boolean>(
  "dashboard/release_notes_signup/hide", (s) => s.localSettings, false,
);

interface MapDispatchToProps {
  signUpForEmailSubscription: (clusterId: string, email: string) => void;
  hidePanel: () => void;
}

const mapDispatchToProps = {
  signUpForEmailSubscription,
  hidePanel: () => hidePanelLocalSetting.set(true),
};

interface MapStateToProps {
  isHiddenPanel: boolean;
  clusterId: string;
}
const mapStateToProps = (state: AdminUIState) => ({
  isHiddenPanel: hidePanelLocalSetting.selector(state),
  clusterId: clusterIdSelector(state),
});

export default connect(mapStateToProps, mapDispatchToProps)(EmailSubscription);
