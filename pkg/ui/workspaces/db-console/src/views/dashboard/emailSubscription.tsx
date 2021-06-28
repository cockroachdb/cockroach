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

import "./emailSubscription.styl";
import {
  loadUIData,
  RELEASE_NOTES_SIGNUP_DISMISSED_KEY,
  saveUIData,
} from "src/redux/uiData";
import { dismissReleaseNotesSignupForm } from "src/redux/uiDataSelectors";
import { emailSubscriptionAlertLocalSetting } from "src/redux/alerts";

type EmailSubscriptionProps = MapDispatchToProps & MapStateToProps;

class EmailSubscription extends React.Component<EmailSubscriptionProps> {
  componentDidMount() {
    this.props.refresh();
  }

  handleEmailSubscriptionSubmit = (email: string) => {
    this.props.signUpForEmailSubscription(this.props.clusterId, email);
  };

  handlePanelHide = () => {
    this.props.dismissAlertMessage();
    this.props.hidePanel();
  };

  componentWillUnmount() {
    this.props.dismissAlertMessage();
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
              Keep up-to-date with CockroachDB software releases and best
              practices.
            </div>
          </div>
          <div className="crl-email-subscription__controls">
            <EmailSubscriptionForm
              onSubmit={this.handleEmailSubscriptionSubmit}
            />
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

interface MapDispatchToProps {
  signUpForEmailSubscription: (clusterId: string, email: string) => void;
  hidePanel: () => void;
  refresh: () => void;
  dismissAlertMessage: () => void;
}

const mapDispatchToProps = {
  signUpForEmailSubscription,
  refresh: () => loadUIData(RELEASE_NOTES_SIGNUP_DISMISSED_KEY),
  hidePanel: () => {
    return saveUIData({
      key: RELEASE_NOTES_SIGNUP_DISMISSED_KEY,
      value: true,
    });
  },
  dismissAlertMessage: () => emailSubscriptionAlertLocalSetting.set(false),
};

interface MapStateToProps {
  isHiddenPanel: boolean;
  clusterId: string;
}
const mapStateToProps = (state: AdminUIState) => ({
  isHiddenPanel: dismissReleaseNotesSignupForm(state),
  clusterId: clusterIdSelector(state),
});

export default connect(mapStateToProps, mapDispatchToProps)(EmailSubscription);
