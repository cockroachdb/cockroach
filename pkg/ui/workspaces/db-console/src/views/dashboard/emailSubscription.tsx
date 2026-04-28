// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useCluster } from "@cockroachlabs/cluster-ui";
import React, { useEffect } from "react";
import { useSelector, useStore } from "react-redux";

import { emailSubscriptionAlertLocalSetting } from "src/redux/alerts";
import { signUpEmailSubscription } from "src/redux/customAnalytics";
import { AdminUIState } from "src/redux/state";
import {
  loadUIData,
  RELEASE_NOTES_SIGNUP_DISMISSED_KEY,
  saveUIData,
} from "src/redux/uiData";
import { dismissReleaseNotesSignupForm } from "src/redux/uiDataSelectors";
import { EmailSubscriptionForm } from "src/views/shared/components/emailSubscriptionForm";

import "./emailSubscription.scss";

const EmailSubscription: React.FC = () => {
  const store = useStore<AdminUIState>();
  const isHiddenPanel = useSelector((state: AdminUIState) =>
    dismissReleaseNotesSignupForm(state),
  );
  const { data: clusterData } = useCluster();
  const clusterId = clusterData?.cluster_id ?? "";

  useEffect(() => {
    loadUIData(store.dispatch, store.getState, RELEASE_NOTES_SIGNUP_DISMISSED_KEY);
    return () => {
      store.dispatch(emailSubscriptionAlertLocalSetting.set(false));
    };
  }, [store]);

  const handleEmailSubscriptionSubmit = (email: string) => {
    signUpEmailSubscription(clusterId, email, store.dispatch);
  };

  const handlePanelHide = () => {
    store.dispatch(emailSubscriptionAlertLocalSetting.set(false));
    saveUIData(store.dispatch, store.getState, {
      key: RELEASE_NOTES_SIGNUP_DISMISSED_KEY,
      value: true,
    });
  };

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
          <EmailSubscriptionForm onSubmit={handleEmailSubscriptionSubmit} />
        </div>
        <div
          onClick={handlePanelHide}
          className="crl-email-subscription__close-button"
        >
          &times;
        </div>
      </div>
    </section>
  );
};

export default EmailSubscription;
