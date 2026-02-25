// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";

import { emailSubscriptionAlertLocalSetting } from "src/redux/alerts";
import { signUpForEmailSubscription } from "src/redux/customAnalytics";
import { clusterIdSelector } from "src/redux/nodes";
import { AdminUIState, AppDispatch } from "src/redux/state";
import {
  loadUIData,
  RELEASE_NOTES_SIGNUP_DISMISSED_KEY,
  saveUIData,
} from "src/redux/uiData";
import { dismissReleaseNotesSignupForm } from "src/redux/uiDataSelectors";
import { EmailSubscriptionForm } from "src/views/shared/components/emailSubscriptionForm";

import "./emailSubscription.scss";

const EmailSubscription: React.FC = () => {
  const dispatch: AppDispatch = useDispatch();
  const isHiddenPanel = useSelector((state: AdminUIState) =>
    dismissReleaseNotesSignupForm(state),
  );
  const clusterId = useSelector((state: AdminUIState) =>
    clusterIdSelector(state),
  );

  useEffect(() => {
    dispatch(loadUIData(RELEASE_NOTES_SIGNUP_DISMISSED_KEY));
    return () => {
      dispatch(emailSubscriptionAlertLocalSetting.set(false));
    };
  }, [dispatch]);

  const handleEmailSubscriptionSubmit = (email: string) => {
    dispatch(signUpForEmailSubscription(clusterId, email));
  };

  const handlePanelHide = () => {
    dispatch(emailSubscriptionAlertLocalSetting.set(false));
    dispatch(
      saveUIData({
        key: RELEASE_NOTES_SIGNUP_DISMISSED_KEY,
        value: true,
      }),
    );
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
