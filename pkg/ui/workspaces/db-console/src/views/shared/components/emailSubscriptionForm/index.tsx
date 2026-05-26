// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useState } from "react";

import { TextInput, Button } from "src/components";
import { isValidEmail } from "src/util/validation/isValidEmail";

import "./emailSubscriptionForm.scss";

interface EmailSubscriptionFormProps {
  onSubmit?: (emailAddress: string) => void;
}

export function EmailSubscriptionForm({
  onSubmit,
}: EmailSubscriptionFormProps): React.ReactElement {
  const [emailAddress, setEmailAddress] = useState<string | undefined>(
    undefined,
  );
  const [canSubmit, setCanSubmit] = useState(false);

  const handleEmailValidation = (value: string) => {
    const isCorrectEmail = isValidEmail(value);
    const isEmpty = value.length === 0;

    setCanSubmit(isCorrectEmail && !isEmpty);

    if (isCorrectEmail || isEmpty) {
      return undefined;
    }
    return "Invalid email address.";
  };

  const handleChange = (value: string) => {
    handleEmailValidation(value);
    setEmailAddress(value);
  };

  const handleSubmit = () => {
    if (canSubmit) {
      onSubmit(emailAddress);
      setEmailAddress("");
      setCanSubmit(false);
    }
  };

  return (
    <div className="email-subscription-form">
      <TextInput
        name="email"
        className="email-subscription-form__input"
        placeholder="Enter your email"
        validate={handleEmailValidation}
        onChange={handleChange}
        value={emailAddress}
      />
      <Button
        type={"primary"}
        onClick={handleSubmit}
        disabled={!canSubmit}
        className="email-subscription-form__submit-button"
      >
        Sign up
      </Button>
    </div>
  );
}
