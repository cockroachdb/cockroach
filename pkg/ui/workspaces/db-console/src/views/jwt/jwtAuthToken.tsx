// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect, useState } from "react";
import Helmet from "react-helmet";
import { useParams } from "react-router-dom";
import Select from "react-select";
import { Spinner } from "@cockroachlabs/ui-components";

import {
  Button,
  CockroachLabsLockupIcon,
  Text,
  TextInput,
  TextTypes,
} from "src/components";

import ErrorCircle from "assets/error-circle.svg";
import "./jwtAuthToken.styl";

const OIDC_LOGIN_PATH_WITH_JWT = "/oidc/v1/login?jwt";

type Params = {
  oidc: string;
};

export type ConnectionInfo = {
  Usernames: string[];
  Password: string;
  Host: string;
  Port: number;
  Expiry: Date;
};

export const JwtAuthToken = (props: {
  loading: boolean;
  error: string;
  info: ConnectionInfo;
}) => {
  const [username, setUsername] = useState(null);

  const renderLoading = () => {
    return <Spinner />;
  };

  const renderError = (error: string) => {
    return (
      <div>
        <div className="jwt-auth-token__error">
          <img src={ErrorCircle} alt={error} />
          {error}
        </div>
        {renderButton()}
      </div>
    );
  };

  const renderInfo = (info: ConnectionInfo) => {
    if (username == null) {
      setUsername(info.Usernames[0]);
    }

    const usernameOptions = info.Usernames.map(u => {
      return { label: u, value: u };
    });

    return (
      <div>
        <p>
          Use the following values to configure your client connection to the
          cluster.
        </p>
        <div className="crl-input__wrapper">
          <label htmlFor="username" className="crl-input__label">
            Username
          </label>
          <Select
            name="username"
            clearable={false}
            value={username}
            options={usernameOptions}
            onChange={(option: { value: string }) => setUsername(option.value)}
          />
        </div>
        <TextInput
          label={`Password (Expires ${info.Expiry}.)`}
          value={info.Password}
          initialValue={info.Password}
          onChange={() => {}}
        />
        <TextInput
          label="Host"
          value={info.Host}
          initialValue={info.Host}
          onChange={() => {}}
        />
        <TextInput
          label="Port"
          value={info.Port.toString()}
          initialValue={info.Port.toString()}
          onChange={() => {}}
        />
        <TextInput
          label="Options"
          value={"--crdb:jwt_auth_enabled=true"}
          initialValue={"--crdb:jwt_auth_enabled=true"}
          onChange={() => {}}
        />
        {renderButton()}
      </div>
    );
  };

  const renderButton = () => {
    return (
      <a href={OIDC_LOGIN_PATH_WITH_JWT}>
        <Button type="primary" className="submit-button" textAlign={"center"}>
          Generate another token
        </Button>
      </a>
    );
  };

  return (
    <div className="jwt-auth-token">
      <Helmet title="Cluster SSO" />
      <div className="jwt-auth-token__container">
        <CockroachLabsLockupIcon height={37} />
        <div className="content">
          <section className="jwt-auth-token__connection-params">
            <div className="connection-params-container">
              <Text textType={TextTypes.Heading2}>Cluster SSO</Text>
              {!!props.loading && renderLoading()}
              {!!props.error && renderError(props.error)}
              {!!props.info && renderInfo(props.info)}
            </div>
          </section>
          <section className="jwt-auth-token__documentation"></section>
        </div>
      </div>
    </div>
  );
};

export const JwtAuthTokenPage = () => {
  const [loading, setLoading] = useState<boolean>(true);
  const [info, setInfo] = useState<ConnectionInfo>(null);
  const [error, setError] = useState<string>(null);

  const { oidc } = useParams<Params>();

  useEffect(() => {
    const { State, Code } = JSON.parse(atob(oidc));

    fetch(`/oidc/v1/jwt?state=${State}&code=${Code}`).then(
      (response: Response) => {
        setLoading(false);
        if (response.ok) {
          response.json().then(setInfo);
        } else {
          response.text().then(setError);
        }
      },
    );
  }, [oidc]);

  return <JwtAuthToken loading={loading} error={error} info={info} />;
};
