// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { defineConfig } from "cypress";

const DOCKER_OVERRIDES: Partial<Cypress.UserConfigOptions["e2e"]> = {
  baseUrl: "https://crdbhost:8080",
  reporter: "teamcity",
  downloadsFolder: "/artifacts/cypress/downloads",
  screenshotsFolder: "/artifacts/cypress/screenshots",
  videosFolder: "/artifacts/cypress/videos",
};

export default defineConfig({
  e2e: {
    baseUrl: "http://localhost:8080",
    setupNodeEvents(on, config) {
      config.env.username = "cypress";
      config.env.password = "tests";
      return config;
    },
    // Override some settings when running in Docker
    ...(process.env.IS_DOCKER ? DOCKER_OVERRIDES : {}),
  },
});
