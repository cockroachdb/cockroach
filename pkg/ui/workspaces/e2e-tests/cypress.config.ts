// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
    video: true,
    setupNodeEvents(on, config) {
      config.env.username = "cypress";
      config.env.password = "tests";
      return config;
    },
    // Override some settings when running in Docker
    ...(process.env.IS_DOCKER ? DOCKER_OVERRIDES : {}),
  },
});
