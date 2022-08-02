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
import { readFileSync } from "fs";

const DOCKER_OVERRIDES: Partial<Cypress.UserConfigOptions["e2e"]> = {
  baseUrl: "https://crdbhost:8080",
  downloadsFolder: "/artifacts/cypress/downloads",
  screenshotsFolder: "/artifacts/cypress/screenshots",
  videosFolder: "/artifacts/cypress/videos",
};

export default defineConfig({
  e2e: {
    baseUrl: "http://localhost:8080",
    setupNodeEvents(on, config) {
      if (process.env.IS_DOCKER) {
        config.env.username = "cypress";
        config.env.password = "tests";
        return config;
      }

      // Implement custom node event listeners here
      const connUrlFile = process.env.CONN_URL_FILE;
      if (!connUrlFile) {
        throw new Error(
          "Unable to find default username and password: $CONN_URL_FILE not provided",
        );
      }

      try {
        const connUrlStr = readFileSync(connUrlFile, "utf-8");
        const connUrl = new URL(connUrlStr);
        config.env.username = connUrl.username;
        config.env.password = connUrl.password;
        console.log("connUrl = ", connUrl);
        return config;
      } catch (err) {
        console.error(
          `Unable to read default username and password from ${connUrlFile}:`,
          err,
        );
        throw err;
      }
    },
    // Override some settings when running in Docker
    ...(process.env.IS_DOCKER ? DOCKER_OVERRIDES : {}),
  },
});
