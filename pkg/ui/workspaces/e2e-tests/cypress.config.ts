import { defineConfig } from "cypress";

const TEAMCITY_OVERRIDES: Partial<Cypress.UserConfigOptions> = {
  // Don't remove other artifacts, in case other job phases included some already.
  trashAssetsBeforeRuns: false,
  downloadsFolder: "/artifacts/cypress/downloads",
  screenshotsFolder: "/artifacts/cypress/screenshots",
  videosFolder: "/artifacts/cypress/videos",
};

export default defineConfig({
  e2e: {
    baseUrl: "http://localhost:8080",
    setupNodeEvents(on, config) {
      // Implement custom node event listeners here
    },
    // Override some settings when running in TeamCity
    ...(
      process.env["TEAMCITY_VERSION"] && process.env["IS_DOCKER"]
        ? TEAMCITY_OVERRIDES
        : {}
    ),
  },
});
