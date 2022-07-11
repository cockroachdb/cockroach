import { defineConfig } from "cypress";

export default defineConfig({
  e2e: {
    baseUrl: "http://localhost:8084",
    setupNodeEvents(on, config) {
      // Implement custom node event listeners here
    },
  },
});
