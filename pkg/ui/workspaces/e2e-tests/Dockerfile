# WARNING
# This Dockerfile must be built from the root of cockroachdb after the cockroach
# binary has been built. Running it from other directories will cause errors.
FROM cypress/browsers:node16.14.2-slim-chrome100-ff99-edge

# Add the compiled CRDB binary
ADD ./cockroach /usr/bin/cockroach

# Switch to the non-root user
USER node

# Install e2e test dependencies during the build phase
WORKDIR /cypress
ADD ./pkg/ui/workspaces/e2e-tests/package.json ./pkg/ui/workspaces/e2e-tests/yarn.lock ./
RUN yarn install --ignore-scripts

# Run tests during the execution phase, assuming pkg/ui/workspaces/e2e-tests was
# mounted to /cypress during the build. This allows the dependencies to be
# cached within the built container, but the tests to be iterated on with
# minimal overhead.
CMD COCKROACH=/usr/bin/cockroach yarn test
