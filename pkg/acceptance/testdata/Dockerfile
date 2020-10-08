FROM ubuntu:18.04

# This Dockerfile bundles several language runtimes and test frameworks into one
# container for our acceptance tests.
#
# Here are some tips for keeping this file maintainable.
#
#   - When possible, keep commands that are slow to rebuild towards the top of
#     the file and keep commands that change frequently towards the bottom of
#     the file. Changing a command invalidates the build cache for all following
#     commands.
#
#   - Group logical stages into one RUN command, but not when it groups a slow
#     step that's unlikely to change with a step that's likely to change
#     frequently.
#
#   - Follow the visual indentation scheme.
#
#   - Don't cargo cult installation instructions from the web. There are many
#     ways to add a new APT repository, for example; we should only use one.
#     There are many ways to invoke curl and tar; we should be consistent.
#
#   - Store artifacts in /opt rather than cluttering the root directory.
#
#   - Prefer packages from APT to packages from each language's package
#     managers.

RUN apt-get update \
 && apt-get install --yes --no-install-recommends ca-certificates curl

# The forward reference version is the oldest version from which we support
# upgrading. The bidirectional reference version is the oldest version that we
# support upgrading from and downgrading to.
ENV FORWARD_REFERENCE_VERSION="v2.0.0"
ENV BIDIRECTIONAL_REFERENCE_VERSION="v2.0.0"
RUN mkdir /opt/forward-reference-version /opt/bidirectional-reference-version \
 && curl -fsSL https://binaries.cockroachdb.com/cockroach-${FORWARD_REFERENCE_VERSION}.linux-amd64.tgz \
    | tar xz -C /opt/forward-reference-version --strip-components=1 \
 && curl -fsSL https://binaries.cockroachdb.com/cockroach-${BIDIRECTIONAL_REFERENCE_VERSION}.linux-amd64.tgz \
    | tar xz -C /opt/bidirectional-reference-version --strip-components=1

RUN apt-get install --yes --no-install-recommends openjdk-8-jdk \
 && curl -fsSL https://github.com/cockroachdb/finagle-postgres/archive/94b1325270.tar.gz | tar xz \
 && cd finagle-postgres-* \
 && ./sbt assembly \
 && mv target/scala-2.11/finagle-postgres-tests.jar /opt/finagle-postgres-tests.jar \
 && rm -rf /finagle-postgres-* ~/.ivy2

RUN curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg > /etc/apt/trusted.gpg.d/yarn.asc \
 && echo "deb https://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc > /etc/apt/trusted.gpg.d/microsoft.asc \
 && echo "deb https://packages.microsoft.com/repos/microsoft-ubuntu-bionic-prod/ bionic main" > /etc/apt/sources.list.d/microsoft.list \
 && apt-get install --yes --no-install-recommends gnupg \
 && curl https://packages.erlang-solutions.com/erlang-solutions_2.0_all.deb > erlang-solutions_2.0_all.deb && dpkg -i erlang-solutions_2.0_all.deb \
 && apt-get update \
 && apt-get install --yes --no-install-recommends \
    dotnet-sdk-2.1 \
    dotnet-runtime-2.1 \
    expect \
    elixir \
    esl-erlang \
    libc6-dev \
    libcurl4 \
    libpq-dev \
    libpqtypes-dev \
    make \
    maven \
    nodejs \
    gcc \
    golang \
    php-cli \
    php-pgsql \
    postgresql-client \
    python \
    python-psycopg2 \
    ruby \
    ruby-pg \
    xmlstarlet \
    yarn

RUN curl -fsSL https://github.com/benesch/autouseradd/releases/download/1.0.0/autouseradd-1.0.0-amd64.tar.gz \
    | tar xz -C /usr --strip-components 1

# When system packages are not available for a language's PostgreSQL driver,
# fall back to using that language's package manager. The high-level process
# looks like this:
#
#   1. Configure the package manager to install dependencies into a system-wide
#      location (/var/lib/...) instead of a user-specific location (/root/...).
#      /root is not world-readable, and we want the packages to be available to
#      non-root users.
#
#   2. Instruct the package manager to install the dependencies by reading
#      whatever language-specific package manifest exists in testdata/LANG.
#
#   3. Either remove the package manager entirely, if it's not used to drive
#      tests, or put it into offline mode. This ensures that future dependencies
#      get baked into the container, lest we accidentally introduce a CI-time
#      dependency on the remote package repository.

COPY . /testdata

# Handle C# dependencies using the NuGet package manager.
ENV DOTNET_CLI_TELEMETRY_OPTOUT=1 NUGET_PACKAGES=/var/lib/nuget/packages
RUN (cd /testdata/csharp && dotnet restore --no-cache) \
 && xmlstarlet ed --inplace \
    --subnode /configuration --type elem --name packageRestore \
    --subnode '$prev' --type elem --name add \
    --insert '$prev' --type attr --name key --value enabled \
    --insert '$prev' --type attr --name value --value false \
    --delete /configuration/packageSources \
    ~/.nuget/NuGet/NuGet.Config

# Handle Java dependencies using the Maven package manager.
RUN xmlstarlet ed --inplace \
    --subnode /_:settings --type elem --name localRepository --value /var/lib/maven/repository \
    /usr/share/maven/conf/settings.xml \
 && (cd /testdata/java && mvn -Dtest=NoopTest dependency:go-offline package) \
 && xmlstarlet ed --inplace \
    --subnode /_:settings --type elem --name offline --value true \
    /usr/share/maven/conf/settings.xml

# Handle Node dependencies using the Yarn package manager.
RUN (cd /testdata/node && yarn install && mv node_modules /usr/lib/node) \
 && apt-get purge --yes yarn

RUN apt-get purge --yes \
    curl \
    xmlstarlet \
 && rm -rf /tmp/* /var/lib/apt/lists/* /testdata
