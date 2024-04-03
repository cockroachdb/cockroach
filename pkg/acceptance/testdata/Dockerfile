FROM ubuntu:18.04
ARG TARGETPLATFORM

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

RUN case ${TARGETPLATFORM} in \
    "linux/amd64") URL=https://download.visualstudio.microsoft.com/download/pr/8159607a-e686-4ead-ac99-b4c97290a5fd/ec6070b1b2cc0651ebe57cf1bd411315/dotnet-sdk-6.0.401-linux-x64.tar.gz ;; \
    "linux/arm64") URL=https://download.visualstudio.microsoft.com/download/pr/a567a07f-af9d-451a-834c-a746ac299e6b/1d9d74b54cf580f93cad71a6bf7b32be/dotnet-sdk-6.0.401-linux-arm64.tar.gz ;; \
   esac \
 && curl -sL -o dotnet.tar.gz $URL \
 && mkdir -p /usr/share/dotnet \
 && tar -zxf dotnet.tar.gz -C /usr/share/dotnet \
 && ln -s /usr/share/dotnet/dotnet /usr/bin/dotnet

RUN curl -fsSL https://dl.yarnpkg.com/debian/pubkey.gpg > /etc/apt/trusted.gpg.d/yarn.asc \
 && echo "deb https://dl.yarnpkg.com/debian/ stable main" > /etc/apt/sources.list.d/yarn.list \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc > /etc/apt/trusted.gpg.d/microsoft.asc \
 && echo "deb https://packages.microsoft.com/repos/microsoft-ubuntu-bionic-prod/ bionic main" > /etc/apt/sources.list.d/microsoft.list \
 && apt-get install --yes --no-install-recommends gnupg \
 && apt-get update \
 && apt-get install --yes --no-install-recommends \
    expect \
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

RUN case ${TARGETPLATFORM} in \
    "linux/amd64") ARCH=amd64; SHASUM=442dae58b727a79f81368127fac141d7f95501ffa05f8c48943d27c4e807deb7 ;; \
    "linux/arm64") ARCH=arm64; SHASUM=b216bebfbe30c3c156144cff07233654e23025e26ab5827058c9b284e130599e ;; \
   esac \
  && curl -fsSL "https://github.com/benesch/autouseradd/releases/download/1.3.0/autouseradd-1.3.0-$ARCH.tar.gz" -o autouseradd.tar.gz \
  && echo "$SHASUM autouseradd.tar.gz" | sha256sum -c - \
  && tar xzf autouseradd.tar.gz --strip-components 1 \
  && rm autouseradd.tar.gz

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
