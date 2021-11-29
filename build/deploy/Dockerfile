FROM registry.access.redhat.com/ubi8/ubi-minimal

# For deployment, we need the following additionally installed:
# tzdata - for time zone functions; reinstalled to replace the missing
#          files in /usr/share/zoneinfo/
# hostname - used in cockroach k8s manifests
# tar - used by kubectl cp
RUN microdnf update -y \
    && rpm --erase --nodeps tzdata \
    && microdnf install tzdata hostname tar gzip xz git m4 make automake libtool wget -y \
    && rm -rf /var/cache/yum

RUN wget http://ftp.gnu.org/gnu/autoconf/autoconf-2.69.tar.gz

RUN tar -xzvf autoconf-2.69.tar.gz \
    && cd autoconf-2.69 \
    && ./configure && make && make install \
    && cd ..


RUN wget https://github.com/inotify-tools/inotify-tools/archive/refs/tags/3.21.9.6.tar.gz

RUN tar -xzvf 3.21.9.6.tar.gz\
    && cd inotify-tools-3.21.9.6\
    && ./autogen.sh \
    && ./configure --prefix=/usr \
    && make \
    && make install

RUN mkdir /usr/local/lib/cockroach /cockroach /licenses /docker-entrypoint-initdb.d
COPY cockroach.sh cockroach /cockroach/
COPY licenses/* /licenses/
# Install GEOS libraries.
COPY libgeos.so libgeos_c.so /usr/local/lib/cockroach/

# Set working directory so that relative paths
# are resolved appropriately when passed as args.
WORKDIR /cockroach/

# Include the directory in the path to make it easier to invoke
# commands via Docker
ENV PATH=/cockroach:$PATH

ENV COCKROACH_CHANNEL=official-docker

EXPOSE 26257 8080
ENTRYPOINT ["/cockroach/cockroach.sh"]
