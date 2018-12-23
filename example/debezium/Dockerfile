ARG DEBEZIUM_VERSION=0.8

FROM debezium/connect:$DEBEZIUM_VERSION

# System setup
ENV dir /home/app
ENV JMXHOST=localhost
ENV JMXPORT=9010

USER root

# Install dependencies
# NOTE: comes w/ bash, curl, python, but not pip and epel-release (needed to find pip)
RUN yum -y install epel-release \
  && yum clean all
RUN yum -y install \
  jq \
  nc \
  make \
  python-pip \
  && yum clean all
RUN yum -y remove epel-release

WORKDIR ${dir}

# File upload
COPY run-docker.sh register-debezium.json ${dir}/

# Reset entrypoint
ENTRYPOINT []

CMD ["./run-docker.sh"]
