# Tooling image for the failure experiments (see experiments/README.md).
# The Kafka CLI comes with the base image (/opt/kafka/bin); the rest from Alpine:
# bash, curl, jq, psql, kcat, and the Docker CLI with the Compose plugin, which
# drives the stack through the Docker socket mounted by docker-compose.yml.
# The scripts are not copied in: they are read from the repository, mounted at
# /workspace, so editing a script never needs a rebuild.
FROM apache/kafka:4.4.0

USER root
RUN apk add --no-cache bash coreutils curl diffutils docker-cli docker-cli-compose jq kcat postgresql-client

# The Kafka CLI on the PATH, for the walkthroughs (`experiments shell`).
ENV PATH="/opt/kafka/bin:${PATH}"

WORKDIR /workspace
ENTRYPOINT ["bash", "/workspace/experiments/bin/experiment"]
CMD ["help"]
