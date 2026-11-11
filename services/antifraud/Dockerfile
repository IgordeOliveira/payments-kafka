# syntax=docker/dockerfile:1
# Build context: the repository root (npm workspaces):
#   docker build -f services/antifraud/Dockerfile .
# Debian slim (glibc): @confluentinc/kafka-javascript ships prebuilt
# librdkafka binaries for glibc; Alpine (musl) would need a source build.
ARG NODE_IMAGE=node:22-bookworm-slim

FROM ${NODE_IMAGE} AS build
WORKDIR /app
COPY . .
RUN npm ci --no-audit --no-fund
RUN npm run build -w @payments-kafka/consumer-runtime -w @payments-kafka/antifraud
RUN npm prune --omit=dev --no-audit --no-fund

FROM ${NODE_IMAGE}
ENV NODE_ENV=production
WORKDIR /app
COPY --from=build /app/package.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/contracts ./contracts
# Whole workspace directories: npm may install some dependencies in a
# workspace's own node_modules (e.g. ajv@8 under packages/consumer-runtime).
COPY --from=build /app/packages/consumer-runtime ./packages/consumer-runtime
COPY --from=build /app/services/antifraud ./services/antifraud
USER node
CMD ["node", "services/antifraud/dist/main.js"]
