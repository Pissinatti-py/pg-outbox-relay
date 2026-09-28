FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked && cp target/release/pg-outbox-relay /pg-outbox-relay

# Same Debian release as the build stage, so the glibc versions match.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /pg-outbox-relay /usr/local/bin/pg-outbox-relay
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/pg-outbox-relay"]
