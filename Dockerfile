# Shopping Sessions: static native binary in an empty image.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY engine/Cargo.toml engine/Cargo.lock ./
COPY engine/src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+crt-static"
RUN cargo build --release --locked --target x86_64-unknown-linux-gnu \
 && cp target/x86_64-unknown-linux-gnu/release/ss /ss

FROM scratch
COPY --from=build /ss /ss
USER 1000:1000
ENTRYPOINT ["/ss"]
