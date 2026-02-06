FROM rust:1.93.0 as base
RUN cargo install cargo-chef

FROM base AS planner
WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM base as builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin rust_spike

FROM rust:1.93.0 AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/rust_spike /usr/local/bin
ENTRYPOINT ["/usr/local/bin/rust_spike"]