FROM rust:alpine3.22 as builder

RUN apk add --no-cache \
    bash \
    curl \
    openjdk17-jdk \
    git \
    musl-dev \
    openssl-dev \
    openssl-libs-static \
    nodejs-current \
    npm \
    ca-certificates \
    build-base

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY dbmsInterface.yaml ./

RUN npm install @openapitools/openapi-generator-cli -g
RUN openapi-generator-cli generate -i dbmsInterface.yaml -g rust-server -o openapi
RUN echo "pub type Int64Column = Vec<i64>;" >> openapi/src/models.rs
RUN	echo "pub type VarcharColumn = Vec<String>;" >> openapi/src/models.rs

RUN cargo build --release || true

COPY . .

RUN cargo build --release --bin proj2

FROM alpine:3.22

RUN apk add --no-cache libgcc

WORKDIR /app

COPY --from=builder /app/target/release/proj2 .

EXPOSE 8080

ENTRYPOINT ["./proj2"]
