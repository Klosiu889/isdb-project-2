FROM rust:alpine3.22 AS builder

RUN apk add --no-cache \
    bash \
    sed \
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
COPY post_process.sh ./

RUN npm install @openapitools/openapi-generator-cli -g
RUN chmod +x post_process.sh
ENV RUST_POST_PROCESS_FILE="./post_process.sh"
RUN openapi-generator-cli generate -i dbmsInterface.yaml -g rust-server -o openapi -p generateAliasAsModel=true --enable-post-process-file

RUN cargo build --release || true

COPY . .

RUN cargo build --release --bin simple_rust_dbms

FROM alpine:3.22

RUN apk add --no-cache libgcc

WORKDIR /app

COPY --from=builder /app/target/release/simple_rust_dbms .

EXPOSE 8080

ENTRYPOINT ["./simple_rust_dbms"]
