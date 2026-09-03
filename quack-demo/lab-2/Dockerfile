FROM debian:bookworm-slim

COPY --from=duckdb/duckdb:1.5.3 /duckdb /usr/local/bin/duckdb

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
COPY sql/ /sql/
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
