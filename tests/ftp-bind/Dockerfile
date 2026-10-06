FROM debian:bookworm-slim AS client
RUN apt-get update && apt-get install -y --no-install-recommends dante-client tnftp python3 && rm -rf /var/lib/apt/lists/*

FROM debian:trixie-slim AS server
RUN apt-get update && apt-get install -y --no-install-recommends vsftpd && rm -rf /var/lib/apt/lists/*
RUN useradd -m -d /srv/ftp tester && echo 'tester:test-only' | chpasswd && mkdir -p /var/run/vsftpd/empty && chown tester:tester /srv/ftp
