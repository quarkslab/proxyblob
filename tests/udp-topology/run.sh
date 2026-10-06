#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
output=$(mktemp -d)
prefix="proxyblob-udp-$$"
cleanup() {
  docker rm -f "$prefix-client" "$prefix-agent" "$prefix-proxy" >/dev/null 2>&1 || true
  docker network rm "$prefix-front" "$prefix-back" >/dev/null 2>&1 || true
  rm -rf "$output"
}
trap cleanup EXIT
arch=$(docker version --format '{{.Server.Arch}}')
GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -mod=readonly -o "$output/harness" ./tests/udp-topology
image=alpine:3.23
if ! docker image inspect "$image" >/dev/null 2>&1; then docker pull "$image"; fi
docker network create --internal "$prefix-front" >/dev/null
docker network create --internal "$prefix-back" >/dev/null
docker run -d --name "$prefix-proxy" --network "$prefix-front" --network-alias proxy-front \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" "$image" /harness proxy >/dev/null
docker network connect --alias proxy-back "$prefix-back" "$prefix-proxy"
docker run -d --name "$prefix-agent" --network "$prefix-back" \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" "$image" /harness agent >/dev/null
agent_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$prefix-agent")
docker run --name "$prefix-client" --network "$prefix-front" -e "AGENT_IP=$agent_ip" -e LIVE_BIND=1 \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" "$image" /harness client

GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go test -mod=readonly -c -o "$output/server.test" ./pkg/proxy/server
docker run --rm --network none --mount "type=bind,src=$output/server.test,dst=/server.test,readonly" "$image" /server.test -test.run "TestUDP|TestBind|TestRejectedAuth|TestConnectReply|TestMalformedSOCKS" -test.v -test.timeout=30s
