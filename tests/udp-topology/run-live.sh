#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${AZNET_LIVE_CONFIG:?Set AZNET_LIVE_CONFIG to the existing account-key configuration path}"
driver=${1:-azblob}
case "$driver" in azblob|azqueue|aztable) ;; *) exit 2;; esac
output=$(mktemp -d)
prefix="pb$(uuidgen | tr -d '-' | tr '[:upper:]' '[:lower:]' | cut -c1-22)"
image=alpine:3.23
cleanup() {
  status=$?
  trap - EXIT
  docker rm -f "$prefix-client" "$prefix-agent" "$prefix-proxy" >/dev/null 2>&1 || true
  if [[ -f "$output/harness" ]]; then
    docker run --rm --network bridge -e "LIVE_DRIVER=$driver" -e "LIVE_PREFIX=$prefix" \
      --mount "type=bind,src=$AZNET_LIVE_CONFIG,dst=/config.json,readonly" \
      --mount "type=bind,src=$output/harness,dst=/harness,readonly" "$image" /harness live cleanup || status=1
  fi
  docker network rm "$prefix-front" "$prefix-back" >/dev/null 2>&1 || true
  rm -rf "$output"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
arch=$(docker version --format '{{.Server.Arch}}')
GOWORK=off GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -mod=readonly -o "$output/harness" ./tests/udp-topology
if ! docker image inspect "$image" >/dev/null 2>&1; then docker pull "$image"; fi
mkdir "$output/state" "$output/signals"
# Client network is internal. Only proxy and agent have Azure HTTPS egress.
docker network create --internal "$prefix-front" >/dev/null
docker network create "$prefix-back" >/dev/null
docker run -d --name "$prefix-proxy" --network "$prefix-front" --network-alias proxy-front \
  -e "LIVE_DRIVER=$driver" -e "LIVE_PREFIX=$prefix" \
  --mount "type=bind,src=$AZNET_LIVE_CONFIG,dst=/config.json,readonly" \
  --mount "type=bind,src=$output/signals,dst=/signals,readonly" \
  --mount "type=bind,src=$output/state,dst=/state" \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" "$image" /harness live proxy >/dev/null
docker network connect bridge "$prefix-proxy"
docker run -d --name "$prefix-agent" --network "$prefix-back" \
  -e "LIVE_DRIVER=$driver" -e "LIVE_PREFIX=$prefix" \
  --mount "type=bind,src=$output/state,dst=/state,readonly" \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" "$image" /harness live agent >/dev/null
agent_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$prefix-agent")
set +e
docker run --name "$prefix-client" --network "$prefix-front" -e "AGENT_IP=$agent_ip" -e LIVE_TEST=1 \
  --mount "type=bind,src=$output/signals,dst=/signals" \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" "$image" /harness client
status=$?
set -e
touch "$output/state/stop"
for role in proxy agent; do
  code=$(docker wait "$prefix-$role")
  docker logs "$prefix-$role"
  [[ "$code" == 0 ]] || status=1
done
exit "$status"
