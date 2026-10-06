#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
driver=${1:-tcp}
case "$driver" in tcp|azblob|azqueue|aztable) ;; *) echo 'expected tcp, azblob, azqueue, or aztable' >&2; exit 2;; esac
if [[ $driver != tcp ]]; then
 : "${AZNET_LIVE_CONFIG:?Set AZNET_LIVE_CONFIG to an authorized Azure test-account configuration}"
fi
if [[ -n ${FTP_BIND_OUTPUT:-} ]]; then
 output=$FTP_BIND_OUTPUT
 mkdir "$output" # Refuse to overwrite previous evidence.
else
 output=$(mktemp -d)
fi
output=$(cd "$output" && pwd)
prefix="pb$(uuidgen | tr -d '-' | tr '[:upper:]' '[:lower:]' | cut -c1-22)"
client_image=proxyblob-ftp-client:bookworm
server_image=proxyblob-ftp-server:trixie
cleanup() {
 status=$?
 trap - EXIT
 docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' "$prefix-ftp" > "$output/ftp-state.log" 2>/dev/null || true
 for role in agent proxy ftp; do docker logs "$prefix-$role" > "$output/$role.log" 2>&1 || true; done
 docker rm -f "$prefix-client" "$prefix-agent" "$prefix-proxy" "$prefix-ftp" >/dev/null 2>&1 || true
 if [[ $driver != tcp && -f "$output/harness" ]]; then
  docker run --rm --network bridge -e "LIVE_DRIVER=$driver" -e "LIVE_PREFIX=$prefix" \
   --mount "type=bind,src=$AZNET_LIVE_CONFIG,dst=/config.json,readonly" \
   --mount "type=bind,src=$output/harness,dst=/harness,readonly" alpine:3.23 /harness live cleanup > "$output/cleanup.log" 2>&1 || status=1
  cat "$output/cleanup.log"
 fi
 docker network rm "$prefix-front" "$prefix-back" >/dev/null 2>&1 || true
 rm -rf "$output/state" # Private SAS handoff is never retained with evidence.
 echo "Evidence: $output"
 exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
mkdir "$output/client" "$output/server" "$output/state" "$output/signals"
arch=$(docker version --format '{{.Server.Arch}}')
GOWORK=off go list -mod=readonly -m github.com/atsika/aznet
GOWORK=off GOOS=linux GOARCH="$arch" CGO_ENABLED=0 go build -mod=readonly -o "$output/harness" ./tests/udp-topology
docker build --target client -t "$client_image" tests/ftp-bind > "$output/client-image.log" 2>&1
docker build --target server -t "$server_image" tests/ftp-bind > "$output/server-image.log" 2>&1
docker run --rm "$client_image" dpkg-query -W dante-client tnftp | tee "$output/versions.log"
docker run --rm "$server_image" dpkg-query -W vsftpd | tee -a "$output/versions.log"
python3 - "$output" <<'PY'
from pathlib import Path
import sys
r = Path(sys.argv[1])
(r / 'client/upload.bin').write_bytes((bytes(range(251)) * 8356)[:2*1024*1024])
(r / 'server/sample.bin').write_bytes((bytes(range(250,-1,-1)) * 8356)[:2*1024*1024])
PY
docker network create --internal "$prefix-front" >/dev/null
if [[ $driver == tcp ]]; then
 docker network create --internal "$prefix-back" >/dev/null
else
 docker network create "$prefix-back" >/dev/null
fi
docker run --init -d --name "$prefix-ftp" --network "$prefix-back" \
 --mount "type=bind,src=$output/server,dst=/data" \
 --mount "type=bind,src=$PWD/tests/ftp-bind/vsftpd.conf,dst=/etc/vsftpd.conf,readonly" \
 "$server_image" sh -c 'cp /data/sample.bin /srv/ftp/sample.bin; chown -R tester:tester /srv/ftp; exec /usr/sbin/vsftpd /etc/vsftpd.conf' >/dev/null
if [[ $driver == tcp ]]; then
 docker run -d --name "$prefix-proxy" --network "$prefix-front" --network-alias proxy-front \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" alpine:3.23 /harness proxy >/dev/null
 docker network connect --alias proxy-back "$prefix-back" "$prefix-proxy"
 docker run -d --name "$prefix-agent" --network "$prefix-back" \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" alpine:3.23 /harness agent >/dev/null
else
 docker run -d --name "$prefix-proxy" --network "$prefix-front" --network-alias proxy-front \
  -e "LIVE_DRIVER=$driver" -e "LIVE_PREFIX=$prefix" \
  --mount "type=bind,src=$AZNET_LIVE_CONFIG,dst=/config.json,readonly" \
  --mount "type=bind,src=$output/signals,dst=/signals" \
  --mount "type=bind,src=$output/state,dst=/state" \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" alpine:3.23 /harness live proxy >/dev/null
 docker network connect bridge "$prefix-proxy"
 docker run -d --name "$prefix-agent" --network "$prefix-back" \
  -e "LIVE_DRIVER=$driver" -e "LIVE_PREFIX=$prefix" \
  --mount "type=bind,src=$output/state,dst=/state,readonly" \
  --mount "type=bind,src=$output/harness,dst=/harness,readonly" alpine:3.23 /harness live agent >/dev/null
fi
ftp_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$prefix-ftp")
agent_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$prefix-agent")
proxy_ip=$(docker inspect -f "{{(index .NetworkSettings.Networks \"$prefix-front\").IPAddress}}" "$prefix-proxy")
docker run --name "$prefix-client" --network "$prefix-front" -e "FTP_IP=$ftp_ip" -e "PROXY_IP=$proxy_ip" \
 --mount "type=bind,src=$output/client,dst=/data" \
 --mount "type=bind,src=$PWD/tests/ftp-bind/client.sh,dst=/client.sh,readonly" "$client_image" bash /client.sh | tee "$output/client.log"
for mode in eprt port; do docker cp "$prefix-ftp:/srv/ftp/uploaded-$mode.bin" "$output/server/uploaded-$mode.bin"; done
python3 tests/ftp-bind/verify.py "$output" "$ftp_ip" "$agent_ip" | tee "$output/verification.log"
if [[ $driver != tcp ]]; then
 touch "$output/state/stop"
 for role in proxy agent; do
  code=$(docker wait "$prefix-$role")
  [[ "$code" == 0 ]] || exit 1
 done
fi
