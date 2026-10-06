#!/usr/bin/env bash
set -euo pipefail
python3 - <<'PY'
import os,socket,time
try:
 s=socket.create_connection((os.environ['FTP_IP'],2121),2)
except OSError:
 print('PASS direct client-to-FTP TCP blocked',flush=True)
else:
 s.close()
 raise SystemExit('FAIL client reached FTP directly')
for _ in range(180):
 try:
  s=socket.create_connection((os.environ['PROXY_IP'],1080),1);s.close();break
 except OSError:time.sleep(.5)
else:raise SystemExit('proxy startup timeout')
PY
export SOCKS5_SERVER="$PROXY_IP:1080" SOCKS_AUTOADD_LANROUTES=no SOCKS_DIRECTROUTE_FALLBACK=no SOCKS_DEBUG=2
cd /data
for mode in eprt port; do
 export SOCKS_LOGOUTPUT="/data/socks-$mode.log"
 extended=''
 if [[ $mode == port ]]; then extended=epsv4; fi
 timeout 180 socksify tnftp -4 -A -n -i -v "$FTP_IP" 2121 <<COMMANDS
user tester test-only
$extended
binary
put upload.bin uploaded-$mode.bin
get sample.bin download-$mode.bin
ls . listing-$mode.txt
bye
COMMANDS
done
