"""Check independent application logs as well as transferred bytes."""
import hashlib
import re
import sys
from pathlib import Path

root, ftp_ip, agent_ip = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
server_log = (root / "server/server.log").read_text()
assert '"PASV"' not in server_log and '"EPSV"' not in server_log
for mode in ("eprt", "port"):
    log = (root / f"client/socks-{mode}.log").read_text()
    requests = re.findall(r"sending request to server: VER: 5 CMD: (\d+)", log)
    assert requests == ["1", "2", "2", "2"], requests
    replies = re.findall(r"received response from server: VER: 5 REP: (\d+).*address: (\S+)", log)
    assert len(replies) == 7 and all(code == "0" for code, _ in replies), replies
    # CONNECT gets one reply; each BIND gets a listener reply followed by its peer.
    for index in (1, 3, 5):
        assert replies[index][1].startswith(agent_ip + "."), replies[index]
        assert replies[index + 1][1] == ftp_ip + ".20", replies[index + 1]
    assert log.count("Raccept(): accepted forwarded connection") == 3
    listing = (root / f"client/listing-{mode}.txt").read_text()
    assert "sample.bin" in listing and f"uploaded-{mode}.bin" in listing
    for source, target in (
        ("client/upload.bin", f"server/uploaded-{mode}.bin"),
        ("server/sample.bin", f"client/download-{mode}.bin"),
    ):
        original, received = (root / source).read_bytes(), (root / target).read_bytes()
        assert len(original) == 2 * 1024 * 1024 and original == received, target
        print(f"PASS {mode} {target} bytes={len(received)} sha256={hashlib.sha256(received).hexdigest()}")
    print(f"PASS {mode} CONNECT=1 BIND=3 successful_replies=7 listing=true")
assert server_log.count('"EPRT |1|' + agent_ip + '|') == 3
assert server_log.count('"PORT ' + agent_ip.replace('.', ',') + ',') == 3
assert server_log.count('"226 Transfer complete."') == 4
assert server_log.count('"226 Directory send OK."') == 2
print("PASS active FTP: both command forms, upload/download/list, no passive fallback")
