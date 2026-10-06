# Independent application test for SOCKS BIND

SFTP uses an SSH connection and normally exercises SOCKS CONNECT. **Active FTP**
is a practical BIND example: its control connection goes out through CONNECT,
while the FTP server opens separate data connections back through BIND.

This test uses unmodified `tnftp` with Dante's `socksify` client library and a
real `vsftpd` service. No custom FTP client/server or SOCKS handshake generator is
used. The existing topology executable supplies the production ProxyBlob proxy
and agent handlers, using either a TCP-carried tunnel or real Azure via aznet.
It does not launch the interactive proxy CLI or the packaged agent executable.

## Run

Requirements: Docker, Go, Python 3, Bash and `uuidgen`. Docker image builds need
Debian package access. Client tools use Debian Bookworm (Dante is available there),
and the server uses Debian Trixie. Installed package versions are recorded with
each run. These are disposable test images and a test-only FTP account.

```sh
tests/ftp-bind/run.sh tcp
AZNET_LIVE_CONFIG=/absolute/authorized-config.json tests/ftp-bind/run.sh azblob
```

The live run uses the committed published dependency with `GOWORK=off
-mod=readonly`. Use `azqueue` or `aztable` for the other services. It creates only
randomly prefixed bootstrap/session resources in the explicitly authorized test
account. An independent catalog check requires zero residual resources and fails
if it must reclaim leftovers. Credentials are mounted only into the proxy and
cleanup containers; the agent gets a private SAS handoff that is removed on exit.
The FTP client and server receive no Azure credentials.

The script prints its temporary evidence directory. Set `FTP_BIND_OUTPUT` to an
absolute, **not yet existing** directory to choose where it retains application
logs, hashes and transferred fixtures. Containers and networks are removed on
exit. The server uses Docker `--init`: in this environment, running vsftpd as
PID 1 exited with code 139 on direct login/quit even without ProxyBlob. Running
it under init passed consecutive sessions; the fixture does not mask/restart a
crashed server.

## Assertions

- Client and FTP server occupy separate networks, with a failed direct TCP probe.
  The agent can reach the FTP service; the client can reach only the proxy.
- `tnftp -A` forces active mode; vsftpd disables passive mode. Dante's automatic
  LAN routes and direct fallback are disabled.
- Two sessions exercise extended `EPRT` and classic `PORT`. Each uploads a 2 MiB
  binary, downloads a different 2 MiB binary and obtains a directory listing.
- Independent Dante logs must show one SOCKS5 CONNECT and three BIND requests per
  session, with seven successful replies: one for CONNECT and two per BIND.
- First BIND replies advertise the agent interface; second replies identify the
  FTP server's data endpoint. Server logs must show the corresponding active-mode
  address commands, four completed file transfers and two completed listings.
- Uploads and downloads are compared byte for byte and SHA-256 hashes are printed.
  Merely getting a zero FTP process exit status is not sufficient.

This validates native IPv4 active FTP interoperability, not SFTP using BIND,
FTPS, public NAT traversal, WASM BIND, every FTP client, concurrent transfers or a
throughput benchmark. IPv6/domain BIND, cancellation and peer-policy behavior have
separate protocol regressions documented in [native BIND](socks-bind.md).

References: [Dante socksify](https://www.inet.no/dante/doc/latest/socksify.1.html),
[tnftp active mode](https://manpages.debian.org/bookworm/tnftp/ftp.1.en.html),
[vsftpd configuration](https://manpages.debian.org/bookworm/vsftpd/vsftpd.conf.5.en.html).

## Recorded result — 2026-10-06

All four modes passed: isolated TCP tunnel, real Azure Blob, real Azure Queue,
and real Azure Table. Runtime source was ProxyBlob
`048789407e8cb19f181b82b8bf656187a841f785`; the retained fixture is commit
`10dd01b605a440300c298f0df52cbba234da76ec`. The published aznet dependency was
`v0.0.0-20261006132211-7abcd80a7a28`. Go 1.26.4 built the Linux/arm64 harness.
Tools were Dante client `1.4.2+dfsg-7`, tnftp `20210827-4+b1` and vsftpd `3.0.5-0.2`.

Per mode: two FTP control sessions, six successful BIND conversations, four
byte-identical 2 MiB file transfers and two directory listings. The active data
connections reached the advertised agent address from the FTP server's port 20.
All three Azure runs independently reported zero residual resources before and
after the cleanup check. Test containers and networks were removed.

The two fixture hashes were:

```text
upload:   1e075c8d478ad21844e33e830a695ef03a4d2488b69ee275bd8947618bb1be1e
download: 12d23d71c5fe90e2fb248621126e104c9eed8b8796156a3043453e692db36ad5
```

No production fix was required. The earlier single-session harness evidence is
now supplemented by an independent FTP client/library/server interoperability
case; it still does not establish public inbound reachability for a deployment.
