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
