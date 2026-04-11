"""
Local SOCKS5-to-HTTP proxy bridge.

Chromium/Playwright does NOT support SOCKS5 with username/password
authentication. This module starts a small local HTTP proxy (on
127.0.0.1:RANDOM_PORT) that accepts HTTP CONNECT requests from the
browser, opens a SOCKS5 connection to the upstream proxy with
authentication, and forwards bytes bidirectionally.

Usage:
    from src.core.proxy_bridge import get_or_create_bridge

    # Returns http://127.0.0.1:PORT (no auth)
    local_url = get_or_create_bridge("socks5://user:pass@host:port")

    # Pass local_url to Playwright/CloakBrowser instead of the original
    # SOCKS5 URL. The bridge tunnels traffic through the real proxy.

Behavior:
    - Same upstream URL returns the SAME local bridge (port cached).
    - Bridges are daemon threads — auto-terminate on process exit.
    - If the upstream URL is already HTTP(S) or SOCKS5 without auth,
      no bridge is needed — returns the original URL unchanged.
"""

import socket
import socketserver
import threading
import time
from urllib.parse import urlparse


# Cache: upstream URL -> local bridge URL (so we reuse bridges)
_BRIDGE_CACHE: dict = {}
_BRIDGE_CACHE_LOCK = threading.Lock()


def _do_socks5_handshake(sock: socket.socket, user: str, pwd: str,
                          target_host: str, target_port: int) -> bool:
    """Perform SOCKS5 username/password auth + CONNECT. Returns True on success."""
    try:
        # Step 1: method negotiation — offer both "no auth" (0x00) and "user/pass" (0x02)
        sock.sendall(b"\x05\x02\x00\x02")
        resp = sock.recv(2)
        if len(resp) < 2 or resp[0] != 0x05:
            return False
        method = resp[1]

        # Step 2: authenticate if server demands it
        if method == 0x02:
            user_b = user.encode("utf-8")
            pwd_b = pwd.encode("utf-8")
            if len(user_b) > 255 or len(pwd_b) > 255:
                return False
            auth_packet = b"\x01" + bytes([len(user_b)]) + user_b + bytes([len(pwd_b)]) + pwd_b
            sock.sendall(auth_packet)
            auth_resp = sock.recv(2)
            if len(auth_resp) < 2 or auth_resp[1] != 0x00:
                return False
        elif method != 0x00:
            return False  # unsupported auth method

        # Step 3: CONNECT request (ATYP=0x03 domain name)
        host_b = target_host.encode("idna") if any(ord(c) > 127 for c in target_host) else target_host.encode("ascii")
        if len(host_b) > 255:
            return False
        connect_packet = (
            b"\x05\x01\x00\x03"
            + bytes([len(host_b)])
            + host_b
            + target_port.to_bytes(2, "big")
        )
        sock.sendall(connect_packet)

        # Read CONNECT response — 4 byte header then variable BND.ADDR + BND.PORT
        conn_resp = sock.recv(4)
        if len(conn_resp) < 4 or conn_resp[1] != 0x00:
            return False
        atyp = conn_resp[3]
        if atyp == 0x01:  # IPv4
            sock.recv(4 + 2)
        elif atyp == 0x03:  # domain
            length_byte = sock.recv(1)
            if length_byte:
                sock.recv(length_byte[0] + 2)
        elif atyp == 0x04:  # IPv6
            sock.recv(16 + 2)
        return True
    except Exception:
        return False


def _forward(src: socket.socket, dst: socket.socket):
    """Forward bytes from src to dst until connection closes."""
    try:
        while True:
            data = src.recv(8192)
            if not data:
                break
            dst.sendall(data)
    except Exception:
        pass
    finally:
        try:
            dst.shutdown(socket.SHUT_WR)
        except Exception:
            pass


class _BridgeHandler(socketserver.BaseRequestHandler):
    """Handles one incoming HTTP CONNECT from Chromium."""

    # Set by factory: upstream proxy details
    upstream_host: str = ""
    upstream_port: int = 0
    upstream_user: str = ""
    upstream_pass: str = ""

    def handle(self):
        client = self.request
        try:
            # Read HTTP request line + headers (up to \r\n\r\n)
            buffer = b""
            while b"\r\n\r\n" not in buffer:
                chunk = client.recv(4096)
                if not chunk:
                    return
                buffer += chunk
                if len(buffer) > 16384:
                    return  # malformed

            request_line = buffer.split(b"\r\n", 1)[0].decode("latin-1", errors="ignore")
            parts = request_line.split()
            if len(parts) < 2 or parts[0].upper() != "CONNECT":
                # Only HTTPS tunneling supported (Chrome sends CONNECT for https://)
                client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
                return

            # Parse "host:port"
            target = parts[1]
            if ":" not in target:
                client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                return
            target_host, target_port_str = target.rsplit(":", 1)
            try:
                target_port = int(target_port_str)
            except ValueError:
                client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
                return

            # Connect to upstream SOCKS5 proxy
            upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            upstream.settimeout(15)
            try:
                upstream.connect((self.upstream_host, self.upstream_port))
            except Exception:
                client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                upstream.close()
                return

            upstream.settimeout(15)
            ok = _do_socks5_handshake(
                upstream,
                self.upstream_user,
                self.upstream_pass,
                target_host,
                target_port,
            )
            if not ok:
                client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                upstream.close()
                return

            # Tell browser tunnel is ready
            client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")

            # Remove timeout for tunneling
            upstream.settimeout(None)
            client.settimeout(None)

            # Bidirectional forwarding in two threads
            t1 = threading.Thread(target=_forward, args=(client, upstream), daemon=True)
            t2 = threading.Thread(target=_forward, args=(upstream, client), daemon=True)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            try:
                upstream.close()
            except Exception:
                pass
        except Exception:
            pass
        finally:
            try:
                client.close()
            except Exception:
                pass


class _ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _start_bridge(
    upstream_host: str,
    upstream_port: int,
    upstream_user: str,
    upstream_pass: str,
) -> int:
    """Start a local HTTP-to-SOCKS5 bridge. Returns the local port."""

    class Handler(_BridgeHandler):
        pass

    Handler.upstream_host = upstream_host
    Handler.upstream_port = upstream_port
    Handler.upstream_user = upstream_user
    Handler.upstream_pass = upstream_pass

    # Bind to random port on 127.0.0.1
    server = _ThreadedTCPServer(("127.0.0.1", 0), Handler)
    local_port = server.server_address[1]

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    return local_port


def get_or_create_bridge(proxy_url: str) -> str:
    """
    Convert a proxy URL into a browser-compatible URL.

    - If proxy is empty -> returns ""
    - If proxy is HTTP / HTTPS / SOCKS5 without auth -> returns unchanged
    - If proxy is SOCKS5 with username/password -> starts a local bridge
      and returns http://127.0.0.1:PORT (no auth needed by browser)

    Thread-safe. Same upstream URL returns the same cached local port.
    """
    if not proxy_url:
        return ""

    url = proxy_url.strip()
    if not url:
        return ""

    parsed = urlparse(url)
    scheme = (parsed.scheme or "").lower()

    # HTTP/HTTPS proxies work directly with Chromium — pass through
    if scheme in ("http", "https"):
        return url

    # SOCKS4/SOCKS5 without auth works directly — pass through
    if scheme in ("socks4", "socks4a", "socks5", "socks5h") and not parsed.username:
        return url

    # SOCKS5 WITH auth — start local bridge
    if scheme not in ("socks5", "socks5h"):
        # Unknown scheme — pass through as-is
        return url

    host = parsed.hostname or ""
    port = parsed.port or 1080
    user = parsed.username or ""
    pwd = parsed.password or ""
    if not host or not port:
        return url

    with _BRIDGE_CACHE_LOCK:
        if url in _BRIDGE_CACHE:
            return _BRIDGE_CACHE[url]
        try:
            local_port = _start_bridge(host, port, user, pwd)
            local_url = f"http://127.0.0.1:{local_port}"
            _BRIDGE_CACHE[url] = local_url
            return local_url
        except Exception:
            return url
