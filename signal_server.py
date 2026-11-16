"""KH2 co-op signaling server.

Handles lobby codes and hole-punch coordination. Most connections become
direct peer-to-peer after the punch; only strict-NAT users fall back to
TCP relay for lobby messages (game traffic is always direct or fails).

Deploy anywhere Python runs (Railway, Render, Fly.io, a $5 VPS …).
  python signal_server.py [port]        # default port 7420
  PORT=7420 python signal_server.py     # or via env var (Railway sets $PORT)

Both players set  SIGNAL_URL = "yourhost:7420"  in lobby.py.
"""
import json
import os
import secrets
import socket
import string
import sys
import threading

PORT     = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 7420))
ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ"   # no I/O confusion
CODE_LEN = 4
ROOM_TTL = 600   # seconds to wait for joiner

rooms: dict = {}
lock  = threading.Lock()


# ── helpers ────────────────────────────────────────────────────────────────

def gen_code() -> str:
    for _ in range(500):
        c = "".join(secrets.choice(ALPHABET) for _ in range(CODE_LEN))
        if c not in rooms:
            return c
    raise RuntimeError("room space full")


def jrecv(conn: socket.socket, timeout: float = 15.0) -> dict:
    """Read one newline-terminated JSON object."""
    buf = b""
    conn.settimeout(timeout)
    while b"\n" not in buf:
        c = conn.recv(1)
        if not c:
            return {}
        buf += c
    try:
        return json.loads(buf.decode("utf-8", errors="replace").strip())
    except ValueError:
        return {}


def jsend(conn: socket.socket, obj: dict):
    conn.sendall((json.dumps(obj) + "\n").encode())


def relay_frames(src: socket.socket, dst: socket.socket):
    """Forward 4-byte-length-prefixed frames from src → dst until either closes."""
    try:
        while True:
            hdr = b""
            while len(hdr) < 4:
                chunk = src.recv(4 - len(hdr))
                if not chunk:
                    return
                hdr += chunk
            n = int.from_bytes(hdr, "big")
            data = b""
            while len(data) < n:
                chunk = src.recv(n - len(data))
                if not chunk:
                    return
                data += chunk
            dst.sendall(hdr + data)
    except OSError:
        pass
    finally:
        for s in (src, dst):
            try:
                s.close()
            except OSError:
                pass


# ── connection handler ──────────────────────────────────────────────────────

def handle(conn: socket.socket, addr):
    try:
        msg  = jrecv(conn, timeout=20)
        role = msg.get("role", "")
        ep   = msg.get("ep", "unknown")

        if role == "host":
            _handle_host(conn, addr, ep)
        elif role == "join":
            _handle_join(conn, addr, ep, msg.get("code", "").strip().upper())
        else:
            jsend(conn, {"error": "bad role"})
            conn.close()

    except Exception as exc:
        print(f"[!] {addr}: {exc}")
        try:
            conn.close()
        except OSError:
            pass


def _handle_host(conn: socket.socket, addr, ep: str):
    with lock:
        code = gen_code()
        ev   = threading.Event()
        rooms[code] = {"host": conn, "host_ep": ep,
                       "joiner": None, "join_ep": "", "event": ev}

    jsend(conn, {"code": code})
    print(f"[+] {addr} hosted  {code}  ep={ep}")

    paired = ev.wait(timeout=ROOM_TTL)
    with lock:
        room = rooms.pop(code, None)

    if not paired or not room or room["joiner"] is None:
        try:
            jsend(conn, {"error": "timeout"})
            conn.close()
        except OSError:
            pass
        print(f"[-] {code} timed out")
        return

    joiner   = room["joiner"]
    join_ep  = room["join_ep"]

    # Exchange endpoints — both clients start punching simultaneously
    jsend(conn,   {"peer": join_ep})
    jsend(joiner, {"peer": ep})

    # Collect punch results (both sides report within ~6 s)
    host_ok = jrecv(conn,   timeout=8).get("ok", False)
    join_ok = jrecv(joiner, timeout=8).get("ok", False)

    if host_ok and join_ok:
        jsend(conn,   {"direct": True})
        jsend(joiner, {"direct": True})
        conn.close()
        joiner.close()
        print(f"[=] {code}  direct P2P")
    else:
        jsend(conn,   {"relay": True})
        jsend(joiner, {"relay": True})
        print(f"[~] {code}  relay fallback")
        t = threading.Thread(target=relay_frames, args=(joiner, conn), daemon=True)
        t.start()
        relay_frames(conn, joiner)


def _handle_join(conn: socket.socket, addr, ep: str, code: str):
    with lock:
        room = rooms.get(code)
        if not room or room["joiner"] is not None:
            jsend(conn, {"error": "not found"})
            conn.close()
            return
        room["joiner"] = conn
        room["join_ep"] = ep
        ev = room["event"]
    ev.set()
    print(f"[+] {addr} joined  {code}  ep={ep}")
    # Socket ownership transferred to host thread — do not read/write here.


# ── server loop ─────────────────────────────────────────────────────────────

def main():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", PORT))
    srv.listen(200)
    print(f"KH2 signal server  port={PORT}")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
