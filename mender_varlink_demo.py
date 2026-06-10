#!/usr/bin/env python3
"""Showcase the mender-update varlink IPC, entirely on-device.

Talks the varlink wire protocol (NUL-separated JSON) directly to the
mender-update daemon's unix socket -- no varlink library required.

Socket: /run/mender/update.sock  (interface io.mender.Update1)

Usage (on the device):
    python3 /root/mender_varlink_demo.py            # run the full showcase
    python3 /root/mender_varlink_demo.py getstate
    python3 /root/mender_varlink_demo.py continue
    python3 /root/mender_varlink_demo.py abort
    python3 /root/mender_varlink_demo.py extendtimeout 3600
    python3 /root/mender_varlink_demo.py monitor [seconds]   # live feed
"""
import json
import socket
import sys
import time

SOCKET_PATH = "/run/mender/update.sock"
IFACE = "io.mender.Update1"
NUL = b"\x00"


def _connect():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(SOCKET_PATH)
    return s


def call(method, parameters=None, more=False):
    """Send one varlink call; return the first reply as a dict."""
    s = _connect()
    msg = {"method": f"{IFACE}.{method}"}  # type: dict
    if parameters is not None:
        msg["parameters"] = parameters
    if more:
        msg["more"] = True
    s.sendall(json.dumps(msg).encode() + NUL)
    buf = b""
    while NUL not in buf:
        chunk = s.recv(4096)
        if not chunk:
            break
        buf += chunk
    s.close()
    frame = buf.split(NUL)[0]
    return json.loads(frame) if frame else {}


def monitor(seconds=None):
    """Subscribe to the live status/download-progress feed and print frames.

    seconds=None streams until Ctrl-C; a number streams for that long.
    """
    s = _connect()
    s.sendall(json.dumps({"method": f"{IFACE}.Monitor", "more": True}).encode() + NUL)
    if seconds is None:
        print("[monitor] subscribed; streaming until Ctrl-C...")
        deadline = None
    else:
        print(f"[monitor] subscribed; streaming for {seconds}s (Ctrl-C to stop)...")
        deadline = time.time() + seconds
    buf = b""
    s.settimeout(1.0)
    try:
        while deadline is None or time.time() < deadline:
            try:
                chunk = s.recv(4096)
            except socket.timeout:
                continue
            if not chunk:
                print("[monitor] connection closed by daemon")
                break
            buf += chunk
            while NUL in buf:
                frame, buf = buf.split(NUL, 1)
                if frame:
                    print("[signal]", json.dumps(json.loads(frame)))
    except KeyboardInterrupt:
        print("\n[monitor] stopped")
    finally:
        s.close()


def show(label, result):
    print(f"\n=== {label} ===")
    print(json.dumps(result, indent=2, sort_keys=True))


def showcase():
    # READ-ONLY showcase: never mutates the deployment. Continue/Abort/
    # ExtendTimeout are NOT called here (they would resume/kill a real paused
    # deployment) -- run them as explicit subcommands when you want to act.
    print("mender-update varlink IPC showcase (read-only)  (socket: %s)" % SOCKET_PATH)

    state = call("GetState")
    show("GetState", state)

    p = state.get("parameters", {})
    if p.get("paused"):
        print(f"\n>> Deployment is PAUSED before {p.get('state')!r}.")
        print("   To act, run one of:")
        print("     ./mender_varlink_demo.py continue        # resume")
        print("     ./mender_varlink_demo.py abort           # abort + roll back")
        print("     ./mender_varlink_demo.py extendtimeout 3600")
    else:
        print(f"\n>> status={p.get('status')!r} (not paused).")
        print("   Control methods (continue/abort/extendtimeout) return NotPaused"
              " until a deployment is paused.")

    # Live feed: every status change and every download-% increase is pushed
    # here -- no polling. Read-only; Ctrl-C to stop.
    print("\n=== Monitor (live feed) ===")
    print("download-progress climbs during download, then a paused frame, then")
    print("(on continue/abort/timeout) the transition. Ctrl-C to stop.")
    # Stream until Ctrl-C (a number as arg 2 limits it to that many seconds).
    monitor(seconds=int(sys.argv[2]) if len(sys.argv) > 2 else None)


def main():
    if len(sys.argv) < 2:
        showcase()
        return
    cmd = sys.argv[1].lower()
    if cmd == "getstate":
        show("GetState", call("GetState"))
    elif cmd == "continue":
        show("Continue", call("Continue"))
    elif cmd == "abort":
        show("Abort", call("Abort"))
    elif cmd == "extendtimeout":
        secs = int(sys.argv[2]) if len(sys.argv) > 2 else 3600
        show(f"ExtendTimeout {secs}", call("ExtendTimeout", {"seconds": secs}))
    elif cmd == "monitor":
        # No arg => stream until Ctrl-C; `monitor N` => N seconds.
        monitor(seconds=int(sys.argv[2]) if len(sys.argv) > 2 else None)
    elif cmd == "raw":
        # raw '<json>' -- send an arbitrary frame
        s = _connect()
        s.sendall(sys.argv[2].encode() + NUL)
        buf = b""
        while NUL not in buf:
            d = s.recv(4096)
            if not d:
                break
            buf += d
        print(buf.split(NUL)[0].decode())
        s.close()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
