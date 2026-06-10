#!/usr/bin/env python3
"""Read the logged-in user's X bookmarks via their existing Chrome session.

No X API key, no manual paste. Launches the *real* Google Chrome binary headless
against a COPY of the user's Chrome profile (so Chrome itself decrypts its cookies
via Keychain -- no prompt, since the Chrome binary is on the Keychain ACL), drives
it over the DevTools protocol to x.com/i/bookmarks, and intercepts the Bookmarks
GraphQL responses the page makes (correct query-id / features handled by X's own
client). Scrolls to paginate. Parses tweets out and writes JSON.

Output: .cache/x_bookmarks.json   (id, author, name, text, urls, url)

Standalone: only needs the websocket-client lib (present in .venv) + Google Chrome.
"""
from __future__ import annotations

import json
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from websocket import create_connection

PORT = 9333
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
SRC = Path.home() / "Library/Application Support/Google/Chrome"
TMP = Path(f"/tmp/oa_chrome_bm_{int.from_bytes(socket.gethostname().encode()[:2],'big')}")
OUT = Path("/Users/edwarddgao/openapply/.cache/x_bookmarks.json")
MAX_SCROLLS = 25


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def http_json(path):
    return json.load(urllib.request.urlopen(f"http://localhost:{PORT}{path}", timeout=5))


class CDP:
    def __init__(self, ws_url):
        self.ws = create_connection(ws_url, timeout=30,
                                    header=[f"Origin: http://localhost:{PORT}"],
                                    max_size=64 * 1024 * 1024)
        self._id = 0
        self.events = []

    def send(self, method, params=None):
        self._id += 1
        self.ws.send(json.dumps({"id": self._id, "method": method, "params": params or {}}))
        return self._id

    def cmd(self, method, params=None, timeout=30):
        _id = self.send(method, params)
        end = time.time() + timeout
        while time.time() < end:
            self.ws.settimeout(max(0.1, end - time.time()))
            try:
                m = json.loads(self.ws.recv())
            except Exception:
                continue
            if m.get("id") == _id:
                return m
            self.events.append(m)
        raise TimeoutError(method)

    def pump(self, seconds):
        end = time.time() + seconds
        while time.time() < end:
            self.ws.settimeout(max(0.1, end - time.time()))
            try:
                self.events.append(json.loads(self.ws.recv()))
            except Exception:
                continue


def launch_chrome():
    if TMP.exists():
        shutil.rmtree(TMP, ignore_errors=True)
    (TMP / "Default").mkdir(parents=True)
    shutil.copy2(SRC / "Local State", TMP / "Local State")
    shutil.copy2(SRC / "Default" / "Cookies", TMP / "Default" / "Cookies")
    pref = SRC / "Default" / "Preferences"
    if pref.exists():
        shutil.copy2(pref, TMP / "Default" / "Preferences")
    args = [
        CHROME, "--headless=new", f"--remote-debugging-port={PORT}",
        "--remote-allow-origins=*", f"--user-data-dir={TMP}",
        "--no-first-run", "--no-default-browser-check", "--disable-extensions",
        "--disable-gpu", "--disable-background-networking", "--disable-sync",
        "--window-size=1280,2000", "about:blank",
    ]
    proc = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(60):
        try:
            http_json("/json/version"); return proc
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("Chrome devtools did not come up")


def page_ws():
    for _ in range(40):
        for t in http_json("/json"):
            if t.get("type") == "page" and t.get("webSocketDebuggerUrl"):
                return t["webSocketDebuggerUrl"]
        time.sleep(0.25)
    raise RuntimeError("no page target")


def extract_tweets(body, sink):
    """Walk a Bookmarks GraphQL response, append tweet dicts to sink (by id)."""
    try:
        data = json.loads(body)
    except Exception:
        return
    insts = (data.get("data", {}).get("bookmark_timeline_v2", {})
             .get("timeline", {}).get("instructions", []))
    for inst in insts:
        for entry in inst.get("entries", []):
            content = entry.get("content", {})
            item = content.get("itemContent", {})
            res = item.get("tweet_results", {}).get("result")
            if not res:
                continue
            if res.get("__typename") == "TweetWithVisibilityResults":
                res = res.get("tweet", res)
            legacy = res.get("legacy", {})
            rest_id = res.get("rest_id") or legacy.get("id_str")
            if not rest_id:
                continue
            note = (res.get("note_tweet", {}).get("note_tweet_results", {})
                    .get("result", {}).get("text"))
            text = note or legacy.get("full_text", "")
            user_result = res.get("core", {}).get("user_results", {}).get("result", {})
            ucore = user_result.get("core", {})          # new schema location
            ulegacy = user_result.get("legacy", {})       # older location
            screen = ucore.get("screen_name") or ulegacy.get("screen_name", "")
            name = ucore.get("name") or ulegacy.get("name", "")
            urls = [u.get("expanded_url") for u in legacy.get("entities", {}).get("urls", [])
                    if u.get("expanded_url")]
            sink[rest_id] = {
                "id": rest_id,
                "author": screen,
                "name": name,
                "text": text,
                "urls": urls,
                "created_at": legacy.get("created_at", ""),
                "url": f"https://x.com/{screen}/status/{rest_id}" if screen else "",
            }


def main():
    proc = launch_chrome()
    try:
        cdp = CDP(page_ws())
        cdp.cmd("Network.enable")
        cdp.cmd("Page.enable")
        cdp.cmd("Page.navigate", {"url": "https://x.com/i/bookmarks"})
        cdp.pump(6)

        tweets = {}
        seen_req = set()
        last_count = -1
        stale = 0
        for i in range(MAX_SCROLLS):
            # harvest any Bookmarks responses seen so far
            for ev in cdp.events:
                if ev.get("method") == "Network.responseReceived":
                    p = ev["params"]
                    url = p.get("response", {}).get("url", "")
                    if "/Bookmarks" in url and p["requestId"] not in seen_req:
                        seen_req.add(p["requestId"])
                        try:
                            r = cdp.cmd("Network.getResponseBody", {"requestId": p["requestId"]})
                            extract_tweets(r.get("result", {}).get("body", ""), tweets)
                        except Exception:
                            pass
            cdp.events.clear()
            log(f"  scroll {i}: {len(tweets)} bookmarks so far")
            if len(tweets) == last_count:
                stale += 1
                if stale >= 3:
                    break
            else:
                stale = 0
                last_count = len(tweets)
            cdp.cmd("Runtime.evaluate",
                    {"expression": "window.scrollTo(0, document.body.scrollHeight)"})
            cdp.pump(2.5)

        result = sorted(tweets.values(), key=lambda t: int(t["id"]), reverse=True)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(result, indent=2))
        log(f"\nwrote {len(result)} bookmarks -> {OUT}")
        print(json.dumps({"count": len(result), "out": str(OUT)}))
    finally:
        try:
            proc.terminate(); proc.wait(timeout=5)
        except Exception:
            proc.kill()
        shutil.rmtree(TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
