"""ge.py — PR0 harness helpers: HTTP to the _admin/test routes, git via subprocess.

Stdlib only (spec). GE_URL (see below) carries write-token userinfo exactly like
tests/conformance/run.sh — for urllib calls the userinfo is stripped and the
credential rides an Authorization header instead; git(1) gets the URL verbatim.
"""

import base64
import hashlib
import json
import os
import re
import subprocess
import tempfile
import urllib.error
import urllib.request
from urllib.parse import urlparse, urlunparse

GE_URL = os.environ.get("GE_URL", "http://test:write-test-token@127.0.0.1:8795")

_USERINFO = re.compile(r"(https?://)[^\s/@]+@")


def redact(s):
    """Strip URL userinfo from a string: scheme://user:pass@ -> scheme://***@."""
    return _USERINFO.sub(r"\1***@", s)


def _endpoint():
    u = urlparse(GE_URL)
    netloc = u.hostname or "127.0.0.1"
    if u.port:
        netloc += ":%d" % u.port
    base = urlunparse((u.scheme or "http", netloc, "", "", "", ""))
    auth = None
    if u.username is not None:
        auth = "Basic " + base64.b64encode(("%s:%s" % (u.username, u.password or "")).encode()).decode()
    return base, auth


def http(method, path, body=None, auth=True, raw=None):
    """Raw request -> (status, body). Never raises on HTTP errors."""
    base, cred = _endpoint()
    req = urllib.request.Request(base + path, method=method)
    if auth and cred:
        req.add_header("Authorization", cred)
    data = None
    if raw is not None:
        data = raw
        req.add_header("Content-Type", "application/octet-stream")
    elif body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data=data, timeout=120) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def admin(repo, method, path, body=None):
    st, data = http(method, "/%s/_admin/test/%s" % (repo, path), body)
    if st != 200:
        raise AssertionError(
            "%s /%s/_admin/test/%s -> %d: %s"
            % (method, repo, path, st, redact(data[:400].decode("utf-8", "replace")))
        )
    return json.loads(data)


def jobs(repo):
    return admin(repo, "GET", "jobs")


def step(repo, kind=None):
    return admin(repo, "POST", "step", {} if kind is None else {"kind": kind})


def failpoint(repo, name, action, count=1):
    return admin(repo, "POST", "failpoint", {"name": name, "action": action, "count": count})


def repair(repo):
    return admin(repo, "POST", "repair", {})


def fsck(repo, mode="refs"):
    return admin(repo, "GET", "fsck?mode=%s" % mode)


def drop_object(repo, sha):
    return admin(repo, "POST", "drop_object", {"sha": sha})


def corrupt_object(repo, sha, action="corrupt"):
    return admin(repo, "POST", "drop_object", {"sha": sha, "action": action})


def drop_pack(repo, pack):
    return admin(repo, "POST", "drop_pack", {"pack": pack})


def knob(repo, name, value):
    return admin(repo, "POST", "knob", {"name": name, "value": value})


def meta(repo, key, value):
    """Set one meta row directly; value=None deletes it (legacy-state jig)."""
    return admin(repo, "POST", "meta", {"key": key, "value": value})


def enqueue(repo, kind, payload="{}"):
    """Queue a job row the way repair resurrection does (dedups vs queued)."""
    return admin(repo, "POST", "enqueue", {"kind": kind, "payload": payload})


def state(repo):
    """The public /_state probe — (status, dict|None)."""
    st, data = http("GET", "/%s/_state" % repo)
    return st, (json.loads(data) if st == 200 else None)


def import_stage(repo, pack):
    """POST _admin/import/stage with raw pack bytes -> {push,key,bytes}."""
    st, data = http("POST", "/%s/_admin/import/stage" % repo, raw=pack)
    if st != 200:
        raise AssertionError("import stage -> %d: %s" % (st, redact(data[:400].decode("utf-8", "replace"))))
    return json.loads(data)


def import_begin(repo, push, parts, commands):
    """POST _admin/import — queues the import_pack job; returns response body."""
    st, data = http(
        "POST",
        "/%s/_admin/import" % repo,
        {"push": push, "parts": parts, "commands": commands},
    )
    if st != 200:
        raise AssertionError("import begin -> %d: %s" % (st, redact(data[:400].decode("utf-8", "replace"))))
    return data


def import_status(repo, push):
    st, data = http("GET", "/%s/_admin/import/%s" % (repo, push))
    if st != 200:
        raise AssertionError("import status -> %d" % st)
    return json.loads(data)


def pack_file(repo):
    """Bytes of a pack holding HEAD's closure (pack-objects --stdout, REF
    deltas like the conformance import) — the _admin/import stage payload."""
    listing = git("-C", repo.dir, "rev-list", "--objects", "HEAD").stdout
    return pack_objects(repo.dir, listing)


def pack_objects(repo_dir, listing):
    """pack-objects --stdout over an explicit object list — exactly the listed
    objects, no closure. `listing` is either the raw stdin text or a list of
    lines ("<sha>[ <name>]" — the same shape rev-list --objects emits)."""
    if not isinstance(listing, str):
        listing = "\n".join(listing) + "\n"
    p = subprocess.run(
        ["git", "-C", repo_dir, "pack-objects", "--stdout"],
        input=listing.encode(),
        capture_output=True,
        env=_GIT_ENV,
    )
    if p.returncode != 0:
        raise AssertionError("pack-objects -> %d: %s" % (p.returncode, redact(p.stderr.decode("utf-8", "replace"))))
    return p.stdout


# --- raw smart-HTTP receive-pack (C2's minimal/empty-pack pushes) ---


def pkt_line(payload):
    """One pkt-line: 4-hex length prefix (itself counted) + payload bytes."""
    return b"%04x" % (len(payload) + 4) + payload


def empty_pack():
    """A legal 0-object pack: PACKv2 header + sha1 trailer — a ref-only push."""
    head = b"PACK" + (2).to_bytes(4, "big") + (0).to_bytes(4, "big")
    return head + hashlib.sha1(head).digest()


def receive_pack(repo, commands, pack, caps=("report-status",)):
    """Raw POST /<o>/<r>/git-receive-pack — pkt-line commands + flush + pack
    bytes. commands = [(old_hex, new_hex, refname)]; -> (status, body)."""
    out = []
    for i, (old, new, name) in enumerate(commands):
        line = ("%s %s %s" % (old, new, name)).encode()
        if i == 0:
            line += b"\0" + " ".join(caps).encode()
        out.append(pkt_line(line))
    return http("POST", "/%s/git-receive-pack" % repo, raw=b"".join(out) + b"0000" + pack)


# --- git over subprocess: a real client over real smart-HTTP ---

_GIT_ENV = dict(
    os.environ,
    GIT_AUTHOR_NAME="harness",
    GIT_AUTHOR_EMAIL="harness@test",
    GIT_COMMITTER_NAME="harness",
    GIT_COMMITTER_EMAIL="harness@test",
    GIT_TERMINAL_PROMPT="0",
    GIT_CONFIG_NOSYSTEM="1",
)


def git(*args, cwd=None, check=True, input=None):
    r = subprocess.run(["git", *args], cwd=cwd, env=_GIT_ENV, capture_output=True, text=True, input=input)
    if check and r.returncode != 0:
        raise AssertionError(
            "git %s -> %d\n%s%s" % (redact(" ".join(args)), r.returncode, redact(r.stdout), redact(r.stderr))
        )
    return r


class Repo:
    """A unique scratch repo on the dev server plus a local seed working tree."""

    def __init__(self, name):
        self.name = name  # "owner/repo"
        self.url = GE_URL.rstrip("/") + "/" + name
        self.dir = tempfile.mkdtemp(prefix="ge-harness-")
        git("init", "-q", "-b", "main", self.dir)
        git("config", "user.email", "harness@test", cwd=self.dir)
        git("config", "user.name", "harness", cwd=self.dir)

    def commit_file(self, fname, content, msg):
        with open(os.path.join(self.dir, fname), "w") as f:
            f.write(content)
        git("add", fname, cwd=self.dir)
        git("commit", "-qm", msg, cwd=self.dir)
        return git("rev-parse", "HEAD", cwd=self.dir).stdout.strip()

    def push(self, refspec="HEAD:main", force=False):
        args = ["push", "-q"]
        if force:
            args.append("-f")
        return git(*(args + [self.url, refspec]), cwd=self.dir)

    def clone(self, suffix="-clone"):
        target = self.dir + suffix
        git("clone", "-q", self.url, target)
        return target
