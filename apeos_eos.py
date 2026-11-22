# apeos_eos.py -- EOS-direct store leg: local metadata staging + FileXRootD
# sidecar, so the UNCHANGED analyzers read payload straight from the archive.
#
# Architecture (verified end-to-end against GSI Helios, 2026-11-20):
#   * BP5Reader opens metadata (md.idx, md.0, mmd.0 -- a few MB) locally from
#     a POSIX directory, and streams data files (data.N) over XRootD via the
#     fork's FileXRootD transport, activated by a local s3.json sidecar.
#     (The sidecar is the config channel because openPMD-api 0.17 silently
#     drops unknown adios2 engine params, and ADIOS2 has no Python bindings
#     in the container.)
#   * Credential: EOS capability token (zteos64), presented as ?authz= on the
#     complete object URL -- reads via xrdfs/metadata paths are token-blind
#     on Helios and are never used here.
#   * Remote listing is impossible with a token (xrdfs limitation), so EOS
#     legs enumerate units from concrete step numbers, never by discovery.
import json
import os
import subprocess
import sys
import time

HOST = os.environ.get("EOS_HOST", "helios.hpc.gsi.de")
META_REQUIRED = ("md.idx",)                                   # BP5: always present
META_DEFAULT = ("md.idx", "md.0", "mmd.0", "bbstage.final")   # bbstage aggregated layout
TOKEN_FILE = os.environ.get(
    "EOS_TOKEN_FILE", "/lustre/rz/dbertini/io-tests/adios-tests/eos/helios.tok")
TTL_S = int(os.environ.get("EOS_TOKEN_TTL_S", 8 * 3600))     # minted token lifetime
MARGIN_S = int(os.environ.get("EOS_TOKEN_MARGIN_S", 1800))   # safety floor for a leg


def token(path=TOKEN_FILE):
    """Load the capability token and enforce expiry margin BEFORE any work.
    A run that discovers at second 1 that its credential dies mid-leg beats
    one that discovers it by dying at 80% of the campaign."""
    tok = os.environ.get("EOSAUTHZ", "").strip()
    if os.path.exists(path):
        age = time.time() - os.path.getmtime(path)
        if age > TTL_S - MARGIN_S:
            sys.exit(f"[eos] token {path} is {age / 3600:.1f} h old "
                     f"(TTL {TTL_S / 3600:.0f} h, margin {MARGIN_S / 60:.0f} min): "
                     "re-mint on lxbk0960:  eos token --path /eos/helios/Users/<user>/ "
                     "--tree --expires $(( $(date +%s) + 8*3600 )) | "
                     "sed -n 's/.*\\(zteos64:[^ ]*\\).*/\\1/p' > "
                     f"{path}   (charset trap: tokens contain '-' and '~'; "
                     "grep-charset extraction truncates them)")
        tok = open(path).read().strip() or tok
    if not tok:
        sys.exit(f"[eos] no token: export EOSAUTHZ or mint into {path}")
    os.environ["EOSAUTHZ"] = tok          # FileXRootD (libXrdCl) reads it from env
    return tok


def units(eos_root, steps, host=HOST):
    """(step, remote unit path) from concrete steps -- the fleet convention is
    openpmd_%06d.bp5 under eos_root, one unit per step."""
    return [(int(s), f"{eos_root.rstrip('/')}/openpmd_{int(s):06d}.bp5")
            for s in steps if str(s).strip()]


def stage(unit_remote, work, host=HOST, tok=None, meta=None):
    """Materialize the small local view of a remote unit: metadata files +
    s3.json sidecar. Returns the local directory to open.

    RACE-SAFE BY CONSTRUCTION: every MPI rank calls this for every unit it
    opens, all targeting the same staging directory.  At NP=16 that raced
    (16 concurrent 'xrdcp -f' onto one md.0; readers opened truncated
    metadata and ADIOS2 reported "truncated by an interrupted writer").
    Protocol: each file downloads to a per-PID temp name and is published
    with os.replace -- atomic on POSIX, so a reader sees only ever a
    complete file -- and a completeness marker naming the file list is
    published LAST.  Marker present and matching => every file complete;
    otherwise the whole small set is (re)fetched, which also repairs any
    corrupt leftover from an interrupted run.  Duplicated downloads across
    racing ranks cost a few MB once per unit; a lock would cost a dead
    rank's orphaned lockfile instead."""
    tok = tok or token()
    name = os.path.basename(unit_remote.rstrip("/"))
    d = os.path.join(work, name)
    os.makedirs(d, exist_ok=True)
    files = (os.environ.get("EOS_META_FILES", ",".join(meta or META_DEFAULT))
             .split(","))
    done = os.path.join(d, ".apeos_staged")
    try:                                   # marker content = staged file set
        fresh = open(done).read().strip() == ",".join(files)
    except OSError:
        fresh = False
    if not fresh:
        for f in files:
            dst = os.path.join(d, f)
            tmp = f"{dst}.{os.getpid()}.tmp"
            url = f"root://{host}//{unit_remote.lstrip('/')}/{f}?authz={tok}"
            r = subprocess.run(["xrdcp", "-f", url, tmp],
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                               text=True)
            if r.returncode != 0:
                try:
                    os.remove(tmp)
                except OSError:
                    pass
                missing = any(k in r.stderr for k in
                              ("could not stat", "No such file", "5003", "Unable"))
                if missing and f not in META_REQUIRED:
                    continue    # layout without this metadata file: fine
                sys.exit(f"[eos] staging {url.split('?')[0]} failed: "
                         f"{r.stderr.strip()[:200]} (expired token answers "
                         "'public access level restriction')")
            os.replace(tmp, dst)           # atomic publish
    # Sidecar: host+path fused in `endpoint` is the double-slash-safe spelling
    # for ADIOS2 builds before 2cd0c1dda and remains valid after it.
    # "threads"/"maxopenfiles" are engine defaults the fork's BP5Reader picks
    # up from the sidecar (openPMD-api cannot forward them). Measured on GSI
    # Helios (2026-11-21): opens are the currency -- every engine thread
    # re-opens (nearly) every data file it touches, each paying the MGM token
    # tax (~0.1 s). threads=1 with a handle window covering ALL data files
    # opens each file exactly once (3-4x faster than the default window of 10,
    # which silently re-opens on file revisits); threads>=8 degrade 4x. Scale
    # with PROCESSES (mpirun ranks), not engine threads. Older libraries
    # ignore these keys -- forward-safe.  Republished atomically every call
    # so EOS_DATA_THREADS/EOS_DATA_MAXOPEN changes take effect on rerun.
    side = os.path.join(d, "s3.json")
    side_tmp = f"{side}.{os.getpid()}.tmp"
    with open(side_tmp, "w") as j:
        json.dump({"version": 1, "transport": "xrootd",
                   "endpoint": f"{host}//{unit_remote.lstrip('/')}",
                   "threads": os.environ.get("EOS_DATA_THREADS", "1"),
                   "maxopenfiles": os.environ.get("EOS_DATA_MAXOPEN", "512")}, j)
    os.replace(side_tmp, side)
    if not fresh:                          # completeness marker LAST
        done_tmp = f"{done}.{os.getpid()}.tmp"
        with open(done_tmp, "w") as m:
            m.write(",".join(files) + "\n")
        os.replace(done_tmp, done)
    return d


def add_arguments(ap):
    g = ap.add_argument_group("EOS-direct leg")
    g.add_argument("--eos-root", default=None,
                   help="EOS directory holding openpmd_XXXXXX.bp5 units; "
                        "enables the direct leg (concrete --steps required)")
    g.add_argument("--eos-host", default=HOST, help="XRootD endpoint host[:port]")
    g.add_argument("--eos-work", default=None,
                   help="metadata staging dir (default: <out>/eos_meta)")
    g.add_argument("--eos-token-file", default=TOKEN_FILE,
                   help="token file (0600); expiry-margin enforced")


def resolver(a):
    """(unit_is_local: bool, callable remote_path -> local openable dir)."""
    if not a.eos_root:
        return False, (lambda p: p)
    tok = token(a.eos_token_file)
    work = a.eos_work or os.path.join(a.out, "eos_meta")
    return True, (lambda p: stage(p, work, a.eos_host, tok))
