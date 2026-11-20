#!/usr/bin/env python3
"""apeos_fields.py -- the field twin of apeos_particles.py: on-axis
lineouts (wake Ez, laser envelope), EM field energy, bucket/envelope
peaks, peak density (-> lambda_p), and mid-propagation-plane slices.

Physics bytes per step, at the floor (DESIGN.md mechanisms 1-2):
  pass 1   all six field components streamed once: sums of squares
           -> EM energy (eps0/2 <E^2> + <B^2>/2mu0) * dV,
              max|Ez| (bucket amplitude), max|rho| (plasma density)
  pass 2   three 1-D rows along the propagation axis at transverse
           center (Ex, Ey, Ez): envelope sqrt(Ex^2+Ey^2) + wake Ez
  slices   --slice-steps: Ez and rho planes in the mid-plane, for
           the bucket-structure maps

The store is INVISIBLE: -d is any POSIX-visible openPMD series --
Lustre scratch or the leos-sealed unit on the EOS mount. Run it
twice, once per store, identical output schema; the only legal
difference between the two runs is *time*. apeos_compare.py turns
that pair into (1) a physics-integrity verdict and (2) the
Lustre-vs-EOS rate table, bytes_touched identical by construction.

Execution model identical to the particle analyzer: MPI-optional,
unit<->task over steps (embarrassingly parallel, coordination-free
-- the two stream levels of mechanism 3), the reduction is a few
KB of gathered numpy. This is the dask-shaped task graph --
independent read tasks + a tiny end reduction -- dispatched without
a scheduler tax; a dask scheduler can drive these same tasks later,
nothing in the analysis would change.

    srun -n 7 apeos_fields.py -d diags/bp5_native \
        --steps all --slices 0,50,100 -o out_lustre --plot
    srun -n 7 apeos_fields.py -d /eos/mnt/.../bp5_native \
        --steps all --slices 0,50,100 -o out_eos   --plot
"""
import argparse
import json
import os
import re
import time

import numpy as np
import openpmd_api as io

import apeos_eos

try:
    from mpi4py import MPI
    HAVE_MPI = True
except ImportError:
    HAVE_MPI = False

# vacuum constants (exact SI definitions)
EPS0 = 8.8541878128e-12
MU0 = 1.25663706212e-06
E_CH = 1.602176634e-19
M_E = 9.1093837015e-31
C = 299792458.0

SLAB = 1 << 24   # <=128 MiB f64 per streamed read


# ---------------------------------------------------------------- store-agnostic reader

def read_access():
    for name in ("read_only", "read_random_access", "READ_ONLY"):
        if hasattr(io.Access, name):
            return getattr(io.Access, name)
    raise SystemExit("no read-only Access mode found")


def comp(rec, name=None):
    """subscripted (proven-to-read) handle of a mesh component: mesh
    records must be SUBSCRIPTED on current pybind builds -- iterating
    them yields component copies that slice empty (validate_nway lesson)."""
    if name is None:
        return rec if hasattr(rec, "shape") else rec[""]
    return rec[name]


def slabs(shape):
    """contiguous axis-0 slabs of at most SLAB elements (validator copy)."""
    n = len(shape)
    if n == 1:
        step = max(1, min(SLAB, shape[0]))
        for a in range(0, shape[0], step):
            yield (slice(a, min(a + step, shape[0])),)
    elif n in (2, 3):
        last = shape[-1]
        if last > SLAB:
            for a in range(0, last, SLAB):
                sl = [slice(None)] * (n - 1) + [slice(a, min(a + SLAB, last))]
                yield tuple(sl)
        else:
            per = max(1, SLAB // last)
            for a in range(0, shape[0], per):
                if n == 3:
                    yield (slice(a, min(a + per, shape[0])), slice(None), slice(None))
                else:
                    yield (slice(None), slice(a, min(a + per, shape[-1])))
    else:
        raise SystemExit(f"unsupported ndim {n}")


def stream(s, rc):
    """one streaming pass: (sum, sum of squares, max|.|, bytes touched)."""
    tot = tot2 = mx = 0.0
    nb = 0
    for sl in slabs(tuple(rc.shape)):
        a = rc[sl]
        s.flush()
        a = np.ascontiguousarray(a)
        x = a.astype(np.float64, copy=False)
        tot += float(x.sum())
        tot2 += float(np.square(x).sum())
        if x.size:
            mx = max(mx, float(np.abs(x).max()))
        nb += a.nbytes
    return tot, tot2, mx, nb


def _pick(shape, center, full):
    idx = [slice(None) if i in full else center[i] for i in range(len(shape))]
    return tuple(idx)


def unit_metrics(path, slice_steps):
    """all field physics of one checkpoint unit; plain dict out, so the
    reduction layer only has to gather."""
    t0 = time.perf_counter()
    s = io.Series(path, read_access())
    step = int(min(s.iterations))
    it = s.iterations[step]
    mE, mB = it.meshes["E"], it.meshes["B"]
    rho = comp(it.meshes["rho"])
    sp = [float(x) for x in mE.grid_spacing]
    try:
        off = [float(x) for x in mE.grid_global_offset]
    except Exception:
        off = [0.0, 0.0, 0.0]
    shape = list(comp(mE, "x").shape)
    dV = float(np.prod(sp))
    nbytes = 0

    # ---- pass 1: energy + peaks (all six components streamed once) ----
    em = ez_pk = 0.0
    for m, half in ((mE, 0.5 * EPS0), (mB, 0.5 / MU0)):
        for c in "xyz":
            _, s2, mx, nb = stream(s, comp(m, c))
            nbytes += nb
            em += half * s2
            if m is mE and c == "z":
                ez_pk = mx
    _, _, rho_pk, nb = stream(s, rho)
    nbytes += nb
    em *= dV

    # ---- geometry: propagation axis = largest physical extent ----
    prop = int(np.argmax([sh * dx for sh, dx in zip(shape, sp)]))
    trans = [i for i in range(3) if i != prop]
    center = [shape[i] // 2 for i in range(3)]

    # ---- pass 2: 1-D lineouts along the propagation axis ----
    ln = {}
    for c in "xyz":
        rc = comp(mE, c)
        a = rc[_pick(shape, center, {prop})]
        s.flush()
        ln[c] = np.ascontiguousarray(a, dtype=np.float64)
    nbytes += sum(l.nbytes for l in ln.values())
    env = np.hypot(ln["x"], ln["y"])
    axis = (np.arange(shape[prop]) + 0.5) * sp[prop] + off[prop]

    out = dict(step=step, em_energy=em, ez_peak=ez_pk,
               env_peak=float(env.max()), rho_peak=rho_pk,
               lineout_axis=axis, lineout_ez=ln["z"], lineout_env=env,
               bytes=nbytes)
    # plasma wavelength from the peak density: lambda_p = 2 pi c sqrt(eps0 m_e / (n e^2))
    n0 = rho_pk / E_CH
    out["lambda_p"] = (2 * np.pi * C * np.sqrt(EPS0 * M_E / (n0 * E_CH ** 2))
                       if n0 > 0 else 0.0)

    # ---- optional plane slices (prop x trans[0], at trans[1]=center) ----
    if step in slice_steps:
        ts = time.perf_counter()
        planes = {}
        for tag, rc in (("ez", comp(mE, "z")), ("rho", rho)):
            a = rc[_pick(shape, center, {prop, trans[0]})]
            s.flush()
            planes[tag] = np.ascontiguousarray(a, dtype=np.float64)
        out["slice_ez"] = planes["ez"]
        out["slice_rho"] = planes["rho"]
        out["slice_extent"] = [off[prop], off[prop] + shape[prop] * sp[prop],
                               off[trans[0]], off[trans[0]] + shape[trans[0]] * sp[trans[0]]]
        nbytes += planes["ez"].nbytes + planes["rho"].nbytes
        out["seconds"] = time.perf_counter() - t0
    else:
        out["seconds"] = time.perf_counter() - t0
    return out


# ---------------------------------------------------------------- execution (MPI-optional, unit<->task)

def discover(root):
    """(step, path) of openpmd_XXXXXX.bp5 units under root, sorted."""
    pat = re.compile(r"^openpmd_(\d{6,})\.bp5?$")
    found = []
    for base, dirs, _ in os.walk(root):
        for d in dirs:
            m = pat.match(d)
            if m:
                found.append((int(m.group(1)), os.path.join(base, d)))
    return sorted(found)


class SerialComm:
    """mpi4py absent (bare python 3.9 on compute nodes): single process
    over all units -- identical outputs, the reduction is tiny by design."""

    def Get_size(self):
        return 1

    def Get_rank(self):
        return 0

    def gather(self, obj, root=0):
        return [obj]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--dir", default=None,
                    help="series root (or one unit); any POSIX store; "
                         "omit with --eos-root for the EOS-direct leg")
    ap.add_argument("--steps", default="all", help="all | 0,50,100")
    ap.add_argument("--slices", default="", help="steps with plane maps")
    ap.add_argument("-o", "--out", default="apeos_fields")
    ap.add_argument("--plot", action="store_true")
    apeos_eos.add_arguments(ap)
    a = ap.parse_args()

    comm = MPI.COMM_WORLD if HAVE_MPI else SerialComm()
    size, rank = comm.Get_size(), comm.Get_rank()

    eos_leg, local = apeos_eos.resolver(a)
    if eos_leg:
        if a.steps == "all":
            raise SystemExit("--eos-root: token-authenticated remote listing is "
                             "impossible on Helios -- pass concrete --steps "
                             "(e.g. --steps 0,50,100)")
        units = apeos_eos.units(a.eos_root,
                                 [int(x) for x in a.steps.split(",") if x.strip()],
                                 a.eos_host)
    else:
        if not a.dir:
            raise SystemExit("need -d DIR (POSIX store) or --eos-root EOSPATH")
        units = discover(a.dir)
        if not units and os.path.basename(a.dir.rstrip("/")).endswith((".bp5", ".bp")):
            units = discover(os.path.dirname(a.dir.rstrip("/")))
        if a.steps != "all":
            want = {int(x) for x in a.steps.split(",") if x.strip()}
            units = [u for u in units if u[0] in want]
    if not units:
        raise SystemExit(f"no openpmd_*.bp5 units under {a.dir or a.eos_root} "
                         f"for steps={a.steps}")
    slice_steps = {int(x) for x in a.slices.split(",") if x.strip()}

    if rank == 0:
        os.makedirs(a.out, exist_ok=True)
        print(f"[fields] {len(units)} units, {size} task(s), "
              f"slices at {sorted(slice_steps) or '-'}", flush=True)

    t0 = time.perf_counter()
    mine = [unit_metrics(local(p), slice_steps) for _, p in units[rank::size]]
    got = comm.gather(mine, root=0)
    dt = time.perf_counter() - t0

    if rank != 0:
        return
    res = sorted((r for run in got for r in run), key=lambda r: r["step"])
    steps = np.array([r["step"] for r in res])

    npz = {
        "steps": steps,
        "em_energy": np.array([r["em_energy"] for r in res]),
        "ez_peak": np.array([r["ez_peak"] for r in res]),
        "env_peak": np.array([r["env_peak"] for r in res]),
        "rho_peak": np.array([r["rho_peak"] for r in res]),
        "lambda_p": np.array([r["lambda_p"] for r in res]),
        "lineout_axis": res[-1]["lineout_axis"],
        "lineout_ez": np.array([r["lineout_ez"] for r in res]),
        "lineout_env": np.array([r["lineout_env"] for r in res]),
    }
    for r in res:
        if "slice_ez" in r:
            npz[f"slice_ez_{r['step']:06d}"] = r["slice_ez"]
            npz[f"slice_rho_{r['step']:06d}"] = r["slice_rho"]
            npz[f"slice_extent_{r['step']:06d}"] = np.array(r["slice_extent"])
    np.savez(os.path.join(a.out, "apeos_fields.npz"), **npz)

    by = sum(r["bytes"] for r in res)
    meta = {"tool": "fields", "dir": a.dir or a.eos_root, "units_total": len(units),
            "ranks": size, "steps": [int(x) for x in steps],
            "bytes_touched": int(by), "seconds": dt,
            "rate_GiBs": (by / 2 ** 30) / dt if dt > 0 else 0,
            "per_unit": [{"step": r["step"], "bytes": r["bytes"],
                          "seconds": r["seconds"],
                          "rate_GiBs": (r["bytes"] / 2 ** 30) / r["seconds"]}
                         for r in res]}
    with open(os.path.join(a.out, "apeos_fields.json"), "w") as fh:
        json.dump(meta, fh, indent=1)
    print(json.dumps({k: v for k, v in meta.items() if k != "per_unit"}), flush=True)

    if a.plot:
        render(a, res, npz)


# ---------------------------------------------------------------- figures

def render(a, res, npz):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
    out, step = a.out, res[-1]["step"]

    # lineouts, final step: wake + envelope, lambda_p annotated
    ax = npz["lineout_axis"] * 1e6
    fig, (a1, a2) = plt.subplots(2, 1, sharex=True, figsize=(7, 5))
    a1.plot(ax, npz["lineout_env"][-1])
    a1.set_ylabel(r"$|E_\perp|$ [a.u.]")
    a1.set_title(f"on-axis laser envelope and wake, step {step}"
                 f"   ($\\lambda_p$ = {npz['lambda_p'][-1]*1e6:.1f} um)")
    a2.plot(ax, npz["lineout_ez"][-1], color="firebrick")
    a2.set_ylabel("$E_z$ [V/m]")
    a2.set_xlabel("propagation axis [um]")
    fig.tight_layout()
    fig.savefig(f"{out}/lineouts_{step:06d}.png", dpi=110)
    plt.close(fig)

    # evolution curves
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(6, 5))
    a1.semilogy(npz["steps"], npz["em_energy"], "o-")
    a1.set_ylabel("EM field energy [J]")
    a2.semilogy(npz["steps"], npz["ez_peak"], "s-", color="firebrick", label=r"$\max|E_z|$")
    a2.semilogy(npz["steps"], npz["env_peak"], "^-", color="steelblue", label=r"$\max|E_\perp|$")
    a2.set_ylabel("peak field [V/m]")
    a2.set_xlabel("step")
    a2.legend()
    fig.tight_layout()
    fig.savefig(f"{out}/evolution.png", dpi=110)
    plt.close(fig)

    # plane maps: diverging Ez, symmetric about zero; rho contours
    for r in res:
        if "slice_ez" not in r:
            continue
        ext = [e * 1e6 for e in r["slice_extent"]]
        vmax = float(np.percentile(np.abs(r["slice_ez"]), 99.5)) or 1.0
        fig, axm = plt.subplots(figsize=(7, 4))
        im = axm.imshow(r["slice_ez"].T, origin="lower", cmap="RdBu_r",
                        norm=Normalize(-vmax, vmax), extent=ext,
                        aspect="auto", interpolation="nearest")
        cs = axm.contour(r["slice_rho"].T, extent=ext, colors="k",
                         linewidths=0.5, linestyles="--",
                         levels=[v for v in (0.25, 0.5, 0.75)
                                 if 0 < v < float(r["slice_rho"].max())])
        fig.colorbar(im).set_label("$E_z$ [V/m]")
        axm.set_title(f"$E_z$ mid-plane + $\\rho$ contours, step {r['step']}")
        axm.set_xlabel("propagation [um]")
        axm.set_ylabel("transverse [um]")
        fig.tight_layout()
        fig.savefig(f"{out}/slice_{r['step']:06d}.png", dpi=110)
        plt.close(fig)
    print(f"plots -> {out}/*.png")


if __name__ == "__main__":
    main()
