#!/usr/bin/env python3
"""apeos_particles.py -- pp-ana's particle analysis, transplanted onto
the archive. Same physics and MPI reduction structure as
pp-ana's ParticleAnalyzer (energy spectrum, divergence with FWHM,
z-x energy map), but there is no parquet anywhere: the input is an
openPMD series at any POSIX path (Lustre scratch or a leos-sealed unit
on the EOS mount) and only the seven particle components the physics
actually uses are ever touched:

    position.xyz + positionOffset.xyz + momentum.xyz + weight
    (~1/4..1/8 of the stored particle bytes; on EOS those reads are ranged GETs)

    mpirun -n 8 apeos_particles.py -d /eos/.../diag_run -o out/ [--plot]
    python3   apeos_particles.py -d /lustre/runs -f diag00000100.bp --edges pass

MPI is optional: mpi4py absent (e.g. naked python 3.9 on cluster compute
nodes, where MPI lives only inside apptainer) falls back to a single
process over all units; identical outputs, since the MPI layer only
sums local histograms.

Energy convention matches pp-ana (total energy, MeV):
    E = m_e c^2 sqrt(1 + p_norm^2),  p_norm = |p| / (m_e c)
Divergence = arctan2(p_perp, p_z), degrees; FWHM by the exact port of
dataManipulator.width().

--edges pass (default): exact global min/max (pp-ana semantics) costs a
second streaming pass over the SAME selected components (documented
2x-payload trade). --edges probe: bounded sampling of the head of each
unit, one pass, clip-tails approximation (flagged in the meta).
"""
import argparse
import json
import os
import time
from pathlib import Path

import numpy as np
import openpmd_api as io

try:
    from mpi4py import MPI
    HAVE_MPI = True
except ImportError:
    HAVE_MPI = False

ME_C = 9.1093837015e-31 * 299792458.0      # kg m / s
ME_C2_MEV = 0.51099895069                   # electron rest energy, MeV


# ---------------------------------------------------------------- physics
def width(array, axis=(), factor=0.5):
    """Exact port of pp-ana dataManipulator.width (FWHM by edge crossing
    with nearest-pixel adjustment and linear interpolation)."""
    array = np.asarray(array, dtype=float)
    maxInd = int(np.argmax(abs(array)))
    if maxInd != 0:
        indLow = int(np.argmin(abs(array[:maxInd] - array[maxInd] * factor)))
        indUp = int(np.argmin(abs(array[maxInd:] - array[maxInd] * factor)))
        indUp += maxInd
    else:
        return 0.0
    if np.size(axis) == 0:
        return indUp - indLow
    axis = np.asarray(axis, dtype=float)
    if indLow == 0:
        xLow = axis[1]
    else:
        if abs(array[maxInd] * factor - array[indLow - 1]) > \
           abs(array[maxInd] * factor - array[indLow + 1]):
            indLow -= 1
        m = (array[indLow + 1] - array[indLow]) / (axis[indLow + 1] - axis[indLow])
        b = array[indLow] - m * axis[indLow]
        xLow = (array[maxInd] * factor - b) / m
    if indUp == len(array) - 1:
        xUp = axis[indUp]
    else:
        if abs(array[maxInd] * factor - array[indUp - 1]) < \
           abs(array[maxInd] * factor - array[indUp + 1]):
            indUp -= 1
        m = (array[indUp + 1] - array[indUp]) / (axis[indUp + 1] - axis[indUp])
        b = array[indUp] - m * axis[indUp]
        xUp = (array[maxInd] * factor - b) / m
    return float(xUp - xLow)


def energy_mev(px, py, pz):
    p2 = (px / ME_C) ** 2 + (py / ME_C) ** 2 + (pz / ME_C) ** 2
    return ME_C2_MEV * np.sqrt(1.0 + p2)


# ---------------------------------------------------------------- reader
class OpmdParticles:
    """Component-level partial reader over one openPMD iteration, matching
    pp-ana's opmd_reader semantics (unit_SI, position + positionOffset)."""

    def __init__(self, series_path, species):
        self.series = io.Series(series_path, io.Access.read_random_access)
        keys = sorted(self.series.iterations.keys())
        self.it = self.series.iterations[keys[-1]]
        if species not in self.it.particles:
            raise KeyError(f"species {species!r} not in {list(self.it.particles)}")
        self.sp = self.it.particles[species]
        self.n = int(self.sp["position"]["x"].shape[0])
        self.bytes_read = 0

    def _load(self, var, start, count):
        a = np.asarray(var.load_chunk([start], [count]))
        self.series.flush()
        self.bytes_read += a.size * a.itemsize
        return a.astype(float, copy=False)

    def chunk(self, start, count):
        sp, it = self.sp, self.it
        pos = np.empty((count, 3))
        for i, c in enumerate("xyz"):
            v = sp["position"][c]
            o = sp["positionOffset"][c]
            pos[:, i] = self._load(v, start, count) * v.unit_SI \
                + self._load(o, start, count) * o.unit_SI
        mom = np.empty((count, 3))
        for i, c in enumerate("xyz"):
            v = sp["momentum"][c]
            mom[:, i] = self._load(v, start, count) * v.unit_SI
        w = sp["weight"]
        return pos, mom, self._load(w, start, count) * w.unit_SI

    def stream(self, chunk):
        for s in range(0, self.n, chunk):
            yield self.chunk(s, min(chunk, self.n - s))


# ---------------------------------------------------------------- driver
def discover(root, one_file):
    found = []
    for base, dirs, _ in os.walk(root):
        for d in dirs:
            if d.endswith(".bp") and (d == one_file if one_file else True):
                found.append(os.path.join(base, d))
    if not one_file:
        found.sort(key=lambda p: int("".join(
            c for c in os.path.basename(p) if c.isdigit()) or 0))
    return found


class Acc:
    """Streaming accumulators: local hists + running min/max."""

    def __init__(self, bins, bins2d):
        self.bins, self.bins2d = bins, bins2d
        self.lim = {k: [np.inf, -np.inf] for k in
                    ("x", "z", "E", "divx", "divy")}

    def feed(self, pos, mom, w):
        E = energy_mev(mom[:, 0], mom[:, 1], mom[:, 2])
        divx = np.degrees(np.arctan2(mom[:, 0], mom[:, 2]))
        divy = np.degrees(np.arctan2(mom[:, 1], mom[:, 2]))
        for k, v in (("x", pos[:, 0]), ("z", pos[:, 2]), ("E", E),
                     ("divx", divx), ("divy", divy)):
            if v.size:
                lo, hi = self.lim[k]
                self.lim[k] = (min(lo, float(v.min())), max(hi, float(v.max())))

    def hist(self, pos, mom, w, rng):
        E = energy_mev(mom[:, 0], mom[:, 1], mom[:, 2])
        divx = np.degrees(np.arctan2(mom[:, 0], mom[:, 2]))
        divy = np.degrees(np.arctan2(mom[:, 1], mom[:, 2]))
        hE, eE = np.histogram(E, bins=self.bins, range=rng["E"], weights=w)
        hdx, edx = np.histogram(divx, bins=self.bins, range=rng["divx"], weights=w)
        hdy, edy = np.histogram(divy, bins=self.bins, range=rng["divy"], weights=w)
        h2E, e2 = np.histogram2d(pos[:, 2], pos[:, 0], bins=self.bins2d,
                                 range=[rng["z"], rng["x"]], weights=w * E)
        h2n, _, _ = np.histogram2d(pos[:, 2], pos[:, 0], bins=self.bins2d,
                                   range=[rng["z"], rng["x"]], weights=w)
        return (hE, eE), (hdx, edx), (hdy, edy), (h2E, h2n, e2)


def reduce_sum(comm, arr):
    if not HAVE_MPI:
        return arr.copy()
    out = np.zeros_like(arr)
    comm.Allreduce(arr, out, op=MPI.SUM)
    return out


class SerialComm:
    """No-MPI execution: the analysis is embarrassingly parallel (one
    unit per worker, tiny reduction at the end), so a single process is
    a valid run -- required on compute nodes that only expose MPI inside
    apptainer. Same call shapes as COMM_WORLD for the few used here."""

    def Get_size(self):
        return 1

    def Get_rank(self):
        return 0

    def Allreduce(self, sendbuf, recvbuf, op=None):
        # single rank: the local value IS the reduced value. IN_PLACE
        # (sendbuf is recvbuf) is already correct; copy otherwise.
        if recvbuf is not sendbuf and sendbuf is not None:
            recvbuf[...] = sendbuf

    def reduce(self, value, op=None, root=0):
        return value

    def Abort(self, code):
        raise SystemExit(code)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-d", "--dir", required=True)
    ap.add_argument("-f", "--file", default=None)
    ap.add_argument("-o", "--out", default="apeos_out")
    ap.add_argument("-s", "--species", default="electrons")
    ap.add_argument("--bins", type=int, default=1024)
    ap.add_argument("--bins2d", type=int, default=512)
    ap.add_argument("--chunk", type=int, default=1 << 20)
    ap.add_argument("--edges", choices=("pass", "probe"), default="pass")
    ap.add_argument("--probe", type=int, default=1 << 20,
                    help="particles sampled per unit in probe mode")
    ap.add_argument("--plot", action="store_true")
    a = ap.parse_args()

    if HAVE_MPI:
        comm = MPI.COMM_WORLD
        wtime, IN_PLACE = MPI.Wtime, MPI.IN_PLACE
        MINOP, MAXOP = MPI.MIN, MPI.MAX
    else:
        comm = SerialComm()
        wtime, IN_PLACE = time.perf_counter, None
        MINOP, MAXOP = None, None
    size, rank = comm.Get_size(), comm.Get_rank()
    units = discover(a.dir, a.file)
    if not units:
        if rank == 0:
            raise SystemExit(f"no .bp series under {a.dir}")
        comm.Abort(2)
    mine = units[rank::size]
    if rank == 0:
        os.makedirs(a.out, exist_ok=True)

    t0 = wtime()
    t_open = 0.0

    # ---- pass 1: limits (pp-ana semantics; probe mode = head sampling) ----
    acc = Acc(a.bins, a.bins2d)
    readers = []
    for u in mine:
        t = wtime()
        r = OpmdParticles(u, a.species)
        t_open += wtime() - t
        readers.append((u, r))
        if a.edges == "probe":
            pos, mom, w = r.chunk(0, min(a.probe, r.n))
            acc.feed(pos, mom, w)
        else:
            for pos, mom, w in r.stream(a.chunk):
                acc.feed(pos, mom, w)
    rng = {}
    for k, (lo, hi) in acc.lim.items():
        glo = np.array(lo if np.isfinite(lo) else -1.0)
        ghi = np.array(hi if np.isfinite(hi) else 1.0)
        comm.Allreduce(IN_PLACE, glo, op=MINOP)
        comm.Allreduce(IN_PLACE, ghi, op=MAXOP)
        rng[k] = (float(glo[0]), float(ghi[0]))
    if a.edges == "probe":  # clip guard: probe range padded so tails survive
        for k in rng:
            lo, hi = rng[k]
            pad = 0.02 * (hi - lo) + 1e-30
            rng[k] = (lo - pad, hi + pad)

    # ---- pass 2: stream and accumulate (or reuse probe pass, clipped) ----
    acc = Acc(a.bins, a.bins2d)
    hE = np.zeros(a.bins)
    hdx = np.zeros(a.bins)
    hdy = np.zeros(a.bins)
    h2E = np.zeros((a.bins2d, a.bins2d))
    h2n = np.zeros((a.bins2d, a.bins2d))
    e2 = None
    n_tot = 0
    for u, r in readers:
        for pos, mom, w in r.stream(a.chunk):
            (h, _), (hx, _), (hy, _), (h2, hn, ed) = acc.hist(pos, mom, w, rng)
            hE += h
            hdx += hx
            hdy += hy
            h2E += h2
            h2n += hn
            e2 = ed
            n_tot += pos.shape[0]

    # ---- reduce: the entire cross-rank traffic is these arrays ----
    hE = reduce_sum(comm, hE)
    hdx = reduce_sum(comm, hdx)
    hdy = reduce_sum(comm, hdy)
    h2E = reduce_sum(comm, h2E)
    h2n = reduce_sum(comm, h2n)
    bytes_local = comm.reduce(sum(r.bytes_read for _, r in readers),
                              op=(MPI.SUM if HAVE_MPI else None))
    dt = wtime() - t0

    if rank == 0:
        if e2 is None:  # nothing streamed (empty selection): degenerate axes
            e2 = (np.linspace(*rng["z"], a.bins2d + 1),
                  np.linspace(*rng["x"], a.bins2d + 1))
        ecx = 0.5 * (np.linspace(*rng["E"], a.bins + 1)[:-1]
                     + np.linspace(*rng["E"], a.bins + 1)[1:])
        divx_c = 0.5 * (np.linspace(*rng["divx"], a.bins + 1)[:-1]
                        + np.linspace(*rng["divx"], a.bins + 1)[1:])
        divy_c = 0.5 * (np.linspace(*rng["divy"], a.bins + 1)[:-1]
                        + np.linspace(*rng["divy"], a.bins + 1)[1:])
        fw_x = width(hdx, divx_c)
        fw_y = width(hdy, divy_c)
        mean_E = np.divide(h2E, h2n, out=np.zeros_like(h2E), where=h2n > 0)
        np.savez(os.path.join(a.out, "apeos_particles.npz"),
                 energy_edges=np.linspace(*rng["E"], a.bins + 1), energy=hE,
                 divx_edges=np.linspace(*rng["divx"], a.bins + 1), divx=hdx,
                 divy_edges=np.linspace(*rng["divy"], a.bins + 1), divy=hdy,
                 map_z_edges=e2[0], map_x_edges=e2[1],
                 map_mean_energy=mean_E, map_weight=h2n,
                 fwhm_divx=fw_x, fwhm_divy=fw_y)
        meta = {"dir": a.dir, "species": a.species, "units": len(units),
                "ranks": size, "particles": int(n_tot), "edges_mode": a.edges,
                "bytes_touched": int(bytes_local), "seconds": dt,
                "open_seconds": t_open,
                "rate_GiBs": (bytes_local / 2**30) / dt if dt > 0 else 0,
                "fwhm_divx_deg": fw_x, "fwhm_divy_deg": fw_y}
        with open(os.path.join(a.out, "apeos_particles.json"), "w") as fh:
            json.dump(meta, fh, indent=1)
        print(json.dumps(meta))

        if a.plot:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            from matplotlib.colors import LogNorm
            plt.semilogy(ecx, np.maximum(hE, 1e-3))
            plt.title(f"electron energy spectrum ({a.species})")
            plt.xlabel("E [MeV]")
            plt.savefig(f"{a.out}/energy.png", dpi=100)
            plt.close()
            for name, h, ax_, fw in (("x-z", hdx, divx_c, fw_x),
                                     ("y-z", hdy, divy_c, fw_y)):
                plt.semilogy(ax_, np.maximum(h, 1e-3))
                plt.title(f"Divergence {name} plane (FWHM: {fw:.2f}\u00b0)")
                plt.xlabel("Angle [\u00b0]")
                plt.savefig(f"{a.out}/divergence_{name}.png", dpi=100)
                plt.close()
            plt.imshow(mean_E.T, origin="lower", cmap="seismic",
                       extent=[rng["x"][0], rng["x"][1], rng["z"][0], rng["z"][1]],
                       norm=LogNorm())
            plt.colorbar().set_label("Mean energy [MeV]")
            plt.title("z-x mean energy")
            plt.xlabel("z [m]")
            plt.ylabel("x [m]")
            plt.savefig(f"{a.out}/energy_map_zx.png", dpi=100)
            plt.close()
            print(f"plots -> {a.out}/*.png")


if __name__ == "__main__":
    main()
