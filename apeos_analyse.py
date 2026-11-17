#!/usr/bin/env python3
"""apeos_analyse.py -- pp-ana's second step, deleted.

Reads an openPMD series (WarpX/PICongPU/fbpic) from ANY POSIX path --
Lustre scratch or an EOS/Helios mount -- and writes histograms directly.
No parquet intermediate: the filter and the analysis share one process
generation, exactly like pp-ana's opmd_filter.py read path (openpmd-api,
MPI rank per file), minus the export step that made the old workflow
complicated.

Partial I/O is not a feature added here: openpmd-api's BP5 engine reads
block-indexed byte ranges (pread at offsets), so pointing --dir at a
remote object-store mount turns every chunk load into a range request.
A leos-sealed unit is readable in place -- the .archived license is one
more file, and BP5 readers ignore foreign files.

    # one series on a scratch path, 4 ranks, momentum histogram
    mpirun -n 4 python apeos_analyse.py -d /lustre/runs -f diag00000100.bp \
        --species electrons --quantities momentum_z weight --bins 256

    # a sealed Helios unit through the EOS mount, same command
    mpirun -n 4 python apeos_analyse.py -d /eos/.../unit --species electrons

Outputs: <out>/apeos_hist.npz (+ .json meta). --parquet still writes the
reduced particle table for exploratory work; off by default (the lesson).
"""
import argparse
import json
import os
from pathlib import Path

import numpy as np

import mpi4py  # noqa: F401  (must precede openpmd for thread init order habits)
from mpi4py import MPI

import openpmd_api as io

PARTICLE_DEFAULTS = ("momentum_x", "momentum_y", "momentum_z", "weight")
FIELD_DEFAULTS = ("E_x", "E_y", "E_z")


def discover(dir_path, one_file):
    """WarpX one-file-per-step layout, same walk as pp-ana: every *.bp dir
    is one series; without --file, all of them, sorted by step number."""
    found = []
    for root, dirs, _files in os.walk(dir_path):
        for d in dirs:
            if not d.endswith(".bp"):
                continue
            p = os.path.join(root, d)
            if one_file:
                if d == one_file:
                    found.append(p)
            else:
                found.append(p)
    if not one_file:
        def step_key(p):
            digits = "".join(c for c in os.path.basename(p) if c.isdigit())
            return int(digits) if digits else 0
        found.sort(key=step_key)
    return found


def hist_field(iteration, record, component, bins, edges, acc):
    """Chunked slab read of one field component: bytes touched == bytes
    needed, which is the whole point of running on the archive."""
    rc = iteration[record][component]
    shape = rc.shape
    dtype = io.dtypes.dtype_to_numpy(rc.dtype)
    # slab along axis 0, roughly <= 64 MiB per load
    slab = max(1, (64 << 20) // max(1, int(np.prod(shape[1:])) * np.dtype(dtype).itemsize))
    off = [0] + [0] * (len(shape) - 1)
    ext = [slab] + list(shape[1:])
    while off[0] < shape[0]:
        ext[0] = min(slab, shape[0] - off[0])
        block = np.asarray(iteration.load_chunk(rc, off, ext))
        iteration.end_performance()
        h, _ = np.histogram(block, bins=edges)
        acc += h
        off[0] += slab


def hist_particles(iteration, species, quantity, bins, edges, acc, chunk=1 << 22):
    """Chunked particle read (same macro-particle ranges as the writer)."""
    rec = iteration.particles[species]
    parts = quantity.split("_", 1)
    rc = rec[parts[0]]
    rc = rc[parts[1]] if len(parts) == 2 else rc
    n = int(rc.shape[0])
    dtype = io.dtypes.dtype_to_numpy(rc.dtype)
    w = rec["weight"]
    for p0 in range(0, n, chunk):
        ext = min(chunk, n - p0)
        q = np.asarray(iteration.load_chunk(rc, [p0], [ext]))
        wt = np.asarray(iteration.load_chunk(w, [p0], [ext]))
        iteration.end_performance()
        h, _ = np.histogram(q, bins=edges, weights=wt)
        acc += h


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", "-d", required=True,
                    help="root containing *.bp series (scratch or EOS mount)")
    ap.add_argument("--file", "-f", default=None, help="single series, e.g. diag00000100.bp")
    ap.add_argument("--out", "-o", default="apeos_out")
    ap.add_argument("--species", "-s", default="electrons")
    ap.add_argument("--quantities", nargs="*", default=list(PARTICLE_DEFAULTS))
    ap.add_argument("--fields", nargs="*", default=list(FIELD_DEFAULTS))
    ap.add_argument("--bins", type=int, default=128)
    ap.add_argument("--probe-bytes", type=int, default=1 << 20,
                    help="particles sampled from the first chunk to set histogram edges")
    ap.add_argument("--parquet", action="store_true",
                    help="also dump the reduced particle table (opt-in relic of pp-ana step 2)")
    a = ap.parse_args()

    comm = MPI.COMM_WORLD
    size, rank = comm.Get_size(), comm.Get_rank()

    sims = discover(a.dir, a.file)
    if not sims:
        if rank == 0:
            raise SystemExit(f"no *.bp series under {a.dir}")
        comm.Abort(2)
    if rank == 0:
        os.makedirs(a.out, exist_ok=True)
        print(f"apeos: {len(sims)} series, {size} ranks "
              f"({os.uname().nodename}), reading in place (partial I/O)")
    comm.Barrier()

    # rank r takes series r, r+size, ... -- independent objects, no coordination
    mine = sims[rank::size]

    part_hist, field_hist = {}, {}
    part_cnt, tot_bytes = 0, 0
    t0 = MPI.Wtime()
    t_open = 0.0
    pq_frames = []
    for sim in mine:
        t = MPI.Wtime()
        series = io.Series(sim, io.Access.read_random_access)
        it = series.iterations[sorted(series.iterations.keys())[-1]]
        t_open += MPI.Wtime() - t

        # particle quantities: edges from a cheap probe of the first series only
        for q in a.quantities:
            if q not in part_hist:
                rec = it.particles[a.species]
                p = q.split("_", 1)
                rc = rec[p[0]]
                rc = rc[p[1]] if len(p) == 2 else rc
                probe = np.asarray(iteration_probe(it, rc, a.probe_bytes))
                lo, hi = float(probe.min()), float(probe.max())
                if lo == hi:
                    lo, hi = lo - 0.5, hi + 0.5
                part_hist[q] = (np.histogram_bin_edges(None, bins=a.bins,
                                                       range=(lo, hi)),
                                np.zeros(a.bins, dtype=np.float64))
            edges, acc = part_hist[q]
            hist_particles(it, a.species, q, a.bins, edges, acc)
            tot_bytes += it.particles[a.species][q.split('_')[0]].shape[0] * 8

        for fq in a.fields:
            rec, comp = fq.split("_", 1)
            if fq not in field_hist:
                rc = it[rec][comp]
                probe = np.asarray(rc.load_chunk([0] * len(rc.shape),
                                                 [1] * (len(rc.shape) - 1) + [min(1 << 20, rc.shape[-1])]))
                iteration_end(it)
                lo, hi = float(probe.min()), float(probe.max())
                if lo == hi:
                    lo, hi = lo - 0.5, hi + 0.5
                field_hist[fq] = (np.histogram_bin_edges(None, bins=a.bins,
                                                         range=(lo, hi)),
                                  np.zeros(a.bins, dtype=np.float64))
            edges, acc = field_hist[fq]
            hist_field(it, rec, comp, a.bins, edges, acc)

        if a.parquet:
            pq_frames.append(dump_particles(it, a.species, a.quantities))

    dt = MPI.Wtime() - t0

    # ---- reduce: the whole pipeline's output is these arrays ----
    out = {"part": {}, "field": {}}
    for q, (edges, acc) in part_hist.items():
        g = np.zeros_like(acc)
        comm.Allreduce(acc, g, op=MPI.SUM)
        out["part"][q] = (edges, g)
    for fq, (edges, acc) in field_hist.items():
        g = np.zeros_like(acc)
        comm.Allreduce(acc, g, op=MPI.SUM)
        out["field"][fq] = (edges, g)
    t_open_tot = comm.reduce(t_open, op=MPI.MAX)
    n_bytes = comm.reduce(tot_bytes, op=MPI.SUM)

    if rank == 0:
        npz = {f"part_{q}_counts": h for q, (_, h) in out["part"].items()}
        npz.update({f"part_{q}_edges": e for q, (e, _) in out["part"].items()})
        npz.update({f"field_{q}_counts": h for q, (_, h) in out["field"].items()})
        npz.update({f"field_{q}_edges": e for q, (e, _) in out["field"].items()})
        np.savez(os.path.join(a.out, "apeos_hist.npz"), **npz)
        gibs = (n_bytes / 2**30) / dt if dt > 0 else 0
        meta = {"dir": a.dir, "series": len(sims), "ranks": size,
                "species": a.species, "bytes_logical": n_bytes,
                "seconds": dt, "metadata_open_max_s": t_open_tot,
                "rate_gibs": gibs}
        with open(os.path.join(a.out, "apeos_hist.json"), "w") as f:
            json.dump(meta, f, indent=1)
        print(json.dumps(meta))
        print(f"apeos: histograms -> {a.out}/apeos_hist.npz")
        if a.parquet and pq_frames:
            import pandas as pd
            pd.concat(pq_frames).to_parquet(os.path.join(a.out, "reduced.parquet"))
            print(f"apeos: reduced table -> {a.out}/reduced.parquet")


def iteration_probe(it, rc, n):
    return it.load_chunk(rc, [0], [min(int(rc.shape[0]), n)])


def iteration_end(it):
    it.end_performance()


def dump_particles(it, species, quantities):
    import pandas as pd
    rec = it.particles[species]
    cols = {}
    for q in quantities:
        p = q.split("_", 1)
        rc = rec[p[0]]
        rc = rc[p[1]] if len(p) == 2 else rc
        cols[q] = np.asarray(it.load_chunk(rc, [0], list(rc.shape)))
    it.end_performance()
    return pd.DataFrame(cols)


if __name__ == "__main__":
    main()
