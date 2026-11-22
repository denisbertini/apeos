# APEOS — Analyse Plasma sur EOS

*à la manière de FLUKA (flukturiende kaskade) : **A**nalyse **P**lasma
**EOS** — « a-pé-os ». In Provence the **péoust** is the stone seat at
the village fountain: you go fill a jug, you never carry the fountain
home. EOS is the reservoir; APEOS is where the physics is drawn.*

*du déluge au ruisseau — from the deluge to the brook.*

Post-processing for plasma runs that treats the archive as the
computation site: an openPMD series (ADIOS2 BP5 / HDF5) sitting on
POSIX-visible storage — Lustre scratch or the Helios EOS mount — is read
chunk-by-chunk, reduced on the fly, and the pipeline's *only* output is
a small results file. No intermediate format, no bulk copies.

## Contract

| | |
|---|---|
| reads | openPMD series (BP5/HDF5) at any POSIX path: scratch or EOS mount |
| writes | results only — histograms/tables, MB-scale, never bulk data |
| parallelism | rank-per-unit (mpi4py) or single process; every read is a chunk range = partial read on EOS |
| physics I/O | openPMD-api chunk loads, component-level (position+momentum+weight) |
| never | invent a second storage layer (the pp-ana lesson) |

## Why this shape

Full design with cost model and mechanisms: [DESIGN.md](DESIGN.md).
Measured Lustre-vs-EOS-direct verdict and rates (2026-11-22):
[docs/eos-vs-lustre-2026-11.md](docs/eos-vs-lustre-2026-11.md).
For the storage/EOS team: [docs/message-to-eos-team.md](docs/message-to-eos-team.md).

- **BP5 is range-read native**: the per-writer `.idx` maps every block to
  exact byte offsets of `data.*`, so a dask chunk is an `pread(offset,
  len)` — exactly what an object store answers well, and what HDF5-with-
  contig-chunks also supports.
- **bbstage `Aggregate=on` output is the ideal citizen**: one
  `data.<node>` file per node instead of one per rank — fewer EOS
  objects, larger range reads, cheaper metadata opens per dask worker.
- **The reduction is the product.** A momentum histogram computed as a
  map-reduce over chunks moves every particle exactly once and keeps
  `n_bins × 8 B` at the end. Anything that copies 150 GiB to make a
  10 MB histogram is the previous project.

## The one number that decides everything

openPMD-api speaks POSIX; Helios arrives either through the EOS mount or
through ADIOS2's own remote-read path (native XRootD/HTTPS byte-range
gets — the mount is not a requirement). The architecture stands or falls
on **effective range-read throughput of the remote path** (and on
metadata-open latency, since each worker opens the series itself).

| quantity | scratch (Lustre) | EOS remote | verdict |
|---|---|---|---|
| sequential read, GiB/s | _tbd_ | _tbd_ | |
| random 4 MiB pread, GiB/s | _tbd_ | _tbd_ | |
| series metadata open, ms | _tbd_ | _tbd_ | |
| histogram end-to-end, GiB/s | _tbd_ | _tbd_ | |

`spike/read_bench.py` measures the first two on POSIX paths (mount
included); the native-protocol rows come from the ADIOS2 remote-read
prototype. Fill this table or the repo is a meme.

## Layout

```
apeos_particles.py     v2 core: transplanted pp-ana particle analysis
                       (energy spectrum, divergence+FWHM, z-x energy
                       map) reading openPMD directly -- no parquet;
                       mpi4py optional, runs on bare python 3.9
spike/
  read_bench.py        raw mount-vs-scratch I/O characterisation (no openPMD)
```

## Running on the compute nodes (bare python, no MPI on the host)

The cluster's compute nodes expose python 3.9 with no `$HOME` and MPI
only inside apptainer. apeos runs there without either: build one venv
on Lustre (visible from every node), all wheels, nothing compiled:

```bash
VENV=/lustre/home/d.bertini/envs/apeos
python3 -m venv $VENV && $VENV/bin/pip install -U pip
$VENV/bin/pip install "numpy<2.1" "scipy<1.13" matplotlib openpmd-api
# in the job (openpmd-api's wheel ships its own ADIOS2 with BP5 reader):
$VENV/bin/python apeos_particles.py -d /lustre/runs/diag -o out/
```

Same interpreter version and path on login and compute nodes is the one
requirement for a shared venv. `mpi4py` is a pure bonus: install it
inside the apptainer image (built against that image's MPI) and the same
script parallelizes with `mpirun` there; on the host it simply runs
single-process. Standalone-CPython escape hatch if 3.9 wheels go away:
unpack python-build-standalone on Lustre and venv from it.

## Donor lineage

APEOS is the second life of
[pp-ana](https://gitlab.gsi.de/d.bertini/pp-ana) (2024): pp-ana proved the
parallel read/filter machinery but paid for it with a two-format
workflow (openPMD in, split parquet out, analysis on the parquet).
APEOS keeps the first half, deletes the second, and adds the part that
matters in 2026: the input path may be a **leos-sealed Helios unit
mounted from EOS** — the archive layout is untouched by the seal, and
BP5's offset-indexed reads become range requests, so analysis runs on
the archive without restore. *du déluge au ruisseau.*

## Roadmap (only if the table says go)

1. multi-step / multi-run sweeps as dask collections over the series;
2. chunk↔BP5-block alignment: size dask chunks to writer blocks so every
   task is one range request, never two;
3. results discipline: histograms as netCDF/npz next to the archive
   manifest, keyed by the `bbstage.final`/leos seal;
4. job scripts for the dask cluster on the analysis partition.
