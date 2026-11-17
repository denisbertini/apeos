# APEOS — Analyse Plasma sur EOS

*à la manière de FLUKA (flukturiende kaskade) : **A**nalyse **P**lasma
**EOS** — « a-pé-os ». In Provence the **péoust** is the stone seat at
the village fountain: you go fill a jug, you never carry the fountain
home. EOS is the reservoir; APEOS is where the physics is drawn.*

*du déluge au ruisseau — from the deluge to the brook.*

Post-processing for plasma runs that treats the archive as the
computation site: an openPMD series (ADIOS2 BP5 / HDF5) sitting on
POSIX-visible storage — Lustre scratch or the Helios EOS mount — is read
through **openPMD-api's native dask bridge**, reduced chunk-by-chunk, and
the pipeline's *only* output is a small results file. No intermediate
format, no bulk copies.

## Contract

| | |
|---|---|
| reads | openPMD series (BP5/HDF5) at any POSIX path: scratch or EOS mount |
| writes | results only — histograms/tables, MB-scale, never bulk data |
| parallelism | dask (processes); each chunk read = offset/length I/O = partial read on EOS |
| physics I/O | openPMD-api `to_dask_array()` — first-party, no fork |
| never | invent a second storage layer (the pp-ana lesson) |

## Why this shape

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

openPMD-api speaks POSIX; Helios arrives through the EOS mount. The
architecture stands or falls on **effective range-read throughput of
the mount** (and on metadata-open latency, since a dask worker opens the
series itself).

| quantity | scratch (Lustre) | EOS mount | verdict |
|---|---|---|---|
| sequential read, GiB/s | _tbd_ | _tbd_ | |
| random 4 MiB pread, GiB/s | _tbd_ | _tbd_ | |
| series metadata open, ms | _tbd_ | _tbd_ | |
| histogram end-to-end, GiB/s | _tbd_ | _tbd_ | |

`spike/read_bench.py` measures the first two; `spike/histogram_spike.py`
the last two. Fill this table or the repo is a meme.

## Layout

```
spike/
  read_bench.py        raw mount-vs-scratch I/O characterisation (no openPMD)
  histogram_spike.py   dask momentum histogram from one series, timed
```

## Roadmap (only if the table says go)

1. multi-step / multi-run sweeps as dask collections over the series;
2. chunk↔BP5-block alignment: size dask chunks to writer blocks so every
   task is one range request, never two;
3. results discipline: histograms as netCDF/npz next to the archive
   manifest, keyed by the `bbstage.final`/leos seal;
4. job scripts for the dask cluster on the analysis partition.
