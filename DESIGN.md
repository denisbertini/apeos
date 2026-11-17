# APEOS design — efficient post-processing of openPMD on EOS

The thesis: an openPMD/BP5 archive on an object store is already a
queryable layout; efficiency comes from *touching the right bytes* and
*issuing them the way the store likes*, not from copying the archive
somewhere friendlier.

## Cost model

Everything below is justified by one equation for the time of a query:

```
T  ≈  B_touched / (S · r_stream)   +   N_meta · L_meta   +   B_result · ε
      ───────────────────────────      ───────────────      ───────────
      (1) payload: bytes actually      (2) metadata: opens,  (3) result:
      read, spread over S streams       stats, listings       negligible
      at per-stream rate r_stream
```

A restore-then-analyze pipeline pays (1) twice (EOS→scratch, then
scratch→core) plus a copy nobody asked for. pp-ana v1 paid a hidden
fourth term: the parquet rewrite of everything. APEOS targets (1) and
(2) directly and keeps (3) at kilobytes.

## The two formats conspire in our favour

**BP5 (write side, WarpX via openPMD):** every logical array — each
particle *component*, each field component — is stored as independent
contiguous blocks; the per-writer `idx.*.greeb` files map block → (data
file, offset, length). `load_chunk(selection)` therefore reads *only*
the byte ranges of the selected component — no read amplification from
neighbouring data. A momentum histogram touches momentum_z + weight
bytes ≈ 1/6…1/10 of the particle payload. That single fact is the
largest efficiency term in the whole system, and it is free.

**EOS/Helios (read side):** each file is an object answering ranged
GETs; latency is ~ms per *request*, throughput grows with request size
(MB-class GETs stream) and with the number of concurrent streams. Small
files are latency-bound death; large objects + range requests are the
store's happy path. BP5's block-parallel reads are exactly that shape.

**leos seal:** layout-preserving by construction (the `.archived`
license is one extra file). A sealed unit opens in place — the archive
is the query target, restore never enters the story.

## The six mechanisms

1. **Component-level selection.** Queries request record components,
   never records, never whole files. Bytes touched is the physics's
   floor, and BP5 can meet it (see above).
2. **Block-aligned requests.** Chunk sizes are multiples of the BP5
   writer blocks (or the whole block), so every `pread` is one
   contiguous object range of ≥ ~1 MB: EOS sees large GETs, the FUSE
   mount sees no read-modify waste, and there is zero fragmentation.
   (Sub-block reads are allowed but never *manufactured*.)
3. **Two levels of stream parallelism, both coordination-free.**
   WarpX's one-file-per-step layout = N independent object trees ⇒
   task↔step is embarrassingly parallel. Within a step, one `data.*`
   per writer = N objects ⇒ streams scale without any file sharing.
   Target `S` from the store, not the rank count: throughput is
   `S · r_stream`, and EOS does not care about your MPI decomposition.
4. **Metadata discipline.** Per (step, process): one open, one index
   parse (`.greeb` + metadata, a few KB–MB), then only payload reads.
   The hash manifest shipped by leos is used as the directory/trust
   view — no recursive stat walks, seal verification costs one file.
   Repeated queries reuse the parsed index (in-process and across the
   MPI lifetime of the reader).
5. **Reduction pushdown (map–reduce over bins).** Each task histograms
   its chunks into fixed bins; the only cross-task traffic is an
   Allreduce of `n_bins × 8 B`. Particles never exist wholesale
   anywhere: memory is one chunk (≤ 64 MiB), output is a histogram.
   The pipeline's total output size is a physics constant, not a
   data-volume one — that is what "no intermediate format" *means*.
6. **Write-side synergy (bbstage).** `Aggregate=on` turns per-rank
   `data.*` + `idx.*` files into per-node ones: 8–64× fewer objects
   per step ⇒ term (2) shrinks at analysis time for free. The
   breaker keeps the archive's ingress honest during storms. The chain
   is not three tools passing files; it is one format design paid for
   twice.

## Negative design (what we refuse, and why)

* **No restore-for-analysis.** Restoring doubles term (1) and adds a
  copy whose only purpose would be to hide a mount problem.
* **No intermediate format.** pp-ana's parquet step made step 1
  write-bound and step 2 stale-prone. Histograms are results, not data.
* **No full-object streaming.** If bytes of an unqueried component are
  read, the selection is wrong — this is a bug class, not a tuning knob.
* **No scheduler ceremony in v1.** MPI rank↔unit already saturates the
  store; dask remains the interactive/multi-query layer if a second
  tier is ever needed (openPMD-api has it first-party).

## What must be measured (the README table)

| quantity | probe | decides |
|---|---|---|
| `r_stream` scratch vs mount, 4 MiB random | `spike/read_bench.py` | is the mount ≥ ~1 GiB/s/stream? |
| `L_meta` (open/first-byte ms) | `read_bench.py` first line | metadata term at N writers |
| open+index time per step | `apeos_analyse.py` JSON `metadata_open_max_s` | mechanism 4 in practice |
| end-to-end histogram rate | same, scratch vs sealed EOS unit | the thesis, one number |

If mount ≈ scratch: the design is simply true. If mount ≪ scratch:
(1) is latency-bound at the mount layer, and the honest answers are
XRootD-protocol reads or batch prefetch — *not* restore, and definitely
not parquet. Either outcome is a result; both are cheap to learn.
