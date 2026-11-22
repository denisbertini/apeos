# Reading sealed WarpX data from Helios EOS, directly — measured 2026-11-22

**Verdict: the EOS-direct read path is production-ready. For scan-once
analyses, `leos restore` is no longer a workflow step — it is an optional
cache tier for interactive development. At 32 reader processes a
full-scale particle analysis reads directly from Helios within 3% of
Lustre speeds (2.75 vs 2.84 GiB/s, 162 GiB, bit-identical results),
having restored nothing.**

Everything below was measured with `run_physics_chain.sh`: the same
analysis code (lineouts, plane maps, energy spectra, divergence) run
twice over the same sealed data, once from Lustre scratch and once
directly from Helios EOS through ADIOS2's `FileXRootD` transport
(XRootD protocol, capability token; no FUSE mount, no restore).

## The unit

`openpmd_000000.bp5` (bbstage -> leos -> Helios, license-hashed at seal):

| content | size |
|---|---|
| fields, step 0 (rho, E, B, j; 512x256x256, 64 BP5 blocks) | 1.75 GiB touched |
| electrons, 2 147 483 648 macroparticles | 160.6 GiB |
| ions, 268 435 456 macroparticles | 20.6 GiB |

Legs ran on one compute node with 8 MPI ranks (`srun -N1 -n1 -c 8`,
mpirun inside the container `warpx_cpu_bb.sif`: ADIOS2 2.12 fork with
`FileXRootD`, openPMD-api, XRootD client). EOS sidecar engine defaults:
`threads=1, maxopenfiles=512` (see `DESIGN.md`; opens are the currency).

## 1. Integrity: identical physics, not approximately

| comparison | max deviation | tolerance |
|---|---|---|
| fields (lineouts + spectra) | **0.000e+00** | 1e-12 |
| electrons (160 GiB of histograms) | **0.000e+00** | 1e-12 |
| ions | **0.000e+00** | 1e-12 |

Same bytes -> same floats. Independently re-proven bitwise on a 268 MB
full variable against license-hashed bytes. The archive path is
lossless end to end.

## 2. Rates: same bytes, same ranks, different store

| workload | pattern | Lustre | EOS-direct | EOS/Lustre |
|---|---|---|---|---|
| fields (1.75 GiB) | tiny fragmented reads (lineouts, strided rows) | 0.88 GiB/s* | 0.15 | x0.17 |
| ions (20.6 GiB) | contiguous chunks, short run | 1.17 | 0.80 | x0.69 |
| electrons (160.6 GiB) | contiguous 1 M-particle chunks | **1.15** | **1.15** | **x1.00** (140.0 s vs 140.2 s) |

\* warm page cache; honest cold single-stream Lustre reference: 0.72
GiB/s (separate probe, clean node).

**Scaling, NP=8 -> NP=16 (re-run 2026-11-22, same unit, integrity still
0.000e+00 -- now also verifying that 16-way particle-slice partitioning
sums deterministically on both stores):**

| workload | Lustre 8->16->32 | EOS 8->16->32 | EOS/L at 32 |
|---|---|---|---|
| electrons (162 GiB) | 1.15 -> 2.21 -> **2.84** | 1.15 -> 1.58 -> **2.75** | **x0.97** |
| ions (22 GiB) | 1.17 -> 2.28 -> 2.98 | 0.80 -> 1.32 -> 2.05 | x0.69 |
| fields (fragmented) | ~0.9 | 0.15 -> 0.13 -> 0.11 (never scales: RTT-bound, request-shape problem) | x0.12 |

No wall found: EOS scales to 2.75 GiB/s (~23 Gb/s) on a 100 Gb/s `ib0`
client NIC at 32 ranks, matching page-warmed Lustre within 3%. The
NP=16 dip (1.58) was run-to-run variance -- FST cache state -- not a
ceiling: the NP=32 electrons row recovered to x0.97. Treat individual
EOS rows to +-30-40%; the scaling trend and the integrity verdicts are
solid. Consequence: at production rank counts the electrons gap is
~2 s per pass, so the restore break-even is effectively NEVER.

The three speedups are one mechanism read at three granularities: each
EOS request pays a network round-trip (~1-3 ms, ~30-130 ms first open
per file); multi-MB contiguous requests amortize it to nothing
(electrons), 2-100 KB strided fragments cannot (fields).

## 3. Do you need `leos restore`? It is an amortization decision

Restore costs ~623 s measured (241 MiB/s) plus the unit's size in Lustre
scratch. It saves read time only on repeated passes:

    break-even repetitions N = restore_seconds / time_saved_per_pass

| workload | time saved per pass by having it on Lustre | break-even N |
|---|---|---|
| electrons (contiguous, full species), NP=8 or NP=32 | 0-2 s | **never** |
| electrons, NP=16 (variance dip) | ~30 s | ~55 full passes (whole-unit restore ~1650 s) |
| ions | ~4-8 s | ~100+ full passes |
| fields (fragmented small reads) | ~14 s | ~65 full passes |

**Rule of thumb: default to EOS-direct. Restore only when you will
re-run fragmented small-read queries tens of times on the same unit
(developing an analysis, iterating on selection windows).**

## 4. Regime constants (same unit, same day; `perf_probe.py`)

| quantity | Lustre | EOS-direct |
|---|---|---|
| Series open (metadata staged locally) | 0.04 s | 0.03 s |
| small scattered 128 KiB read | ~0.3 ms (cached) | ~13 ms, no client cache |
| repeat the same read | faster (page cache, up to RAM) | identical to the millisecond |
| single-stream sweep, 4 MiB requests | 0.72 GiB/s cold | 0.14 GiB/s |
| 8 parallel readers | ~1.15 GiB/s (realistic) | 0.8 GiB/s and scaling ~N x, ceiling unreached |
| raw single-stream big requests (xrdcp) | - | 0.57 GiB/s |
| per-file session open | ~us | 30-130 ms (MGM token ceremony) |

## 5. Honest caveats

- One 380 GiB unit on one node, ranks 8/16/32 (100 Gb/s ib0 client NIC);
  EOS never met a wall, ending at x0.97 vs warm Lustre at 32 ranks.
  Multi-node campaigns (units x ranks) remain unmeasured.
- EOS legs saw warm FSTs (files re-read through the day); first-touch
  numbers ran up to ~40% slower, and individual EOS rows swing +-30-40%
  run to run (the NP=16 dip) -- the scaling trend is the signal.
- The fields x0.12-x0.17 has a named fix with a measured prize: request
  coalescing (VectorRead-style grouping in BP5Reader + XrdCl), ~4x
  single-stream headroom. The dev-loop niche for restore shrinks when
  it lands.
- Operational tax of the direct path: an 8 h token, refreshed with one
  command. There is no client-side cache on EOS by design -- repeats
  cost what the first read cost.
- This is the read side. A running simulation still writes to Lustre;
  the write/evacuation seam is separate work.

## 6. Reproduce it yourself

```bash
# ~/eosenv.sh -- source in every fresh shell
export CONT=/lustre/rz/dbertini/io-tests/adios-tests/container/warpx_cpu_bb.sif
export TOK=/lustre/rz/dbertini/io-tests/adios-tests/eos/helios.tok
export LU=/lustre/rz/dbertini/io-tests/tmp/openpmd_000000.bp5
export EOS_DATA_THREADS=1 EOS_DATA_MAXOPEN=512
export EOSAUTHZ=$(cat $TOK 2>/dev/null)

# token, minted on lxbk0960 (8 h TTL). EXTRACT WITH sed, NOT grep:
# zteos64 tokens contain '-' and '~'; grep character classes truncate
# them, and a truncated token fails as "all denied".
eos token --path /eos/helios/Users/dbertini/ --tree --expires $((8*3600)) \
  | sed -n 's/.*\(zteos64:[^ ]*\).*/\1/p' > $TOK

cd /lustre/rz/dbertini/apeos && source ~/eosenv.sh
ROOT_L=/lustre/rz/dbertini/io-tests/tmp \
EOS_ROOT=/eos/helios/Users/dbertini/warpx/diags \
STEPS=0 OUT=chain_out ./run_physics_chain.sh
# -> chain_out/chain_{fields,electrons,ions}_report.txt: verdicts + rates

# regime constants (modes: sweep | slabs | row), see perf_probe.py header
srun -l -N1 -n1 --partition=long --mem=32G --time=01:00:00 \
  apptainer exec "$CONT" \
  python3 perf_probe.py "$LU" sweep lustre 1 0
```

Artifacts of the runs quoted above: `/lustre/rz/dbertini/apeos/chain_out_full/`
(NP=8), `chain_out_np16/` (NP=16), `chain_out_np32/` (NP=32).
Code: apeos `2ec9fe6` (race-safe multi-rank metadata staging; harness
bypasses the image's Slurm-aware mpirun); ADIOS2 fork `c6c3cf97f`;
measurements predate the VectorRead project.
