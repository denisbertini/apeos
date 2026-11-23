# Helios EOS in the WarpX storage chain — recap & scoreboard (2026-11-23)

Audience: the system administrators who installed EOS. What it is, what we
measured, what it costs, what it delivers, what we found while doing it.

## The chain we ran, once, end to end, on one day

```
WarpX (128 ranks, fixed seed)
  └─ openPMD/BP5 diagnostics, two write modes ──► Lustre scratch (bbstage_agg / bp5_native)
        └─ leos archive: 3.02 TiB, checksummed + sealed + licensed ──► Helios EOS
              └─ analysis of the ARCHIVED copy, in place (XRootD + capability token,
                 no mount, no restore, no FUSE) ──► physics curves bit-identical
```

Deck: 21 checkpoints (steps 0–100), 146.51 GiB each, 3.02 TiB wire volume.
Analysis: 32 MPI ranks on 2 compute nodes, one rank per Slurm task
(srun-native, containerized), three workloads + a full evolution scan
(~2 B particles and 6 field components read per step).

## Verdict

> **EOS direct access is nearly competitive to Lustre.** On stream-dominated
> analysis it is outright faster (×1.9); Lustre retains the request-heavy
> regime, where its kernel page cache beats any XRootD protocol round-trip.
> Analysis of archived data runs **in place** on Helios — no mount, no
> restore — and returns **bit-identical physics**.

## Scoreboard

### 1. Integrity — the one number that matters first

| gate | comparison | result |
|---|---|---|
| G0 | bbstage-aggregate writes vs native bp5, 21 steps × 10 scalars | max rel dev **0.000e+00** |
| G1 | Lustre copy vs EOS-archived copy (fields, spectra, full scan) | max rel dev **0.000e+00** |

Archive round-trip preserves physics exactly; the write-path layer that feeds
the archive is neutral too. (Fixed seed makes "exact" testable, not just
"close".)

### 2. Analysis rate — same bytes, same code, different store

32 ranks / 2 nodes; identical `bytes_touched` per row; afternoon of normal
shared-Lustre load on both sides.

| workload | access pattern | Lustre | EOS direct | EOS/Lustre |
|---|---|---|---|---|
| fields (lineouts, slices, EM energy) | 21 units × 6 components, many opens, modest reads | 1.84 GiB/s | 0.47 GiB/s | **×0.26** |
| particles, electrons | 162 GiB in few huge sequential streams | 1.35 GiB/s | 2.52 GiB/s | **×1.87** |
| particles, ions | same shape, 22 GiB | 1.16 GiB/s | 1.60 GiB/s | ×1.38 |
| full evolution scan | mixed: chunked particle streams + field pass | 4.25 GiB/s | 2.97 GiB/s | ×0.70 |

Reading the board: the outcome is set by **requests per GiB**, not by raw
bandwidth. Fat streams: XRootD delivers and Lustre's afternoon contention
costs it the round. Request-heavy small reads: every authenticated open on
EOS is a protocol round-trip, while Lustre serves from the kernel path —
and often the page cache. Both halves of the verdict are measured wins,
which is why the sentence is defensible from either side.

### 3. Archive cost (getting data ONTO EOS)

| item | value |
|---|---|
| deck moved | 3 020 GiB, 21 sealed units |
| wall (one mover node, 8 files × 8 streams) | 65 min |
| verified end-to-end throughput | **0.78 GiB/s average** (1.01 first unit → 0.69 last) |
| what the mover did per byte | read twice from Lustre (integrity manifest + payload), adler32, remote size verify, seal, license |
| network stage alone | ~1.9 GiB/s ≈ 15 % of its 100 Gb HDR / IPoIB link — **the interconnect is not the limit** |
| failure behavior | crash-safe, resumable, idempotent (proved itself: resumed mid-run after a bug fix, zero rework) |

Practical meaning: a 3 TB production day archives, verified, in about an
hour, from one half-idle node, using zero compute cores. Scaling axes
(measured headroom, not promises): multi-node claims/array ingest —
bounded only by Helios ingest capacity, which nobody has measured yet;
and pipelining the hash/push stages (~1.3×).

The drift curve (1.01 → 0.69 GiB/s over 50 min) tracked **shared-Lustre
afternoon contention**, not the store or the network: the mover's pure-read
stage degraded more than its network stage while its CPU sat 90 % idle.
The archive is Lustre-read-bound before it is anything-bound.

### 4. Restore — the price of the cache tier (measured earlier, stands)

Restore pays for itself never, for scan-once analysis (that is what direct
reads are for); ~55–65 repeated passes on fragmented/dev workloads; plus
shared multi-reader units. Direct access is the default path; restore is an
opt-in cache tier, not a workflow step.

## What we found in the toolchain (with receipts)

1. **Capability tokens do not authenticate metadata ops.** Read works,
   stat fails (`XRootD applies authz to copy destinations only` — the eos
   client's own words). Any backup/archiving tool needs stat through the
   token to self-verify. *(Ask, already in the letter.)*
2. **`eos cp` parallel-mkdir race**: on missing directories, 8 parallel
   copies race to create it and EEXIST losers die fatal (`errno=17`
   mislabeled). One-directory pre-creation was the field workaround.
3. **Tool contribution, not just complaints**: we root-caused 2 + fixed it
   upstream in our archiver (`leos`, commit 26b397d: pre-create unit root,
   redact tokens from error text, classify EEXIST correctly; regression
   test included). Static binary available; happy to hand over the patch.
4. **Client provisioning is the pipeline's first bottleneck, not bandwidth.**
   2 of 20 cluster nodes can currently move data to EOS (eos client +
   credential present); 18 of 20 can *compute on* it via container+token.
   Concrete ask: eos client + credential bootstrap on the compute/main
   partition — the multi-node ingest experiment needs it to exist.

## Honesty ledger (so the numbers survive questioning)

- Rate rows are one-shot each, minutes apart, on identical byte counts;
  request-count noise on this cluster runs ±40 %, so single ratios finer
  than ~1.3× are not claimed as signal (×0.26 and ×1.87 are).
- Both stores saw the same loaded afternoon; absolute GiB/s are "this
  Tuesday", ratios are the portable number.
- EOS reads were token-authenticated XRootD from compute nodes; no FUSE,
  no staging copy, metadata sidecars fetched once per unit (~MiB).
- Archive rates include the mover's two Lustre read passes — the honest
  cost of verified archiving, stated with the mechanism.

## Reproduce (condensed)

```bash
source /lustre/rz/dbertini/eos/eosenv.sh            # CONT TOK D + EOS env
cd /lustre/rz/dbertini/apeos && git pull

# archive (mover node; pre-create unit dirs until leos 26b397d is deployed)
DECK_L=$D/bbstage_agg SIZE_BYTES=3243063164928 \
ARCHIVE_CMD='$HOME/bin/leos archive "$DECK_L" \
  --dest /eos/helios/Users/dbertini/io-tests/diags/bbstage_agg \
  --streams=8 --jobs=8' \
nohup ./bench_transfer.sh > b3a_agg.out 2>&1 &

# adjudicated chain: Lustre vs EOS-direct, production rank shape
NODES=2 NP=32 OUT=chain_g1_agg \
ROOT_L=$D/bbstage_agg \
EOS_ROOT=/eos/helios/Users/dbertini/io-tests/diags/bbstage_agg \
SCAN=1 SCAN_SPECIES=electrons ./run_physics_chain.sh
```

## Next measurements (queued)

- B3b: timed `leos restore` (cache-tier price, one deck).
- Multi-mover ingest probe (2→8 movers, announced first — it is a store
  capacity experiment, and a useful one for Helios capacity planning).
- Write-path benchmarks under Lustre load (bbstage's insurance value
  regime).
- EOS-team letter: add items 1–2 above with today's log excerpts.
