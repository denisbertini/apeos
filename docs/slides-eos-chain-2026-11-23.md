---
marp: true
theme: default
paginate: true
style: |
  section { font-size: 24px; }
  h1 { font-size: 34px; }
  table { font-size: 21px; }
---

<!-- _class: lead -->
# EOS as a working storage tier
### WarpX → Lustre → Helios: one production day, end to end, with receipts
*GSI RZ — D. Bertini — 2026-11-23*
*full report: `/lustre/rz/dbertini/eos/recap-eos-chain-2026-11-23.md`*

---

# Why this exercise

- WarpX campaigns produce **~3 TiB per production day**; scratch is finite,
  shared, and every simulation wants the hot tier.
- Helios EOS is available as the archive tier — but the traditional pattern
  is **archive = graveyard**: ship bytes, lose access, restore (slowly)
  when someone wants data.
- The real question, for admins and for us: **can an archive be a working
  storage tier?** Not "does it store" — "can science run on it".

**Three questions, answered with numbers today:**
1. **Integrity** — does the archived copy return the *same physics*? (exact, not "close")
2. **Access** — how fast is real analysis reading the archive **in place**, vs Lustre?
3. **Economics** — what does the pipeline cost (archive, restore, scratch freed),
   and when is direct read vs restore the right path?

Method up front: deterministic physics (fixed seed ⇒ *bit-identical* is testable),
production paths (ADIOS2/BP5, srun-native MPI, the site's own archiving toolchain),
both stores measured minutes apart on identical bytes.

---

# The answers, in order

## ① yes, bit-identical ② nearly competitive — faster on streams ③ ~1 h per 3 TiB, verified

**The chain, run once end to end:**
WarpX 128 ranks → 21 checkpoints × 146.5 GiB (**3.02 TiB**) → verified archive
(checksum + seal + license) → full analysis **of the archived copy, in place**
(XRootD + capability token; **no mount, no restore, no FUSE**).

**Verdict:** *EOS direct access is nearly competitive to Lustre. On
stream-dominated analysis it is outright faster (×1.9); Lustre retains the
request-heavy regime, where its kernel page cache beats any XRootD
round-trip.*

---

# ② Analysis rate — same code, same bytes, minutes apart

| analysis workload (32 ranks, 2 nodes) | Lustre | EOS direct | winner |
|---|---|---|---|
| particle spectra — 162 GiB, fat streams | 1.35 GiB/s | **2.52 GiB/s** | **EOS ×1.87** |
| full evolution scan — mixed | **4.25** | 2.97 | Lustre ×1.4 |
| field lineouts/slices — many opens, small reads | **1.84** | 0.47 | Lustre ×3.9 |

The outcome is set by **requests per GiB**, not bandwidth:
fat sequential streams → XRootD delivers, EOS wins outright.
Many authenticated opens → protocol round-trips dominate, kernel path wins.
Both halves are measured wins — that is what "nearly competitive" means.

*(figures: `chain_electrons_rates.png`, `chain_fields_rates.png`)*

---

# ③ Economics — archive, access, restore

**Archive (measured today):** 3.02 TiB in **65 min from one mover node** =
**0.78 GiB/s fully verified** (integrity pass + transfer + seal + license).
Network stage used **15 % of its 100 Gb IPoIB link** → headroom is in
multi-mover fan-out and store ingest capacity, not the wire.
*A 3 TB production day lands verified in ~1 h, zero compute cores touched.*

**Access (measured):** direct reads make scan-once workflows archive-native —
restore is not needed to analyze.

**Restore (measured earlier):** break-even at ~55–65 repeated passes /
shared multi-reader units → restore = **opt-in cache tier, not workflow**.

**Scratch:** archive + license-verified purge frees 3 TiB/day safely.

---

# ① Why you can trust the physics: integrity gates

Fixed seed makes equality *testable*, not a tolerance discussion:

| gate | comparison | result |
|---|---|---|
| G0 — write path | bbstage-aggregate ≡ native bp5, 21 steps × 10 quantities | **0.000e+00** |
| G1 — store round-trip | Lustre vs archived-on-EOS: fields, spectra, full scan | **0.000e+00** |

The archive preserves physics exactly; the write-path layer feeding it is
neutral too. Archive seal is self-verifying (per-file size + adler32,
license written last, resumable after crashes — proven in anger today).

---

# Findings while measuring — with fixes, not complaints

- Capability tokens authenticate reads and writes but **not metadata
  (stat)**: tools cannot self-verify through a token. *(ask filed, with
  the tool's own error text as evidence)*
- `eos cp` parallel-mkdir race (EEXIST treated fatal) → **root-caused and
  patched upstream** in our archiver, regression-tested; patch offered.
- **2 of 20 cluster nodes can currently move data to EOS** (client +
  credential present). Client **provisioning, not bandwidth**, is the
  pipeline's first bottleneck. *(the concrete ask)*

**Honesty ledger:** single-shot ratios claimed only beyond the measured
±40 % noise floor; absolutes are "this loaded Tuesday", the ratios travel;
all rates use identical byte counts on both stores.

---

# Conclusions & next

- **EOS direct access works for science.** Production analysis runs on the
  archive, bit-exactly, at near-scratch speeds — and faster than a loaded
  scratch on streaming workloads.
- Lustre and EOS are complementary, and the boundary is **request density**,
  not size: hot request-heavy interactive work on scratch; sealed units,
  stream analysis, and shared/legacy datasets on EOS direct.
- The toolchain gaps found today are small, named, and partly fixed by us.

**Next measurements:** timed restore (cache-tier price) · announced
multi-mover ingest probe (measures Helios ingest ceiling — useful for
capacity planning) · write-path benchmarks under Lustre load.

*Reproduce: `eosenv.sh` + `bench_transfer.sh` + `run_physics_chain.sh`,
commands in the recap. Data: `/lustre/rz/dbertini/eos/`.*
