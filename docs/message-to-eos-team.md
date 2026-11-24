# To the EOS team — from the WarpX/analysis group

*After a few weeks of reading production data directly from Helios,
plus a joint debugging day on 2026-11-24. Companion to
[eos-vs-lustre-2026-11.md](eos-vs-lustre-2026-11.md) and
[recap-eos-chain-2026-11-23.md](recap-eos-chain-2026-11-23.md); every
claim below is backed by the measurements there.*

## What you now have

A store our physicists can analyze *from*, not just dump *to*.
Bit-exact (verified against license-hashed bytes, 3 TB campaign
archived at 0.776 GiB/s and re-analyzed in place), ~2.75 GiB/s at
32 parallel readers via XRootD + capability tokens, no FUSE mount, no
restore. For scan-once analyses this replaces the copy-to-Lustre habit
— which means **less scratch hoarding and less pressure on your
parallel filesystem**: a capacity win for you, bought with
functionality for us.

## Resolved since the first draft: metadata through tokens works

Our first letter asked you to *fix* stat/list through capability
tokens. Before sending it we tested more carefully, and the honest
conclusion is: **the server honors the token on metadata RPCs when the
CGI rides the path operand; our tools were not sending it.**

Test matrix from Helios, 2026-11-24 (`zteos64` token, single user tree):

| request | result |
|---|---|
| `xrdfs <host> stat <path>` (no credential) | 3010 public access level restriction |
| `XrdSecsssENDORSEMENT=<tok> xrdfs <host> stat <path>` | denied (ignored) |
| `EOS_SESSION_TOKEN=<tok> xrdfs <host> stat <path>` | denied (ignored) |
| `xrdfs <host> stat "<path>?authz=<tok>"` | **works** — Path/Id/Size |
| `xrdfs <host> ls "<path>?authz=<tok>"` | **works** — full listing |
| `xrdfs <host> query dirlist ...` | "invalid arguments" on this build — use `ls` |

Our archiver (`leos`) now attaches `?authz=` to every metadata
operand, and the result is the property every archiving tool needs:
**a 146 GiB archived unit verified — license download, token-scoped
listing, per-file size checks — with the capability token as the
*only* credential.** No certificate leaves its login node.

Two small asks remain from this finding:

1. **Document the per-URL `?authz=` metadata form** (stat/ls/mkdir/rm
   honor it; the session env-var forms do **not** on the current
   branch). That one paragraph in your docs saves every dependent
   tool a week of archaeology like ours.
2. FYI: upstream guidance we found recommends EOS 5.4+/XRootD
   5.6+ for session-level token support on metadata. No pressure —
   per-URL works for us now. Our patch is public (`leos`, commit
   f78235b), happy to hand it over.

## What still hurts — concrete asks

1. **Tokens.** 8 h TTL, no renewal, minted by hand on a login node,
   and two silent failure modes: an expired token answers
   `3010 public access level restriction`, and a token truncated by a
   grep-style extraction (tokens contain `-` and `~`) answers "all
   denied". Please add: renewal (or longer TTLs for read-only
   scopes), machine-parsable output, and an error message that
   literally says "token expired".
2. **Per-file open cost (~30–130 ms, MGM + token ceremony).**
   Streaming is fine; *opens* are the tax, and small-scattered
   analyses lose x6-8 to it. If MGM/redirector session caching can be
   tuned for repeated authenticated opens from one proxy, we volunteer
   as test users. Please also capacity-plan our pattern as *read*
   traffic now: bursts of ~32 clients x ~64 authenticated opens within
   seconds, per analysis step.
3. **One ingest-side trap worth fixing centrally:** ADIOS2 `.bp5`
   units should be **marked final** at seal time (no "actively
   written" state surviving a completed `leos` transfer). Otherwise
   honest readers emit false "truncated by an interrupted writer"
   alarms on perfect archives — nothing erodes trust in an archive
   faster than false bit-rot accusations.
4. **`eos cp` parallel-mkdir race** (kept from the first draft, with
   root cause): on missing directories, parallel copies race to
   create them and EEXIST losers die fatal (`errno=17` mislabeled as
   an auth-class error). We fixed our side (pre-create + tolerate
   EEXIST); a client-side patch would spare everyone.

## The two things we would like to schedule with you

1. **An announced many-node ingest test.** Our archive measured
   0.776 GiB/s from a single mover; the store clearly has headroom.
   The self-draining-staging architecture we want to propose needs
   the number nobody has: what does Helios ingest absorb when many
   compute nodes push simultaneously? We bring the load generator and
   the measurements; we just ask that it is *you* who schedule it.
2. **Client tools on the compute image.** Smaller ask than our first
   draft: we no longer need certificates or the `eos` console on
   compute nodes — token + container already works everywhere.
   Shipping `xrootd >= 5.x` client tools (`xrdcp`, `xrdfs`, libs) in
   the compute image would just remove the Apptainer indirection for
   drains and daemons.

## The positioning we recommend together

**EOS = archive + direct analysis + verified drain target. Lustre
scratch = burst buffer and hot cache**, not a museum: with automated
archive → verify → license → purge, scratch sized in *days of runs*
instead of terabytes of history. The measured data supports exactly
this split, and users need to remember only: *big reads go direct;
many small reads restore once; verified copies leave scratch on
their own.*

All numbers are reproducible by your team in one command
(`run_physics_chain.sh`, see the reproduce appendix of the companion
report) — we will happily walk you through it.
