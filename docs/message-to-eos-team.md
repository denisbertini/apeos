# To the EOS team — from the WarpX/analysis group

*After a few weeks of reading production data directly from Helios.
Companion to [eos-vs-lustre-2026-11.md](eos-vs-lustre-2026-11.md);
every claim below is backed by the measurements there.*

## What you now have

A store our physicists can analyze *from*, not just dump *to*.
Bit-exact (verified 9/9 against license-hashed bytes), ~2.75 GiB/s at
32 parallel readers via XRootD + capability tokens, no FUSE mount, no
restore. For scan-once analyses this replaces the copy-to-Lustre habit
— which means **less scratch hoarding and less pressure on your
parallel filesystem**: a capacity win for you, bought with
functionality for us.

## What made it work — please protect these

1. **XRootD-native + capability tokens is the access model that works
   for science codes.** ADIOS2 reads data files directly over
   `root://` with a `zteos64` token, no mount. Keep it first-class and
   documented — it is what saved us from FUSE, which is wrong for
   MPI-IO-style read patterns anyway.
2. **The documented token recipe works:** `eos token --tree --expires
   8h`, presented as `?authz=` on complete object URLs. We followed
   your docs; the implementation held up end to end.

## What still hurts — concrete asks

1. **Tokens.** 8 h TTL, no renewal, minted by hand on a login node,
   and two silent failure modes: an expired token answers
   `3010 public access level restriction`, and a token truncated by a
   grep-style extraction (tokens contain `-` and `~`) answers "all
   denied". Please add: renewal (or longer TTLs for read-only
   scopes), machine-parsable output, and an error message that
   literally says "token expired".
2. **No listing with a token.** `xrdfs` dirlist is token-blind on
   Helios, so every client must know exact object names. A
   token-scoped listing would simplify everything we write.
3. **Per-file open cost (~30–130 ms, MGM + token ceremony).**
   Streaming is fine; *opens* are the tax, and small-scattered
   analyses lose x6-8 to it. If MGM/redirector session caching can be
   tuned for repeated authenticated opens from one proxy, we volunteer
   as test users. Please also capacity-plan our pattern as *read*
   traffic now: bursts of ~32 clients x ~64 authenticated opens within
   seconds, per analysis step.
4. **One ingest-side trap worth fixing centrally:** ADIOS2 `.bp5`
   units should be **marked final** at seal time (no "actively
   written" state surviving a completed `leos` transfer). Otherwise
   honest readers emit false "truncated by an interrupted writer"
   alarms on perfect archives — nothing erodes trust in an archive
   faster than false bit-rot accusations.

## The positioning we recommend together

**EOS = archive + direct analysis. Lustre scratch = cache tier**
(`leos restore`, still king for interactive work and for shared units
where the restore cost amortizes over many readers). Not EOS *instead
of* Lustre. The measured data supports exactly this split, and users
need to remember only: *big reads go direct; many small reads restore
once.*

All numbers are reproducible by your team in one command
(`run_physics_chain.sh`, see the reproduce appendix of the companion
report) — we will happily walk you through it.
