#!/bin/bash
# bench_transfer.sh -- B3: archiving vs restoring throughput, GiB/s.
# Times ONE real transfer in each direction, sized by the same deck,
# and prints JSON per direction. leos commands are env templates so the
# harness survives leos CLI evolution:
#
#   ARCHIVE_CMD   transfers DECK_L (Lustre dir) INTO eos_root      (B3a)
#   RESTORE_CMD   transfers unit(s) from eos_root BACK to Lustre   (B3b)
#   SIZE_BYTES    bytes moved (default: du -sb of DECK_L)
#
# example (adjust to the real leos syntax in use):
#   DECK_L=/lustre/.../openpmd_000000.bp5 \
#   ARCHIVE_CMD='leos archive --dest eos:/eos/helios/Users/dbertini/warpx/diags $DECK_L' \
#   RESTORE_CMD='leos restore /eos/helios/Users/dbertini/warpx/diags/openpmd_000000.bp5 --dest $REST_TARGET' \
#   ./bench_transfer.sh
#
# Notes: archive rate = network + FST write path (striping, checksum at
# seal); restore rate = FST read + Lustre write (MDS metadata load is the
# usual limiter -- bp5 decks are 64+ files). Run on a compute node
# (srun) if leos requires it; the wrapper does not care where it runs.
set -euo pipefail

DECK_L=${DECK_L:?set DECK_L to the Lustre deck used for both legs}
SIZE_BYTES=${SIZE_BYTES:-$(du -sb "$DECK_L" | cut -f1)}

run_timed () { # run_timed <label> <cmd-template>
  local label=$1 cmd=$2 t0 t1 sec gib dir out
  echo "=== [$label] $cmd" >&2
  t0=$(date +%s%N)
  eval "$cmd"
  t1=$(date +%s%N)
  sec=$(awk "BEGIN{printf \"%.3f\", ($t1-$t0)/1e9}")
  gib=$(awk "BEGIN{printf \"%.3f\", $SIZE_BYTES/2^30}")
  printf '{"bench":"%s","bytes":%s,"GiB":%s,"seconds":%s,"GiBps":%s}\n' \
         "$label" "$SIZE_BYTES" "$gib" "$sec" "$(awk "BEGIN{printf \"%.3f\", $gib/$sec}")"
}

[ -n "${ARCHIVE_CMD:-}" ] && run_timed "archive_lustre_to_eos" "$ARCHIVE_CMD"
[ -n "${RESTORE_CMD:-}" ] && run_timed "restore_eos_to_lustre" "$RESTORE_CMD"
[ -z "${ARCHIVE_CMD:-}${RESTORE_CMD:-}" ] && {
  echo "set ARCHIVE_CMD and/or RESTORE_CMD (see header)"; exit 2; }
