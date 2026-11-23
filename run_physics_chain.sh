#!/bin/bash
# run_physics_chain.sh -- the typical-plasma-physics analysis, executed
# TWICE (identical code, identical physics selection), once per store:
#
#   Lustre scratch (bbstage-written bp5)   EOS unit sealed by leos
#        |                                 (EOS_ROOT: DIRECT via FileXRootD,
#        |                                  no mount no restore; or ROOT_E:
#        |                                  a mount/restore control leg)
#        +--- apeos_fields.py   (all steps)  -------+   lineouts (wake Ez,
#        +--- apeos_particles.py (snapshots) -------+   envelope), EM energy,
#                                                   |   energy spectra,
#                          out_lustre/  out_eos/ ---+   divergence, plane maps
#                                        \          /
#                              apeos_compare.py  -->  INTEGRITY: the two
#                              stores must agree within 1e-12 (same bytes =>
#                              same floats; anything else = archive bit-rot)
#                              RATE: same bytes_touched, so seconds/GiB/s
#                              columns ARE the Lustre-vs-EOS comparison
#
# usage:
#   EOS_ROOT=/eos/helios/Users/<user>/warpx/diags ./run_physics_chain.sh    # direct
#   ROOT_E=/mnt/eos_sealed/bp5_native              ./run_physics_chain.sh    # mount leg
#   knobs: ROOT_L ROOT_E EOS_ROOT STEPS SLICES NP PARTITION SKIP_PARTICLES
#          EOS_TOKEN_FILE  SCAN=1 SCAN_STEPS=0,50,... SCAN_SPECIES="electrons"
# EOS-direct notes: remote listing is impossible with a capability token, so
# the EOS leg reuses the step list discovered under ROOT_L; the token must be
# fresh (8 h TTL, 30 min margin enforced in apeos_eos.token).
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONT=${CONT:-/lustre/rz/dbertini/io-tests/adios-tests/container/warpx_cpu_bb.sif}
ROOT_L=${ROOT_L:-/lustre/rz/dbertini/io-tests/adios-tests/warpx/diags/bp5_native}
ROOT_E=${ROOT_E:-}                       # EOS mount path of the SAME dataset
EOS_ROOT=${EOS_ROOT:-}                   # EOS dir with openpmd_XXXXXX.bp5 (direct leg)
EOS_TOKEN_FILE=${EOS_TOKEN_FILE:-/lustre/rz/dbertini/io-tests/adios-tests/eos/helios.tok}
STEPS=${STEPS:-100}                      # particle snapshot(s): full sweep per step
SLICES=${SLICES:-0,50,100}               # plane-map steps (fields always: all steps)
NP=${NP:-8}                              # MPI ranks in-container = read streams
PARTITION=${PARTITION:-long}
OUT=${OUT:-chain_out}
export APPTAINER_BINDPATH=${APPTAINER_BINDPATH:-/lustre/rz/dbertini/}
export APPTAINER_CONFIGDIR=${APPTAINER_CONFIGDIR:-/tmp/$USER}
export OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
export EOS_TOKEN_FILE                    # analyzers (in-container) enforce margin

units_in() { find "$1" -maxdepth 1 -type d -name 'openpmd_*.bp5' 2>/dev/null | wc -l; }

# ---------------- preflight: the chain must actually contain the data ------------
NL=$(units_in "$ROOT_L"); [ "$NL" -gt 0 ] || { echo "FATAL: no openpmd_*.bp5 under $ROOT_L"; exit 1; }
# the EOS-direct leg cannot enumerate remotely: steps come from the Lustre leg
STEPS_L=$(find "$ROOT_L" -maxdepth 1 -type d -name 'openpmd_*.bp5' \
          | sed -E 's/.*openpmd_0*([0-9]+)\.bp5$/\1/' | sort -n | paste -sd,)
if [ -n "$EOS_ROOT" ]; then
  [ -f "$EOS_TOKEN_FILE" ] || { echo "FATAL: EOS_TOKEN_FILE=$EOS_TOKEN_FILE missing"
    echo "  mint on lxbk0960: eos token --path /eos/helios/Users/<user>/ --tree"
    echo "    --expires \$(( \$(date +%s) + 8*3600 ))   (umask 077, store, chmod 600)"; exit 1; }
  AGE=$(( $(date +%s) - $(stat -c %Y "$EOS_TOKEN_FILE") ))
  [ "$AGE" -lt $(( 8*3600 - 1800 )) ] || { echo "FATAL: token $EOS_TOKEN_FILE is" \
    "$(( AGE/3600 )) h old (TTL 8 h, margin 30 min) -- re-mint before the campaign"; exit 1; }
  apptainer exec "$CONT" command -v xrdcp >/dev/null \
    || { echo "FATAL: xrdcp absent in $CONT -- metadata staging (apeos_eos.stage)" \
         "cannot run; rebuild the image or set EOS_META_DIR"; exit 1; }
elif [ -z "$ROOT_E" ]; then
  echo "neither EOS_ROOT (direct FileXRootD leg) nor ROOT_E (mount leg) set --"
  echo "the chain has one leg. To fill the second:"
  echo "  1) leos archive <unit>   (seal to EOS; skip if already sealed)"
  echo "  2) run direct:  EOS_ROOT=/eos/helios/<path/to/diags> $0"
  echo "     or mount:   ROOT_E=/path/to/mounted/bp5_native $0"
  exit 2
else
  NE=$(units_in "$ROOT_E"); [ "$NE" -eq "$NL" ] || { echo "FATAL: EOS unit count $NE != Lustre $NL ($ROOT_E) -- partial restore is not a chain test"; exit 1; }
fi

if [ -n "$EOS_ROOT" ]; then
  echo "=== chain: $NL units  |  L=$ROOT_L  |  E=EOS-direct $EOS_ROOT (token $(( $(date +%s) - $(stat -c %Y "$EOS_TOKEN_FILE") ))/28800 s old)"
else
  echo "=== chain: $NL units  |  L=$ROOT_L  |  E=$ROOT_E"
fi
echo "=== particles: steps=$STEPS (both species)  |  slices=$SLICES  |  streams/store=$NP"
mkdir -p $OUT

# one leg: single Slurm task, containerized OpenMPI gives NP parallel read
# streams (rank<->unit mapping inside the analyzers; MPI-optional fallback
# still correct at NP=1).
#
# NOTE on "sh: line 1: srun: command not found": the container's OpenMPI 5
# (prterun) has Slurm/PMIx scheduler awareness; seeing SLURM_JOB_ID in the
# inherited environment it tries to stage the app launch through the `srun`
# binary.  srun does not exist INSIDE the image, that attempt fails with
# exactly this message, and OMPI then falls back to its normal local
# launcher -- the app always ran 8-way regardless (confirmed by prterun
# messages and per-rank tracebacks).  We cannot install slurm into the
# image cheaply and must not let OMPI touch Slurm from inside anyway (the
# allocation is already ours), so leg() strips SLURM_* from the launcher's
# environment: OMPI then does a plain local launch, no srun attempt.
# srun itself stays OUTSIDE apptainer exec, where Slurm intends it.
leg() { # leg <tool-script> <out-subdir> <store-selector args...>
  local script=$1 out=$2; shift 2
  srun -l -N1 -n1 -c "$NP" --partition="$PARTITION" --mem=16G \
       --job-name apeos-chain --time=04:00:00 \
    apptainer exec "$CONT" bash -c '
      NP=$1; shift
      for v in $(compgen -e | grep -E "^SLURM_"); do unset "$v"; done
      LAUNCH=""
      for c in mpirun prterun orterun; do
        p=$(command -v "$c" 2>/dev/null) && { LAUNCH=$p; break; }
      done
      [ -n "$LAUNCH" ] || { echo "leg: no MPI launcher found" >&2; exit 9; }
      exec "$LAUNCH" --allow-run-as-root --map-by ":oversubscribe" \
           -np "$NP" "$@"
    ' leg "$NP" python3 -u "$HERE/$script" -o "$OUT/$out" "$@"
}
# store selector: POSIX root vs EOS-direct (steps mandatory: no remote listing)
if [ -n "$EOS_ROOT" ]; then
  EOS_SEL=(--eos-root "$EOS_ROOT")
  FIELDS_STEPS_E=$STEPS_L
else
  EOS_SEL=(-d "$ROOT_E")
  FIELDS_STEPS_E=all
fi

# ---------------- 1) fields: every step (cheap: streamed sums + 1-D rows) --------
leg apeos_fields.py "fields_lustre" -d "$ROOT_L" --steps all   --slices "$SLICES" --plot
leg apeos_fields.py "fields_eos"    "${EOS_SEL[@]}" --steps "$FIELDS_STEPS_E" --slices "$SLICES" --plot

# ---------------- 2) particles: selected snapshots, both species -----------------
if [ -z "${SKIP_PARTICLES:-}" ]; then
  for sp in electrons ions; do
    leg apeos_particles.py "particles_lustre_${sp}" -d "$ROOT_L" \
        --steps "$STEPS" --edges probe -s "$sp" --plot
    leg apeos_particles.py "particles_eos_${sp}" "${EOS_SEL[@]}" \
        --steps "$STEPS" --edges probe -s "$sp" --plot
  done
fi

# ---------------- 2b) scan: per-step evolution (the production analysis
# pattern: ONE pass over MANY steps -> scalar curves + fingerprint) -------
SCAN=${SCAN:-} SCAN_STEPS=${SCAN_STEPS:-all}
if [ -n "$SCAN" ]; then
  for sp in ${SCAN_SPECIES:-electrons}; do
    leg apeos_scan.py "scan_lustre_${sp}" -d "$ROOT_L" \
        --steps "$SCAN_STEPS" -s "$sp"
    leg apeos_scan.py "scan_eos_${sp}" "${EOS_SEL[@]}" \
        --steps "$([ "$SCAN_STEPS" = all ] && echo "$FIELDS_STEPS_E" || echo "$SCAN_STEPS")" -s "$sp"
  done
fi

# ---------------- 3) adjudicate: integrity verdict + rate table ------------------
apptainer exec "$CONT" python3 -u "$HERE/apeos_compare.py" \
    "$OUT/fields_lustre" "$OUT/fields_eos" --out-prefix "$OUT/chain_fields"
if [ -z "${SKIP_PARTICLES:-}" ]; then
  for sp in electrons ions; do
    apptainer exec "$CONT" python3 -u "$HERE/apeos_compare.py" \
        "$OUT/particles_lustre_${sp}" "$OUT/particles_eos_${sp}" \
        --out-prefix "$OUT/chain_${sp}" || true   # integrity verdict kept per report; fields exit code decides the campaign
  done
fi
if [ -n "$SCAN" ]; then
  for sp in ${SCAN_SPECIES:-electrons}; do
    apptainer exec "$CONT" python3 -u "$HERE/apeos_scan_compare.py" \
        "$OUT/scan_lustre_${sp}" "$OUT/scan_eos_${sp}" || true
  done
fi

echo
echo "=================================================================="
echo " chain adjudicated. Artifacts in $OUT/:"
echo "   chain_fields_report.txt      integrity PASS/FAIL + rate table  <-- the answer"
echo "   chain_fields_rates.png       Lustre vs EOS, GiB/s same bytes"
echo "   chain_fields_rates.png + chain_fields.png, lineouts/spectra/slices"
echo "   chain_electrons_report.txt / chain_ions_report.txt"
echo "=================================================================="
