#!/bin/bash
# run_physics_chain.sh -- the typical-plasma-physics analysis, executed
# TWICE (identical code, identical physics selection), once per store:
#
#   Lustre scratch (bbstage-written bp5)      EOS unit sealed by leos (mount)
#        |                                          |
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
#   ROOT_E=/mnt/eos_sealed/bp5_native ./run_physics_chain.sh
#   knobs: ROOT_L ROOT_E STEPS SLICES NP PARTITION SKIP_PARTICLES
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
CONT=${CONT:-/lustre/rz/dbertini/io-tests/adios-tests/container/warpx_cpu_bb.sif}
ROOT_L=${ROOT_L:-/lustre/rz/dbertini/io-tests/adios-tests/warpx/diags/bp5_native}
ROOT_E=${ROOT_E:-}                       # EOS mount path of the SAME dataset
STEPS=${STEPS:-100}                      # particle snapshot(s): full sweep per step
SLICES=${SLICES:-0,50,100}               # plane-map steps (fields always: all steps)
NP=${NP:-8}                              # MPI ranks in-container = read streams
PARTITION=${PARTITION:-long}
OUT=${OUT:-chain_out}
export APPTAINER_BINDPATH=${APPTAINER_BINDPATH:-/lustre/rz/dbertini/}
export APPTAINER_CONFIGDIR=${APPTAINER_CONFIGDIR:-/tmp/$USER}
export OMPI_ALLOW_RUN_AS_ROOT=1 OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1

units_in() { find "$1" -maxdepth 1 -type d -name 'openpmd_*.bp5' 2>/dev/null | wc -l; }

# ---------------- preflight: the chain must actually contain the data ------------
NL=$(units_in "$ROOT_L"); [ "$NL" -gt 0 ] || { echo "FATAL: no openpmd_*.bp5 under $ROOT_L"; exit 1; }
if [ -z "$ROOT_E" ]; then
  echo "ROOT_E not set -- the chain has two legs. To fill the second:"
  echo "  1) leos archive $ROOT_L   (seal to EOS; skip if already sealed)"
  echo "  2) expose the unit on an EOS mount visible to the compute nodes"
  echo "     (eos-fusex is installed there; or copy back for a control leg)"
  echo "  3) re-run: ROOT_E=/path/to/mounted/bp5_native $0"
  exit 2
fi
NE=$(units_in "$ROOT_E"); [ "$NE" -eq "$NL" ] || { echo "FATAL: EOS unit count $NE != Lustre $NL ($ROOT_E) -- partial restore is not a chain test"; exit 1; }

echo "=== chain: $NL units  |  L=$ROOT_L  |  E=$ROOT_E"
echo "=== particles: steps=$STEPS (both species)  |  slices=$SLICES  |  streams/store=$NP"
mkdir -p $OUT

# one leg: single Slurm task, containerized OpenMPI gives NP parallel read
# streams (rank<->unit mapping inside the analyzers; MPI-optional fallback
# still correct at NP=1)
leg() { # leg <tool-script> <out-subdir> <store-root> [extra analyzer args...]
  local script=$1 out=$2 root=$3; shift 3
  srun -l -N1 -n1 -c "$NP" --partition="$PARTITION" --mem=16G \
       --job-name apeos-chain --time=04:00:00 \
    apptainer exec "$CONT" \
      mpirun --allow-run-as-root --oversubscribe -np "$NP" \
        python3 -u "$HERE/$script" -d "$root" -o "$OUT/$out" "$@"
}

# ---------------- 1) fields: every step (cheap: streamed sums + 1-D rows) --------
for tag in lustre eos; do
  root=$( [ "$tag" = lustre ] && echo "$ROOT_L" || echo "$ROOT_E" )
  leg apeos_fields.py "fields_$tag" "$root" --steps all --slices "$SLICES" --plot
done

# ---------------- 2) particles: selected snapshots, both species -----------------
if [ -z "${SKIP_PARTICLES:-}" ]; then
  for tag in lustre eos; do
    root=$( [ "$tag" = lustre ] && echo "$ROOT_L" || echo "$ROOT_E" )
    for sp in electrons ions; do
      leg apeos_particles.py "particles_${tag}_${sp}" "$root" \
          --steps "$STEPS" --edges probe -s "$sp" --plot
    done
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

echo
echo "=================================================================="
echo " chain adjudicated. Artifacts in $OUT/:"
echo "   chain_fields_report.txt      integrity PASS/FAIL + rate table  <-- the answer"
echo "   chain_fields_rates.png       Lustre vs EOS, GiB/s same bytes"
echo "   chain_fields_rates.png + chain_fields.png, lineouts/spectra/slices"
echo "   chain_electrons_report.txt / chain_ions_report.txt"
echo "=================================================================="
