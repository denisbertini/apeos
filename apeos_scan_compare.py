# apeos_scan_compare.py -- physics-validation adjudicator for apeos_scan:
# match two scan.json files step-by-step, compare every scalar. The scan
# is pure streaming sums with identical chunk/rank partition on both
# legs, so the expected deviation is EXACTLY zero; we still report a
# relative deviation per quantity so any nonzero value is instantly
# diagnosable (which quantity, which step).
# usage: apeos_scan_compare.py DIR_LUSTRE DIR_EOS [--tol 1e-12]
import argparse
import json
import math
import os
import sys


def load(d):
    rows, totals = {}, None
    with open(os.path.join(d, "scan.json")) as fh:
        for line in fh:
            r = json.loads(line)
            if r.get("kind") == "totals":
                totals = r
            else:
                rows[r["step"]] = r
    return rows, totals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir_a")
    ap.add_argument("dir_b")
    ap.add_argument("--tol", type=float, default=1e-12)
    ap.add_argument("--out-prefix", default=None)
    a = ap.parse_args()

    ra, ta = load(a.dir_a)
    rb, tb = load(a.dir_b)
    common = sorted(set(ra) & set(rb))
    if not common:
        sys.exit("no common steps between the two scans")

    scalars = sorted({k for k in ra[common[0]]
                      if k not in ("step", "species", "bytes", "seconds")})
    worst = {k: (0.0, None) for k in scalars}   # qty -> (rel dev, step)
    for s in common:
        for k in scalars:
            va, vb = ra[s].get(k), rb[s].get(k)
            if va is None or vb is None:
                continue
            d = abs(va - vb)
            rel = d / max(abs(va), abs(vb), 1e-300) if d else 0.0
            if rel > worst[k][0]:
                worst[k] = (rel, s)

    ok = all(rel <= a.tol for rel, _ in worst.values())
    print(f"\n--- scan validation: {a.dir_a} vs {a.dir_b} "
          f"({len(common)} steps matched) ---")
    print(f"{'quantity':<16} max rel dev        worst step   verdict")
    for k in scalars:
        rel, s = worst[k]
        print(f"{k:<16} {rel:12.3e}   {str(s):>10}   "
              f"{'ok' if rel <= a.tol else 'FAIL'}")
    miss = set(ra) ^ set(rb)
    if miss:
        print(f"WARNING: steps present in only one store: {sorted(miss)}")
        ok = False

    if ta and tb:
        gb_a, gb_b = ta["bytes_touched"] / 2**30, tb["bytes_touched"] / 2**30
        print(f"\n--- scan rate: same steps, same bytes ---")
        print(f"{'store':<24} steps  GiB      seconds   GiB/s")
        for name, t in ((a.dir_a, ta), (a.dir_b, tb)):
            g = t["bytes_touched"] / 2**30
            print(f"{name:<24} {t.get('steps', len(common)):>5}  "
                  f"{g:7.2f}  {t['seconds']:>8.1f}  {g / t['seconds']:6.3f}")
        r_a, r_b = gb_a / ta["seconds"], gb_b / tb["seconds"]
        print(f"speedup {a.dir_b}/{a.dir_a}: x{r_b / r_a:.2f}")

    print(f"\n============ SCAN RESULT: "
          f"{'PASS' if ok else 'FAIL'} "
          f"(all scalars within {a.tol:g} relative) ============")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
