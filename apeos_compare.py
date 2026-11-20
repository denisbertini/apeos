#!/usr/bin/env python3
"""apeos_compare.py -- referee of the full-chain test.

Compares N apeos output directories (apeos_fields.* / apeos_particles.*
npz+json pairs) against the first one (the reference: the Lustre leg),
and answers the two questions the chain was built for:

  INTEGRITY   every analysis array must agree with the reference to
              within --tol (default 1e-12, relative to max|ref|), and
              the exact bookkeeping (bytes_touched, units, particles)
              must match EXACTLY. The stores hold the same bytes, so
              the same deterministic pipeline must return the same
              floats: a difference above tolerance here is archive
              bit-rot or a nondeterministic analysis -- both are news.

  RATE        bytes_touched is identical by construction (same
              physics selection), so seconds and GiB/s columns ARE
              the Lustre-vs-EOS comparison. Per-step rates are plotted
              too when the fields json carries them.

    python3 apeos_compare.py out_lustre out_eos [--out-prefix chain] [--tol 1e-12]

Exit code 0 = integrity PASS (rates are always reported). Artifacts:
chain_report.json + chain_rates.png + overlay figures
(lineouts / spectra / evolution: reference solid, others hairline).
"""
import argparse
import glob
import json
import os
import sys

import numpy as np

TOOLS = ("fields", "particles")
EXACT_JSON = ("bytes_touched", "units", "units_total", "ranks", "particles")
HAIR = dict(lw=0.6, alpha=0.85)


def label_of(d):
    return os.path.basename(d.rstrip("/")) or "run"


def load(d):
    run = {"dir": d, "label": label_of(d)}
    for tool in TOOLS:
        npz_p, js_p = (os.path.join(d, f"apeos_{tool}{ext}")
                       for ext in (".npz", ".json"))
        if os.path.exists(js_p):
            with open(js_p) as fh:
                meta = json.load(fh)
            arrs = ({k: v for k, v in np.load(npz_p).items()}
                    if os.path.exists(npz_p) else {})
            run[tool] = {"meta": meta, "npz": arrs}
    if len(run) <= 2:
        raise SystemExit(f"no apeos_* outputs found in {d}")
    return run


def cmp_array(ref, other):
    """max deviation, normalized by max|ref|; non-finite only where the
    reference is non-finite is tolerated, elsewhere it is a deviation."""
    ref, other = np.asarray(ref, float), np.asarray(other, float)
    if ref.shape != other.shape:
        return None, f"shape {ref.shape} vs {other.shape}"
    ok = np.isfinite(ref)
    if not np.array_equal(ok, np.isfinite(other)):
        return np.inf, "finite-pattern differs"
    if not ok.any():
        return 0.0, None
    dev = float(np.abs(ref[ok] - other[ok]).max())
    scale = float(np.abs(ref[ok]).max())
    rel = dev / scale if scale > 0 else dev
    return rel, None


def integrity(ref, other, tol, say):
    ok_all = True
    for tool in TOOLS:
        if tool not in ref or tool not in other:
            continue
        say(f"\n--- integrity: {tool}  ({ref['label']} vs {other['label']}) ---")
        # bookkeeping: exact
        for k in EXACT_JSON:
            v0, v1 = ref[tool]["meta"].get(k), other[tool]["meta"].get(k)
            if v0 is not None and v1 is not None and v0 != v1:
                ok_all = False
                say(f"FAIL {k}: {v0} vs {v1}  (selection/store bytes must match)")
        # arrays: tolerance-checked
        worst, worst_key = 0.0, None
        for k in sorted(ref[tool]["npz"]):
            if k not in other[tool]["npz"]:
                ok_all = False
                say(f"FAIL {k}: missing in {other['label']}")
                continue
            a, b = ref[tool]["npz"][k], other[tool]["npz"][k]
            if np.asarray(a).dtype.kind in "iu" and np.asarray(b).dtype.kind in "iu":
                if not np.array_equal(a, b):          # steps, counts: exact
                    ok_all = False
                    say(f"FAIL {k}: integer arrays differ")
                continue
            rel, err = cmp_array(a, b)
            if err:
                ok_all = False
                say(f"FAIL {k}: {err}")
                continue
            if rel > worst:
                worst, worst_key = rel, k
            if rel > tol:
                ok_all = False
                say(f"FAIL {k}: rel dev {rel:.3e} > tol {tol:.1e}")
        say(f"ok   {other['label']}: max deviation {worst:.3e} "
            f"(on {worst_key}) vs tol {tol:.1e}")
    return ok_all


def rate_table(runs, say, out_prefix):
    say("\n=== rate: same bytes touched, different stores ===")
    say(f"{'tool':>9} {'store':>14} {'GiB touched':>11} {'seconds':>9} {'GiB/s':>8}")
    rows, per_step = [], {}
    for run in runs:
        for tool in TOOLS:
            if tool not in run:
                continue
            m = run[tool]["meta"]
            gibs = m.get("bytes_touched", 0) / 2 ** 30
            sec = m.get("seconds", float("nan"))
            rate = gibs / sec if sec and sec > 0 else 0.0
            rows.append((tool, run["label"], gibs, sec, rate))
            say(f"{tool:>9} {run['label']:>14} {gibs:11.2f} {sec:9.1f} {rate:8.2f}")
            if "per_unit" in m:
                per_step.setdefault(tool, {})[run["label"]] = m["per_unit"]
    ref = runs[0]["label"]
    for tool in TOOLS:
        r0 = [r for r in rows if r[0] == tool and r[1] == ref]
        for r in rows:
            if r[0] == tool and r[1] != ref and r0 and r0[0][3] > 0:
                say(f"     speedup {tool} {r[1]}/{ref}: "
                    f"x{r0[0][3] / max(r[3], 1e-9):.2f}")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return rows
    if rows:
        fig, ax = plt.subplots(figsize=(5.5, 3.2))
        labels = sorted({r[1] for r in rows})
        tools = [t for t in TOOLS if any(r[0] == t for r in rows)]
        w = 0.8 / max(len(labels), 1)
        for i, lab in enumerate(labels):
            xs, hs = [], []
            for j, t in enumerate(tools):
                hit = [r[4] for r in rows if r[0] == t and r[1] == lab]
                if hit:
                    xs, hs = xs + [j], hs + [hit[0]]
            ax.bar(np.array(xs) + i * w - 0.4 + w / 2, hs, width=w, label=lab)
        ax.set_xticks(range(len(tools)))
        ax.set_xticklabels(tools)
        ax.set_ylabel("GiB/s (same bytes touched)")
        ax.set_title("analysis read rate, store vs store")
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(f"{out_prefix}_rates.png", dpi=110)
        plt.close(fig)
    for tool, series in per_step.items():
        fig, ax = plt.subplots(figsize=(5.5, 3.2))
        for i, (lab, lst) in enumerate(series.items()):
            ax.plot([u["step"] for u in lst], [u["rate_GiBs"] for u in lst],
                    "o-" if i == 0 else "s", lw=1.2 if i else 0.7, label=lab)
        ax.set_xlabel("step")
        ax.set_ylabel(f"{tool}: GiB/s per unit")
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(f"{out_prefix}_rates_{tool}.png", dpi=110)
        plt.close(fig)
    return rows


def overlays(runs, out_prefix, say):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    ref = runs[0]
    if "fields" in ref:
        fig, (a1, a2) = plt.subplots(1, 2, figsize=(9, 3.4))
        for i, run in enumerate(runs):
            if "fields" not in run:
                continue
            npz = run["fields"]["npz"]
            st = dict({}, **({} if i == 0 else HAIR))
            a1.plot(npz["lineout_axis"] * 1e6, npz["lineout_ez"][-1],
                    label=run["label"], **st)
            a2.plot(npz["steps"], npz["em_energy"], "o" if not i else "s",
                    label=run["label"], **st)
        a1.set_xlabel("propagation [um]")
        a1.set_ylabel("$E_z$ on axis")
        a1.set_title("final-step wake lineout")
        a2.set_xlabel("step")
        a2.set_ylabel("EM field energy [J]")
        a2.set_title("EM energy evolution")
        a1.legend(fontsize=7)
        fig.tight_layout()
        fig.savefig(f"{out_prefix}_fields.png", dpi=110)
        plt.close(fig)
    if "particles" in ref:
        fig, ax = plt.subplots(figsize=(5, 3.6))
        for i, run in enumerate(runs):
            if "particles" not in run:
                continue
            npz = run["particles"]["npz"]
            e = 0.5 * (npz["energy_edges"][:-1] + npz["energy_edges"][1:])
            ax.loglog(e, np.maximum(npz["energy"], 1e-3),
                      label=run["label"], **({} if i == 0 else HAIR))
        ax.set_xlabel("E [MeV]")
        ax.set_ylabel("dN/dE")
        ax.set_title("energy spectrum, store overlay")
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(f"{out_prefix}_particles.png", dpi=110)
        plt.close(fig)
    say(f"figures -> {out_prefix}_*.png")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dirs", nargs="+",
                    help="apeos output dirs; FIRST is the reference (Lustre)")
    ap.add_argument("--out-prefix", default="chain")
    ap.add_argument("--tol", type=float, default=1e-12)
    a = ap.parse_args()

    lines = []

    def say(*args):
        print(*args)
        lines.append(" ".join(str(x) for x in args))

    runs = [load(d) for d in a.dirs]
    ok = all(integrity(runs[0], o, a.tol, say) for o in runs[1:])
    rate_table(runs, say, a.out_prefix)
    overlays(runs, a.out_prefix, say)

    say(f"\n============ CHAIN RESULT: {'PASS' if ok else 'FAIL'} "
        f"(integrity within {a.tol:.0e}; rates above) ============")
    with open(f"{a.out_prefix}_report.json", "w") as fh:
        json.dump({"runs": [r["label"] for r in runs], "tol": a.tol,
                   "integrity_pass": ok}, fh, indent=1)
    with open(f"{a.out_prefix}_report.txt", "w") as fh:
        fh.write("\n".join(lines) + "\n")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
