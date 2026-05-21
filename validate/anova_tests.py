"""
anova_tests.py — Pruebas ANOVA sobre datos reales de AgroIA Colombia
=====================================================================
Ejecuta 3 pruebas ANOVA con datos reales del pipeline:

  1. Precipitación mensual  vs  Fase ENSO  (El Niño / La Niña / Neutro)
  2. Precio de insumos      vs  Tipo       (fertilizante / agroquimico / semilla / …)
  3. Precipitación mensual  vs  Trimestre  (Q1 ene-mar / Q2 abr-jun / Q3 jul-sep / Q4 oct-dic)

Cada prueba sigue el protocolo estadístico completo:
  Step 1 — Levene  (homogeneidad de varianzas)
  Step 2 — ANOVA   (scipy.stats.f_oneway)
  Step 3 — Tukey HSD post-hoc si p < 0.05

Genera boxplots en data/quality_reports/anova_*.png y una tabla resumen.

Uso:
    python -m validate.anova_tests
    python -m validate.anova_tests --verbose
"""
import argparse
import io
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path

# Forzar UTF-8 en stdout para Windows
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import scipy.stats as stats
from statsmodels.stats.multicomp import pairwise_tukeyhsd

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config.settings import DATA_RAW, DATA_PROCESSED

REPORT_DIR = Path(__file__).resolve().parent.parent / "data" / "quality_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

PALETTE = {
    "El Niño":      "#E8604C",
    "La Niña":      "#4C9BE8",
    "Neutro":       "#6DBF67",
    "fertilizante": "#F4A261",
    "agroquimico":  "#E76F51",
    "semilla":      "#2A9D8F",
    "combustible":  "#E9C46A",
    "mano_de_obra": "#264653",
    "Q1 (Ene-Mar)": "#A8DADC",
    "Q2 (Abr-Jun)": "#457B9D",
    "Q3 (Jul-Sep)": "#1D3557",
    "Q4 (Oct-Dic)": "#E63946",
}
DEFAULT_COLOR = "#888888"


# ─────────────────────────────────────────────────────────────────────
#  Resultado de cada prueba
# ─────────────────────────────────────────────────────────────────────

@dataclass
class AnovaResult:
    nombre: str
    variable: str
    factor: str
    grupos: list[str]
    n_por_grupo: dict
    levene_p: float
    f_stat: float
    p_valor: float
    significativa: bool
    tukey_df: pd.DataFrame | None = None
    nota: str = ""


results: list[AnovaResult] = []


# ─────────────────────────────────────────────────────────────────────
#  Utilidades estadísticas
# ─────────────────────────────────────────────────────────────────────

def _sig_label(p: float) -> str:
    if p < 0.001:
        return "***"
    if p < 0.01:
        return "**"
    if p < 0.05:
        return "*"
    return "ns"


def _run_anova(grupos_data: dict[str, np.ndarray]) -> tuple[float, float, float]:
    arrays = list(grupos_data.values())
    lev_stat, lev_p = stats.levene(*arrays)
    f, p = stats.f_oneway(*arrays)
    return lev_p, f, p


def _tukey(series_list: list[np.ndarray], labels: list[str]) -> pd.DataFrame:
    all_vals = np.concatenate(series_list)
    all_labs = np.concatenate([np.full(len(s), l) for s, l in zip(series_list, labels)])
    result = pairwise_tukeyhsd(all_vals, all_labs, alpha=0.05)
    df = pd.DataFrame(
        data=result._results_table.data[1:],
        columns=result._results_table.data[0],
    )
    return df


# ─────────────────────────────────────────────────────────────────────
#  Gráficas
# ─────────────────────────────────────────────────────────────────────

def _boxplot(grupos_data: dict[str, np.ndarray], title: str,
             ylabel: str, filename: str, p_valor: float):
    labels = list(grupos_data.keys())
    data   = [grupos_data[l] for l in labels]
    colors = [PALETTE.get(l, DEFAULT_COLOR) for l in labels]

    fig, ax = plt.subplots(figsize=(max(7, len(labels) * 1.8), 5))
    bp = ax.boxplot(data, patch_artist=True, notch=False,
                    medianprops=dict(color="white", linewidth=2.5),
                    whiskerprops=dict(linewidth=1.2),
                    capprops=dict(linewidth=1.2),
                    flierprops=dict(marker="o", markersize=3, alpha=0.4))

    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.85)

    ax.set_xticks(range(1, len(labels) + 1))
    ax.set_xticklabels(labels, fontsize=11)
    ax.set_ylabel(ylabel, fontsize=11)
    ax.set_title(f"{title}\nANOVA p = {p_valor:.4f}  {_sig_label(p_valor)}", fontsize=12, pad=12)
    ax.grid(axis="y", linestyle="--", alpha=0.4)

    # Anotar n por grupo
    for i, (lbl, arr) in enumerate(grupos_data.items(), 1):
        ax.text(i, ax.get_ylim()[0], f"n={len(arr)}", ha="center",
                va="bottom", fontsize=9, color="#555")

    # Leyenda de significancia
    legend_text = "* p<0.05   ** p<0.01   *** p<0.001   ns = no significativo"
    fig.text(0.5, 0.01, legend_text, ha="center", fontsize=8, color="#666")

    plt.tight_layout(rect=[0, 0.04, 1, 1])
    out = REPORT_DIR / filename
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Gráfica guardada: {out}")


# ─────────────────────────────────────────────────────────────────────
#  PRUEBA 1 — Precipitación vs Fase ENSO
# ─────────────────────────────────────────────────────────────────────

def prueba1_precipitacion_enso(verbose: bool = False):
    print("\n" + "═" * 60)
    print("  PRUEBA 1 — Precipitación mensual vs Fase ENSO")
    print("═" * 60)

    clima = pd.read_parquet(DATA_PROCESSED / "clima_mensual.parquet")
    enso  = pd.read_csv(DATA_RAW / "enso_noaa_raw.csv", encoding="utf-8")

    # Alinear dtypes para el merge (clima usa Int64 nullable)
    clima = clima.copy()
    clima["anio"] = clima["anio"].astype("int64")
    clima["mes"]  = clima["mes"].astype("int64")

    # Merge por anio + mes
    df = clima.merge(enso[["anio", "mes", "fase_enso"]], on=["anio", "mes"], how="inner")
    df = df.dropna(subset=["precipitacion_mm", "fase_enso"])
    df = df[df["precipitacion_mm"] >= 0]

    orden_grupos = ["El Niño", "La Niña", "Neutro"]
    grupos_data: dict[str, np.ndarray] = {}
    for fase in orden_grupos:
        sub = df[df["fase_enso"] == fase]["precipitacion_mm"].values
        if len(sub) >= 3:
            grupos_data[fase] = sub

    if len(grupos_data) < 2:
        print("  [SKIP] Datos insuficientes para ANOVA.")
        return

    n_por_grupo = {k: len(v) for k, v in grupos_data.items()}
    print(f"  Registros por grupo: {n_por_grupo}")

    lev_p, f, p = _run_anova(grupos_data)
    sig = p < 0.05

    print(f"  Levene p = {lev_p:.4f}  ({'varianzas heterogéneas' if lev_p < 0.05 else 'varianzas OK'})")
    print(f"  ANOVA   F = {f:.3f},  p = {p:.6f}  {_sig_label(p)}")
    print(f"  Conclusión: {'Diferencias SIGNIFICATIVAS entre fases ENSO' if sig else 'Sin diferencias significativas'}")

    tukey_df = None
    if sig and verbose:
        tukey_df = _tukey(list(grupos_data.values()), list(grupos_data.keys()))
        print("\n  Post-hoc Tukey HSD:")
        print(tukey_df.to_string(index=False))

    _boxplot(grupos_data,
             title="Precipitación mensual por Fase ENSO",
             ylabel="Precipitación (mm)",
             filename="anova_precipitacion_enso.png",
             p_valor=p)

    results.append(AnovaResult(
        nombre="Precipitación vs Fase ENSO",
        variable="precipitacion_mm",
        factor="fase_enso",
        grupos=list(grupos_data.keys()),
        n_por_grupo=n_por_grupo,
        levene_p=lev_p,
        f_stat=f,
        p_valor=p,
        significativa=sig,
        tukey_df=tukey_df,
        nota="Join clima_mensual + enso_noaa por anio/mes",
    ))


# ─────────────────────────────────────────────────────────────────────
#  PRUEBA 2 — Precio de insumos vs Tipo de insumo
# ─────────────────────────────────────────────────────────────────────

def prueba2_precio_tipo_insumo(verbose: bool = False):
    print("\n" + "═" * 60)
    print("  PRUEBA 2 — Precio de insumos vs Tipo de insumo")
    print("═" * 60)

    df = pd.read_parquet(DATA_PROCESSED / "insumos_clean.parquet")
    df = df.dropna(subset=["precio_cop_unidad", "tipo_insumo"])
    df = df[df["precio_cop_unidad"] > 0]

    # Excluir 'indice' (no es un insumo físico con precio real)
    df = df[df["tipo_insumo"] != "indice"]

    # Solo grupos con >= 10 observaciones
    conteos = df["tipo_insumo"].value_counts()
    grupos_validos = conteos[conteos >= 10].index.tolist()
    df = df[df["tipo_insumo"].isin(grupos_validos)]

    grupos_data: dict[str, np.ndarray] = {}
    for tipo in sorted(grupos_validos):
        sub = df[df["tipo_insumo"] == tipo]["precio_cop_unidad"].values
        grupos_data[tipo] = sub

    if len(grupos_data) < 2:
        print("  [SKIP] Datos insuficientes para ANOVA.")
        return

    n_por_grupo = {k: len(v) for k, v in grupos_data.items()}
    print(f"  Registros por grupo: {n_por_grupo}")

    lev_p, f, p = _run_anova(grupos_data)
    sig = p < 0.05

    print(f"  Levene p = {lev_p:.4f}  ({'varianzas heterogéneas' if lev_p < 0.05 else 'varianzas OK'})")
    print(f"  ANOVA   F = {f:.3f},  p = {p:.6f}  {_sig_label(p)}")
    print(f"  Conclusión: {'Diferencias SIGNIFICATIVAS entre tipos de insumo' if sig else 'Sin diferencias significativas'}")

    tukey_df = None
    if sig and verbose:
        tukey_df = _tukey(list(grupos_data.values()), list(grupos_data.keys()))
        print("\n  Post-hoc Tukey HSD:")
        print(tukey_df.to_string(index=False))

    _boxplot(grupos_data,
             title="Precio de insumos por Tipo",
             ylabel="Precio (COP / unidad)",
             filename="anova_precio_tipo_insumo.png",
             p_valor=p)

    results.append(AnovaResult(
        nombre="Precio insumos vs Tipo",
        variable="precio_cop_unidad",
        factor="tipo_insumo",
        grupos=list(grupos_data.keys()),
        n_por_grupo=n_por_grupo,
        levene_p=lev_p,
        f_stat=f,
        p_valor=p,
        significativa=sig,
        tukey_df=tukey_df,
        nota="Excluye tipo='indice'. Solo grupos con n>=10.",
    ))


# ─────────────────────────────────────────────────────────────────────
#  PRUEBA 3 — Precipitación vs Trimestre (estacionalidad Colombia)
# ─────────────────────────────────────────────────────────────────────

def prueba3_precipitacion_trimestre(verbose: bool = False):
    print("\n" + "═" * 60)
    print("  PRUEBA 3 — Precipitación mensual vs Trimestre")
    print("═" * 60)

    clima = pd.read_parquet(DATA_PROCESSED / "clima_mensual.parquet")
    df = clima.dropna(subset=["precipitacion_mm", "mes"])
    df = df[df["precipitacion_mm"] >= 0]

    def asignar_trimestre(m):
        if m in [1, 2, 3]:
            return "Q1 (Ene-Mar)"
        if m in [4, 5, 6]:
            return "Q2 (Abr-Jun)"
        if m in [7, 8, 9]:
            return "Q3 (Jul-Sep)"
        return "Q4 (Oct-Dic)"

    df["trimestre"] = df["mes"].apply(asignar_trimestre)

    orden = ["Q1 (Ene-Mar)", "Q2 (Abr-Jun)", "Q3 (Jul-Sep)", "Q4 (Oct-Dic)"]
    grupos_data: dict[str, np.ndarray] = {}
    for q in orden:
        sub = df[df["trimestre"] == q]["precipitacion_mm"].values
        if len(sub) >= 3:
            grupos_data[q] = sub

    if len(grupos_data) < 2:
        print("  [SKIP] Datos insuficientes para ANOVA.")
        return

    n_por_grupo = {k: len(v) for k, v in grupos_data.items()}
    print(f"  Registros por grupo: {n_por_grupo}")

    lev_p, f, p = _run_anova(grupos_data)
    sig = p < 0.05

    print(f"  Levene p = {lev_p:.4f}  ({'varianzas heterogéneas' if lev_p < 0.05 else 'varianzas OK'})")
    print(f"  ANOVA   F = {f:.3f},  p = {p:.6f}  {_sig_label(p)}")
    print(f"  Conclusión: {'Estacionalidad SIGNIFICATIVA en precipitación' if sig else 'Sin variación estacional significativa'}")

    tukey_df = None
    if sig and verbose:
        tukey_df = _tukey(list(grupos_data.values()), list(grupos_data.keys()))
        print("\n  Post-hoc Tukey HSD:")
        print(tukey_df.to_string(index=False))

    _boxplot(grupos_data,
             title="Precipitación mensual por Trimestre (Estacionalidad Colombia)",
             ylabel="Precipitación (mm)",
             filename="anova_precipitacion_trimestre.png",
             p_valor=p)

    results.append(AnovaResult(
        nombre="Precipitación vs Trimestre",
        variable="precipitacion_mm",
        factor="trimestre",
        grupos=list(grupos_data.keys()),
        n_por_grupo=n_por_grupo,
        levene_p=lev_p,
        f_stat=f,
        p_valor=p,
        significativa=sig,
        tukey_df=tukey_df,
        nota="Trimestres del calendario civil. Captura bimodalidad lluvias Colombia.",
    ))


# ─────────────────────────────────────────────────────────────────────
#  Resumen final
# ─────────────────────────────────────────────────────────────────────

def print_summary():
    print("\n" + "=" * 80)
    print("   RESUMEN — Pruebas ANOVA  |  AgroIA Colombia")
    print("=" * 80)

    rows = []
    for r in results:
        n_total = sum(r.n_por_grupo.values())
        rows.append({
            "Prueba":        r.nombre,
            "Variable":      r.variable,
            "Factor":        r.factor,
            "Grupos":        len(r.grupos),
            "N total":       n_total,
            "Levene p":      f"{r.levene_p:.4f}",
            "F":             f"{r.f_stat:.3f}",
            "p-valor":       f"{r.p_valor:.6f}",
            "Sig.":          _sig_label(r.p_valor),
            "Conclusión":    "SIGNIFICATIVA" if r.significativa else "No significativa",
        })

    df = pd.DataFrame(rows)

    col_widths = {c: max(len(c), df[c].astype(str).str.len().max()) for c in df.columns}
    header = "  ".join(c.ljust(col_widths[c]) for c in df.columns)
    sep    = "  ".join("-" * col_widths[c] for c in df.columns)
    print(header)
    print(sep)
    for _, row in df.iterrows():
        print("  ".join(str(row[c]).ljust(col_widths[c]) for c in df.columns))

    out_csv = REPORT_DIR / "anova_resumen.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\nTabla guardada en: {out_csv}")
    print("=" * 80)


# ─────────────────────────────────────────────────────────────────────
#  Entrypoint
# ─────────────────────────────────────────────────────────────────────

def run_all(verbose: bool = False):
    pruebas = [prueba1_precipitacion_enso, prueba2_precio_tipo_insumo, prueba3_precipitacion_trimestre]
    for fn in pruebas:
        try:
            fn(verbose=verbose)
        except Exception as e:
            print(f"  [ERROR] {fn.__name__}: {type(e).__name__}: {e}")
    print_summary()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pruebas ANOVA — AgroIA Colombia")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Mostrar tablas Tukey HSD post-hoc")
    args = parser.parse_args()
    run_all(verbose=args.verbose)
