
import sys
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import text
from dataclasses import dataclass, field
from datetime import datetime

# Añadir el directorio raíz al path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from load.db import get_engine
from config.settings import DATA_PROCESSED

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/audit_total.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("total_audit")

@dataclass
class AuditResult:
    category: str  # SCHEMA, INTEGRITY, QUALITY
    table: str
    check_name: str
    status: str  # PASS, FAIL, WARNING
    details: str = ""
    count: int = 0

class TotalAuditor:
    def __init__(self, engine):
        self.engine = engine
        self.results = []
        self.tables = []

    def add_result(self, category, table, check_name, status, details="", count=0):
        res = AuditResult(category, table, check_name, status, details, count)
        self.results.append(res)
        log_msg = f"[{status}] {category} | {table} | {check_name}: {details}"
        if status == "PASS":
            logger.info(log_msg)
        elif status == "WARNING":
            logger.warning(log_msg)
        else:
            logger.error(log_msg)

    def run_all(self):
        logger.info("Iniciando Auditoría Total de Base de Datos...")
        
        # 1. SCHEMA CHECKS
        self._check_schema_existence()
        
        # Si no hay tablas, no podemos seguir con integridad/calidad
        if not self.tables:
            logger.error("No se encontraron tablas para auditar.")
            return

        # 2. INTEGRITY CHECKS (NULLs, Duplicates, Orphans)
        self._check_nulls()
        self._check_uniqueness()
        self._check_orphans()
        
        # 3. QUALITY CHECKS (Ranges, Consistency)
        self._check_quality_ranges()
        
        # 4. REPORTING
        self._generate_report()

    def _check_schema_existence(self):
        """Verifica que las tablas principales existan."""
        expected_tables = [
            "dim_region_natural", "dim_municipio", "dim_tiempo", "dim_cultivo",
            "dim_estacion_ideam", "dim_central_abastos", "fact_produccion_agricola",
            "fact_clima_mensual", "fact_precios_mayoristas", "fact_aptitud_suelo",
            "fact_censo_agropecuario", "fact_alerta_enso", "fact_precios_insumos",
            "model_version", "pred_rendimiento", "pred_alerta_climatica"
        ]
        
        with self.engine.connect() as conn:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            db_tables = [t[0] for t in conn.execute(text(query)).fetchall()]
            self.tables = db_tables
            
            for table in expected_tables:
                if table in db_tables:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    self.add_result("SCHEMA", table, "Existencia", "PASS", f"Tabla presente con {count} registros", count)
                else:
                    self.add_result("SCHEMA", table, "Existencia", "FAIL", "Tabla no encontrada en la base de datos")

    def _check_nulls(self):
        """Verifica nulos en columnas críticas."""
        checks = [
            ("dim_municipio", ["nombre_municipio", "id_departamento"]),
            ("dim_tiempo", ["fecha", "anio", "mes"]),
            ("dim_cultivo", ["nombre_cultivo", "nombre_normalizado"]),
            ("fact_produccion_agricola", ["id_municipio", "id_cultivo", "id_tiempo"]),
            ("fact_clima_mensual", ["id_estacion", "id_tiempo"]),
            ("fact_precios_mayoristas", ["id_central", "id_cultivo", "id_tiempo"]),
            ("pred_rendimiento", ["id_municipio", "id_cultivo", "id_tiempo", "rendimiento_predicho_t_ha"]),
        ]
        
        with self.engine.connect() as conn:
            for table, cols in checks:
                if table not in self.tables: continue
                for col in cols:
                    query = f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
                    null_count = conn.execute(text(query)).scalar()
                    if null_count == 0:
                        self.add_result("INTEGRITY", table, f"NULL Check: {col}", "PASS", "Sin nulos")
                    else:
                        self.add_result("INTEGRITY", table, f"NULL Check: {col}", "FAIL", f"Encontrados {null_count} nulos", null_count)

    def _check_uniqueness(self):
        """Verifica duplicados en columnas UNIQUE."""
        checks = [
            ("dim_region_natural", ["nombre_region"]),
            ("dim_municipio", ["id_municipio"]),
            ("dim_tiempo", ["fecha"]),
            ("dim_cultivo", ["nombre_normalizado"]),
            ("fact_produccion_agricola", ["id_municipio", "id_cultivo", "id_tiempo"]),
            ("fact_clima_mensual", ["id_estacion", "id_tiempo"]),
            ("fact_precios_mayoristas", ["id_central", "id_cultivo", "id_tiempo"]),
            ("fact_aptitud_suelo", ["id_municipio", "id_cultivo"]),
            ("fact_censo_agropecuario", ["id_municipio", "anio_censo"]),
        ]
        
        with self.engine.connect() as conn:
            for table, cols in checks:
                if table not in self.tables: continue
                cols_str = ", ".join(cols)
                query = f"""
                    SELECT COUNT(*) 
                    FROM (
                        SELECT {cols_str}, COUNT(*) 
                        FROM {table} 
                        GROUP BY {cols_str} 
                        HAVING COUNT(*) > 1
                    ) t
                """
                dupe_groups = conn.execute(text(query)).scalar()
                if dupe_groups == 0:
                    self.add_result("INTEGRITY", table, f"Unique Check: ({cols_str})", "PASS", "Sin duplicados")
                else:
                    self.add_result("INTEGRITY", table, f"Unique Check: ({cols_str})", "FAIL", f"Encontrados {dupe_groups} grupos de duplicados", dupe_groups)

    def _check_orphans(self):
        """Verifica registros que apuntan a dimensiones inexistentes."""
        checks = [
            ("fact_produccion_agricola", "id_municipio", "dim_municipio", "id_municipio"),
            ("fact_produccion_agricola", "id_cultivo", "dim_cultivo", "id_cultivo"),
            ("fact_produccion_agricola", "id_tiempo", "dim_tiempo", "id_tiempo"),
            ("fact_clima_mensual", "id_estacion", "dim_estacion_ideam", "id_estacion"),
            ("fact_clima_mensual", "id_tiempo", "dim_tiempo", "id_tiempo"),
            ("fact_precios_mayoristas", "id_cultivo", "dim_cultivo", "id_cultivo"),
            ("pred_rendimiento", "id_municipio", "dim_municipio", "id_municipio"),
        ]
        
        with self.engine.connect() as conn:
            for f_table, f_col, d_table, d_col in checks:
                if f_table not in self.tables or d_table not in self.tables: continue
                query = f"""
                    SELECT COUNT(*) 
                    FROM {f_table} f
                    LEFT JOIN {d_table} d ON f.{f_col} = d.{d_col}
                    WHERE d.{d_col} IS NULL AND f.{f_col} IS NOT NULL
                """
                orphans = conn.execute(text(query)).scalar()
                if orphans == 0:
                    self.add_result("INTEGRITY", f_table, f"Orphan Check: {f_col} -> {d_table}", "PASS", "Integridad referencial OK")
                else:
                    self.add_result("INTEGRITY", f_table, f"Orphan Check: {f_col} -> {d_table}", "FAIL", f"Encontrados {orphans} registros huérfanos", orphans)

    def _check_quality_ranges(self):
        """Verifica rangos lógicos de los datos."""
        checks = [
            ("fact_clima_mensual", "temperatura_media_c", -10, 50, "Rango Temperatura"),
            ("fact_clima_mensual", "precipitacion_mm", 0, 3000, "Rango Precipitación"),
            ("fact_produccion_agricola", "rendimiento_t_ha", 0, 150, "Rango Rendimiento"),
            ("fact_produccion_agricola", "area_sembrada_ha", 0, 1000000, "Rango Área"),
            ("pred_rendimiento", "rendimiento_predicho_t_ha", 0, 150, "Rango Predicción"),
        ]
        
        with self.engine.connect() as conn:
            for table, col, vmin, vmax, name in checks:
                if table not in self.tables: continue
                query = f"SELECT COUNT(*) FROM {table} WHERE {col} < {vmin} OR {col} > {vmax}"
                out_of_range = conn.execute(text(query)).scalar()
                if out_of_range == 0:
                    self.add_result("QUALITY", table, f"{name}: {col}", "PASS", f"Valores en [{vmin}, {vmax}]")
                else:
                    self.add_result("QUALITY", table, f"{name}: {col}", "WARNING", f"Encontrados {out_of_range} valores fuera de rango [{vmin}, {vmax}]", out_of_range)

            # Consistencia de áreas
            if "fact_produccion_agricola" in self.tables:
                query = "SELECT COUNT(*) FROM fact_produccion_agricola WHERE area_cosechada_ha > area_sembrada_ha"
                inconsistent = conn.execute(text(query)).scalar()
                if inconsistent == 0:
                    self.add_result("QUALITY", "fact_produccion_agricola", "Consistencia Área", "PASS", "cosechada <= sembrada")
                else:
                    self.add_result("QUALITY", "fact_produccion_agricola", "Consistencia Área", "WARNING", f"{inconsistent} registros con cosechada > sembrada", inconsistent)

    def _generate_report(self):
        """Genera el reporte final en Markdown y CSV."""
        df = pd.DataFrame([
            {
                "Categoría": r.category,
                "Tabla": r.table,
                "Validación": r.check_name,
                "Estado": r.status,
                "Detalles": r.details,
                "Conteo": r.count
            } for r in self.results
        ])
        
        # Guardar CSV
        csv_path = DATA_PROCESSED / "audit_total_results.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        
        # Generar Markdown
        md_path = DATA_PROCESSED / "audit_total_report.md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write("# Reporte de Auditoría Total de Base de Datos\n\n")
            f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Resumen
            total = len(df)
            passed = len(df[df["Estado"] == "PASS"])
            failed = len(df[df["Estado"] == "FAIL"])
            warns = len(df[df["Estado"] == "WARNING"])
            
            f.write("## Resumen Ejecutivo\n\n")
            f.write(f"- **Total de validaciones**: {total}\n")
            f.write(f"- **Exitosas [PASS]**: {passed}\n")
            f.write(f"- **Fallidas [FAIL]**: {failed}\n")
            f.write(f"- **Alertas [WARNING]**: {warns}\n\n")
            
            if failed == 0:
                f.write("> [!NOTE]\n> La base de datos no presenta fallos críticos de integridad.\n\n")
            else:
                f.write("> [!CAUTION]\n> Se detectaron fallos críticos que requieren atención inmediata.\n\n")
            
            # Tablas por estado
            f.write("## Detalle por Categoría\n\n")
            for cat in df["Categoría"].unique():
                f.write(f"### {cat}\n\n")
                sub = df[df["Categoría"] == cat]
                f.write("| Tabla | Validación | Estado | Detalles |\n")
                f.write("| :--- | :--- | :--- | :--- |\n")
                for _, row in sub.iterrows():
                    status_icon = "✅" if row["Estado"] == "PASS" else ("❌" if row["Estado"] == "FAIL" else "⚠️")
                    f.write(f"| {row['Tabla']} | {row['Validación']} | {status_icon} {row['Estado']} | {row['Detalles']} |\n")
                f.write("\n")
                
        logger.info(f"Auditoría finalizada. Reportes generados en {csv_path} y {md_path}")

if __name__ == "__main__":
    engine = get_engine(fail_silently=False)
    auditor = TotalAuditor(engine)
    auditor.run_all()
