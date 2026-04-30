"use client";
import { Icon } from "./icons";

export default function PageMetodologia() {
  return (
    <section className="section">
      <div className="container">
        <div className="section-head">
          <span className="eyebrow blue">Metodología</span>
          <h2>Pipeline reproducible y auditable</h2>
          <p>Cada paso del modelado está documentado, versionado y validado contra datos retenidos. Los baselines persistentes mantienen el listón técnico para defender resultados ante jurado.</p>
        </div>

        <div className="method-grid">
          <div className="method-card">
            <div className="icon-wrap"><Icon.database /></div>
            <h3>Fuentes de datos abiertos</h3>
            <ul>
              <li>DANE — evaluaciones agropecuarias municipales</li>
              <li>IDEAM — series climáticas diarias 2007–2026</li>
              <li>UPRA — uso del suelo y aptitud agrícola</li>
              <li>SIPSA — precios mayoristas semanales</li>
            </ul>
          </div>
          <div className="method-card">
            <div className="icon-wrap"><Icon.brush /></div>
            <h3>Flujo ETL</h3>
            <ul>
              <li>Imputación KNN para gaps menores a 6 meses</li>
              <li>Armonización municipio · cultivo · ciclo</li>
              <li>Detección de outliers por z-score robusto</li>
              <li>Feature store versionado en Parquet</li>
            </ul>
          </div>
          <div className="method-card">
            <div className="icon-wrap"><Icon.layers /></div>
            <h3>Star schema</h3>
            <ul>
              <li>dim_municipio · dim_cultivo · dim_tiempo</li>
              <li>fact_produccion_agricola · fact_clima_mensual</li>
              <li>fact_alerta_enso · pred_rendimiento</li>
              <li>pred_alerta_climatica · model_version</li>
            </ul>
          </div>
          <div className="method-card green">
            <div className="icon-wrap"><Icon.tree /></div>
            <h3>Modelo principal</h3>
            <ul>
              <li>XGBoost · 1.500 árboles · profundidad 8</li>
              <li>62 features agroclimáticas y de mercado</li>
              <li>Tuning bayesiano con Optuna (200 trials)</li>
              <li>Inferencia distribuida sobre 1.122 municipios</li>
            </ul>
          </div>
          <div className="method-card amber">
            <div className="icon-wrap"><Icon.ruler /></div>
            <h3>Validación y métricas</h3>
            <ul>
              <li>Time-series split por bloques anuales</li>
              <li>Hold-out 2024–2025 para test final</li>
              <li>Métricas por cultivo: R², MAE, RMSE</li>
              <li>Calibración de intervalos al 95%</li>
            </ul>
          </div>
          <div className="method-card red">
            <div className="icon-wrap"><Icon.shield /></div>
            <h3>Limitaciones y trabajo futuro</h3>
            <ul>
              <li>Cobertura desigual en zonas amazónicas</li>
              <li>Brecha de datos en cultivos transitorios</li>
              <li>Pendiente: integración satelital semanal</li>
              <li>Hoja de ruta: explicabilidad SHAP por predicción</li>
            </ul>
          </div>
        </div>

        <div className="metrics-table-wrap">
          <div className="head">
            <div>
              <h3>XGBoost vs. baselines</h3>
              <p>Hold-out 2024–2025 · agregado nacional sobre 87 cultivos.</p>
            </div>
            <span className="src-badge">model_version · v1.4.2</span>
          </div>
          <table className="metrics-table">
            <thead>
              <tr><th>Modelo</th><th>R²</th><th>MAE (t/ha)</th><th>RMSE (t/ha)</th><th>Cobertura IC 95%</th></tr>
            </thead>
            <tbody>
              <tr>
                <td><strong>XGBoost — campeón</strong> <span className="winner-cell">activo</span></td>
                <td className="num best">0.81</td>
                <td className="num best">0.18</td>
                <td className="num best">0.27</td>
                <td className="num best">94.2%</td>
              </tr>
              <tr>
                <td>Regresión lineal multivariada</td>
                <td className="num mid">0.62</td>
                <td className="num mid">0.34</td>
                <td className="num mid">0.48</td>
                <td className="num mid">88.1%</td>
              </tr>
              <tr>
                <td>Promedio móvil quinquenal (naive)</td>
                <td className="num worst">0.41</td>
                <td className="num worst">0.52</td>
                <td className="num worst">0.71</td>
                <td className="num worst">79.6%</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
