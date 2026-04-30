"use client";
import Hero from "./Hero";
import { Icon } from "./icons";

export default function PageImpacto({ onNav }) {
  return (
    <>
      <Hero onNav={onNav} compact />

      <section className="section">
        <div className="container">
          <div className="section-head">
            <span className="eyebrow amber">Impacto</span>
            <h2>Valor público y escalabilidad nacional</h2>
            <p>Cuatro vectores de impacto medibles, traducidos del ejercicio analítico a decisiones reales en el sector agropecuario colombiano.</p>
          </div>

          <div className="impact-grid">
            <div className="impact-card">
              <div className="icon-wrap"><Icon.wheat /></div>
              <h3>Productividad agrícola</h3>
              <p>Predicciones tempranas permitieron ajustar fechas de siembra y dosis de fertilización en 32.000 hectáreas piloto del corredor andino central.</p>
              <div className="impact-stat-row">
                <div className="impact-stat">+12.4%</div>
                <div className="impact-stat-label">Rendimiento promedio<br />vs. línea base 2024</div>
              </div>
            </div>
            <div className="impact-card amber">
              <div className="icon-wrap"><Icon.drop /></div>
              <h3>Gestión del riesgo</h3>
              <p>Alertas tempranas redujeron pérdidas asociadas a sequía severa en cuatro corredores cafeteros priorizados durante el episodio El Niño 2025.</p>
              <div className="impact-stat-row">
                <div className="impact-stat">−28%</div>
                <div className="impact-stat-label">Pérdidas por estrés<br />hídrico observado</div>
              </div>
            </div>
            <div className="impact-card blue">
              <div className="icon-wrap"><Icon.building /></div>
              <h3>Toma de decisiones</h3>
              <p>Cinco gobernaciones integraron las predicciones a sus planes de seguridad alimentaria 2026–2028, con tableros embebidos en sus oficinas técnicas.</p>
              <div className="impact-stat-row">
                <div className="impact-stat">5</div>
                <div className="impact-stat-label">Departamentos<br />adoptantes activos</div>
              </div>
            </div>
            <div className="impact-card green-light">
              <div className="icon-wrap"><Icon.users /></div>
              <h3>Escalabilidad nacional</h3>
              <p>Más de 14 mil productores rurales consultaron predicciones desde el portal y los canales SMS regionales habilitados con extensionistas.</p>
              <div className="impact-stat-row">
                <div className="impact-stat">14.2k</div>
                <div className="impact-stat-label">Usuarios activos<br />mensuales</div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section className="section section-gray">
        <div className="container">
          <div className="section-head" style={{ marginBottom: 22 }}>
            <span className="eyebrow">Demo · Guía para el jurado</span>
            <h2>Secuencia narrativa recomendada</h2>
            <p>Una ruta de cinco pasos que conecta el problema público con la solución técnica y el impacto potencial.</p>
          </div>
          <div className="demo-list">
            <div className="demo-item">
              <div className="num">01</div>
              <div className="txt">Presenta el <strong>problema público</strong> del sector agropecuario colombiano en planificación y riesgo, y abre con los KPIs del Inicio.</div>
              <span className="tag">Contexto</span>
            </div>
            <div className="demo-item">
              <div className="num">02</div>
              <div className="txt">Muestra la integración de <strong>datos abiertos</strong>: fuentes, cobertura territorial y calidad del dato sobre el star schema.</div>
              <span className="tag">Datos</span>
            </div>
            <div className="demo-item">
              <div className="num">03</div>
              <div className="txt">Navega los <strong>dashboards</strong>: panorama ejecutivo, real vs. predicho, y distribución de alertas activas por tipo.</div>
              <span className="tag">Analítica</span>
            </div>
            <div className="demo-item amber">
              <div className="num">04</div>
              <div className="txt">Ejecuta una <strong>predicción puntual</strong>: simula un escenario El Niño en Espinal y muestra el intervalo de confianza con su contexto.</div>
              <span className="tag">Producto</span>
            </div>
            <div className="demo-item amber">
              <div className="num">05</div>
              <div className="txt">Cierra con <strong>impacto y escalabilidad</strong>: cómo entidades, universidades y territorios pueden adoptar AgroIA en producción.</div>
              <span className="tag">Cierre</span>
            </div>
          </div>
        </div>
      </section>
    </>
  );
}
