# Diccionario de Datos — AgroIA Colombia

Definición completa de cada variable en las tablas de la base de datos PostgreSQL.  
Motor: **PostgreSQL 15 (Supabase)** · Esquema: **Star Schema**

---

## Tablas de Dimensión

---

### `dim_region_natural`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id_region` | SERIAL (PK) | No | Identificador único de la región natural |
| `nombre_region` | VARCHAR(50) | No | Nombre de la región: Andina, Caribe, Pacífico, Orinoquía, Amazonía |

**Valores posibles:**

| id_region | nombre_region |
|-----------|--------------|
| 1 | Andina |
| 2 | Caribe |
| 3 | Pacífico |
| 4 | Orinoquía |
| 5 | Amazonía |

---

### `dim_municipio`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id_municipio` | CHAR(5) (PK) | No | Código DIVIPOLA oficial del municipio (ej. `11001` para Bogotá) |
| `nombre_municipio` | VARCHAR(100) | No | Nombre oficial del municipio |
| `id_departamento` | CHAR(2) | No | Primeros 2 dígitos del código DIVIPOLA (código del departamento) |
| `nombre_departamento` | VARCHAR(100) | No | Nombre oficial del departamento |
| `id_region` | INT (FK) | Sí | Referencia a `dim_region_natural.id_region` |
| `latitud_centroide` | DOUBLE PRECISION | Sí | Latitud del centroide del municipio (WGS84) |
| `longitud_centroide` | DOUBLE PRECISION | Sí | Longitud del centroide del municipio (WGS84) |

---

### `dim_tiempo`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id_tiempo` | SERIAL (PK) | No | Identificador secuencial único del período |
| `fecha` | DATE | No | Primer día del mes (ej. `2024-01-01`) — UNIQUE |
| `anio` | SMALLINT | No | Año (ej. `2024`) |
| `mes` | SMALLINT | No | Mes 1–12 |
| `trimestre` | SMALLINT | No | Trimestre 1–4 |
| `semestre` | CHAR(1) | No | `A` (Ene–Jun) o `B` (Jul–Dic) |
| `nombre_mes` | VARCHAR(20) | No | Nombre del mes en español (ej. `Enero`) |
| `es_anio_nino` | BOOLEAN | No | `TRUE` si ese año fue clasificado como El Niño según NOAA |

---

### `dim_cultivo`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id_cultivo` | SERIAL (PK) | No | Identificador único del cultivo |
| `nombre_cultivo` | VARCHAR(100) | No | Nombre del cultivo tal como viene de la fuente |
| `nombre_normalizado` | VARCHAR(100) | No | Nombre en mayúsculas sin tildes — UNIQUE (ej. `MAIZ`) |
| `tipo_ciclo` | VARCHAR(20) | Sí | `transitorio` o `permanente` |
| `familia_botanica` | VARCHAR(100) | Sí | Grupo de cultivo (ej. `Cereales`, `Frutas`) |

---

### `dim_estacion_ideam`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id_estacion` | VARCHAR(20) (PK) | No | Código único de la estación IDEAM |
| `nombre_estacion` | VARCHAR(150) | Sí | Nombre oficial de la estación |
| `tipo_estacion` | VARCHAR(50) | Sí | Categoría: `Climatológica`, `Pluviométrica`, `Hidrológica`, etc. |
| `latitud` | DOUBLE PRECISION | Sí | Latitud geográfica de la estación (WGS84) |
| `longitud` | DOUBLE PRECISION | Sí | Longitud geográfica de la estación (WGS84) |
| `altitud_msnm` | DOUBLE PRECISION | Sí | Altitud en metros sobre el nivel del mar |
| `id_municipio` | CHAR(5) (FK) | Sí | Municipio asignado por join espacial (radio 50 km) |
| `estado_activa` | BOOLEAN | Sí | `TRUE` si la estación está activa actualmente |

---

### `dim_central_abastos`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id_central` | SERIAL (PK) | No | Identificador único de la central de abastos |
| `nombre_central` | VARCHAR(150) | No | Nombre de la central (ej. `Corabastos`) |
| `ciudad` | VARCHAR(100) | No | Ciudad donde se ubica |
| `id_municipio` | CHAR(5) (FK) | Sí | Municipio asociado |

---

## Tablas de Hechos (Fact Tables)

---

### `fact_produccion_agricola`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador de fila |
| `id_municipio` | CHAR(5) (FK) | No | Municipio de la cosecha |
| `id_cultivo` | INT (FK) | No | Cultivo cosechado |
| `id_tiempo` | INT (FK) | No | Período (año del registro) |
| `area_sembrada_ha` | DOUBLE PRECISION | Sí | Hectáreas sembradas ese año |
| `area_cosechada_ha` | DOUBLE PRECISION | Sí | Hectáreas efectivamente cosechadas |
| `produccion_total_ton` | DOUBLE PRECISION | Sí | Producción total en toneladas |
| `rendimiento_t_ha` | DOUBLE PRECISION | Sí | **Rendimiento en t/ha — Variable objetivo del modelo ML** |
| `fuente_origen` | VARCHAR(50) | Sí | Fuente del dato: `EVA_A04`, `EVA_A05`, etc. |

**Restricción única**: `(id_municipio, id_cultivo, id_tiempo)`

---

### `fact_clima_mensual`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador de fila |
| `id_estacion` | VARCHAR(20) (FK) | No | Estación IDEAM que tomó la medición |
| `id_municipio` | CHAR(5) (FK) | No | Municipio al que se asigna la estación |
| `id_tiempo` | INT (FK) | No | Período (mes y año) |
| `precipitacion_mm` | DOUBLE PRECISION | Sí | Precipitación mensual acumulada en milímetros |
| `temperatura_media_c` | DOUBLE PRECISION | Sí | Temperatura promedio mensual en °C |
| `temperatura_max_c` | DOUBLE PRECISION | Sí | Temperatura máxima mensual en °C |
| `temperatura_min_c` | DOUBLE PRECISION | Sí | Temperatura mínima mensual en °C |
| `humedad_relativa_pct` | DOUBLE PRECISION | Sí | Humedad relativa promedio mensual (%) |
| `brillo_solar_horas_dia` | DOUBLE PRECISION | Sí | Horas promedio de brillo solar diario |

**Restricción única**: `(id_estacion, id_tiempo)`

---

### `fact_precios_mayoristas`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador de fila |
| `id_central` | INT (FK) | No | Central de abastos donde se reportó el precio |
| `id_cultivo` | INT (FK) | No | Producto agrícola |
| `id_tiempo` | INT (FK) | No | Período mensual |
| `precio_min_cop_kg` | DOUBLE PRECISION | Sí | Precio mínimo en COP/kg |
| `precio_max_cop_kg` | DOUBLE PRECISION | Sí | Precio máximo en COP/kg |
| `precio_promedio_cop_kg` | DOUBLE PRECISION | Sí | Precio promedio ponderado en COP/kg |
| `volumen_abastecimiento_ton` | DOUBLE PRECISION | Sí | Volumen total abastecido en toneladas |

---

### `fact_aptitud_suelo`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador de fila |
| `id_municipio` | CHAR(5) (FK) | No | Municipio evaluado |
| `id_cultivo` | INT (FK) | Sí | Cultivo evaluado |
| `clase_aptitud` | VARCHAR(20) | Sí | `alta`, `moderada`, `marginal` o `no_apta` |

---

### `fact_alerta_enso`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador de fila |
| `id_tiempo` | INT (FK) | No | Período mensual |
| `id_region` | INT (FK) | No | Región natural afectada |
| `fase_enso` | VARCHAR(20) | Sí | `El Niño`, `La Niña` o `Neutro` |
| `indice_spi` | DOUBLE PRECISION | Sí | Índice de Precipitación Estandarizado (negativo = déficit, positivo = exceso) |
| `anomalia_precipitacion_pct` | DOUBLE PRECISION | Sí | Anomalía de precipitación respecto a la normal (%) |
| `probabilidad_deficit_hidrico` | DOUBLE PRECISION | Sí | Probabilidad de déficit hídrico (0–1) |
| `probabilidad_exceso_hidrico` | DOUBLE PRECISION | Sí | Probabilidad de exceso hídrico (0–1) |
| `fuente_origen` | VARCHAR(100) | Sí | Fuente: `NOAA`, `IDEAM`, `sintetico` |
| `es_sintetico` | BOOLEAN | No | `TRUE` si el dato fue generado sintéticamente para completar brechas |

---

### `fact_precios_insumos`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador de fila |
| `id_tiempo` | INT (FK) | No | Período mensual |
| `tipo_insumo` | VARCHAR(50) | Sí | Categoría: `fertilizante`, `agroquimico`, `semilla`, `combustible`, `mano_de_obra` |
| `nombre_insumo` | VARCHAR(100) | Sí | Nombre específico del insumo |
| `precio_cop_unidad` | DOUBLE PRECISION | Sí | Precio en COP por unidad de medida |
| `unidad_medida` | VARCHAR(20) | Sí | `kg`, `litro`, `bulto`, `jornal`, etc. |
| `id_region` | INT (FK) | Sí | Región donde aplica el precio |
| `fuente_origen` | VARCHAR(100) | Sí | Fuente del dato |
| `es_sintetico` | BOOLEAN | No | `TRUE` si el dato fue generado sintéticamente |

---

## Tablas de Predicciones del Modelo IA

---

### `model_version`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id_version` | SERIAL (PK) | No | Identificador de versión del modelo |
| `nombre_modelo` | VARCHAR(100) | No | Nombre: `xgboost_rendimiento`, `alerta_climatica` |
| `fecha_entrenamiento` | TIMESTAMPTZ | Sí | Fecha y hora de entrenamiento |
| `metricas_json` | JSONB | Sí | MAE, RMSE, R², parámetros Optuna, SHAP top features |
| `activo` | BOOLEAN | No | `TRUE` si es la versión activa en producción |

---

### `pred_rendimiento`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador de predicción |
| `id_municipio` | CHAR(5) (FK) | No | Municipio de la predicción |
| `id_cultivo` | INT (FK) | No | Cultivo predicho |
| `id_tiempo` | INT (FK) | No | Período predicho |
| `rendimiento_predicho_t_ha` | DOUBLE PRECISION | Sí | Rendimiento predicho por el modelo (t/ha) |
| `intervalo_confianza_inferior` | DOUBLE PRECISION | Sí | Límite inferior: predicción − MAE |
| `intervalo_confianza_superior` | DOUBLE PRECISION | Sí | Límite superior: predicción + MAE |
| `id_version` | INT (FK) | Sí | Versión del modelo que generó la predicción |
| `shap_top` | JSONB | Sí | Top-3 variables SHAP con sus valores de impacto |

---

### `pred_alerta_climatica`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador de alerta |
| `id_municipio` | CHAR(5) (FK) | No | Municipio afectado |
| `id_tiempo` | INT (FK) | No | Período de la alerta |
| `nivel_riesgo` | VARCHAR(10) | Sí | `BAJO`, `MEDIO` o `ALTO` |
| `tipo_evento` | VARCHAR(30) | Sí | Descripción del tipo de evento climático |
| `score_probabilidad` | DOUBLE PRECISION | Sí | Probabilidad del evento (0–1) |
| `descripcion_generada` | TEXT | Sí | Texto descriptivo generado automáticamente de la alerta |
| `activa` | BOOLEAN | Sí | `TRUE` si la alerta está vigente |
| `id_version` | INT (FK) | Sí | Versión del modelo |

---

## Tablas del Asistente IA (Memoria Conversacional)

---

### `chat_session`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id_session` | UUID (PK) | No | Identificador único de la sesión de chat |
| `user_label` | VARCHAR(120) | Sí | Etiqueta o nombre del usuario |
| `creada_at` | TIMESTAMPTZ | No | Fecha y hora de creación de la sesión |
| `ultima_at` | TIMESTAMPTZ | No | Fecha y hora del último mensaje |

---

### `chat_message`

| Columna | Tipo | Nulo | Descripción |
|---------|------|:----:|-------------|
| `id` | SERIAL (PK) | No | Identificador del mensaje |
| `id_session` | UUID (FK) | No | Sesión a la que pertenece el mensaje |
| `role` | VARCHAR(15) | No | `user`, `assistant` o `tool` |
| `content` | TEXT | Sí | Contenido del mensaje |
| `metadata` | JSONB | Sí | Metadatos del mensaje (context, tool_calls, etc.) |
| `creada_at` | TIMESTAMPTZ | No | Fecha y hora del mensaje |

---

## Vistas SQL (Power BI)

| Vista | Descripción |
|-------|-------------|
| `v_dashboard_agro` | Producción agrícola por municipio × cultivo × año, cruzada con clima anual |
| `v_monitor_climatico` | Datos climáticos mensuales por estación y municipio, con fase ENSO |
| `v_predicciones_modelo` | Predicciones del modelo vs. datos reales con error absoluto |
| `v_alertas_climaticas` | Alertas climáticas activas con nivel de riesgo y descripción |

---

*Versión del documento: 1.0 — Julio 2026*
