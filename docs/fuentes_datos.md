# Fuentes de Datos — AgroIA Colombia

Descripción detallada de cada una de las **10 fuentes de datos** integradas en el sistema.

---

## Fuentes de datos.gov.co (Open Data Colombia)

---

### 1. Producción Agrícola Municipal — EVA (DANE)

| Atributo | Valor |
|----------|-------|
| **Nombre oficial** | Evaluaciones Agropecuarias Municipales (EVA) — A04 y A05 |
| **Entidad** | DANE (Departamento Administrativo Nacional de Estadística) |
| **Portal** | [datos.gov.co](https://www.datos.gov.co/Agricultura-y-Desarrollo-Rural/Evaluaciones-Agropecuarias-Municipales-EVA/uejq-wxrr) |
| **Recurso ID** | `uejq-wxrr` |
| **URL API** | `https://www.datos.gov.co/resource/uejq-wxrr.json` |
| **Formato** | JSON (Socrata OData) |
| **Cobertura temporal** | 2007 – 2025 |
| **Cobertura geográfica** | Nacional — todos los municipios con producción reportada |
| **Número de registros** | ~200.000 filas |
| **Frecuencia de actualización** | Anual |
| **Licencia** | Datos abiertos del Estado colombiano — libre uso |

**Variables principales utilizadas:**

| Variable Original | Variable Limpia | Tipo | Descripción |
|-------------------|-----------------|------|-------------|
| `a_o` | `anio` | Integer | Año de la cosecha |
| `municipio` | `municipio` | String | Nombre del municipio |
| `c_digo_dane_municipio` | `id_municipio` | CHAR(5) | Código DIVIPOLA |
| `cultivo` | `cultivo` | String | Nombre del cultivo |
| `grupo_cultivo` | `grupo_de_cultivo` | String | Familia botánica |
| `ciclo_del_cultivo` | `ciclo_de_cultivo` | String | Transitorio / Permanente |
| `rea_sembrada` | `area_sembrada_ha` | Float | Área sembrada en hectáreas |
| `rea_cosechada` | `area_cosechada_ha` | Float | Área cosechada en hectáreas |
| `producci_n` | `produccion_total_ton` | Float | Producción total en toneladas |
| `rendimiento` | `rendimiento_t_ha` | Float | **Rendimiento (t/ha) — Variable Objetivo** |

---

### 2. Catálogo de Estaciones Hidrometeorológicas — IDEAM

| Atributo | Valor |
|----------|-------|
| **Nombre oficial** | Catálogo de estaciones del IDEAM |
| **Entidad** | IDEAM (Instituto de Hidrología, Meteorología y Estudios Ambientales) |
| **Portal** | [datos.gov.co](https://www.datos.gov.co/Medio-Ambiente-y-Desarrollo-Sostenible/Catalogo-de-estaciones-del-IDEAM/hp9r-jxuu) |
| **Recurso ID** | `hp9r-jxuu` |
| **URL API** | `https://www.datos.gov.co/resource/hp9r-jxuu.json` |
| **Formato** | JSON (Socrata OData) |
| **Cobertura geográfica** | Nacional — 991 estaciones terrestres |
| **Número de registros** | 991 estaciones |
| **Frecuencia de actualización** | Semestral |

**Variables principales utilizadas:**

| Variable | Tipo | Descripción |
|----------|------|-------------|
| `codigo` | VARCHAR(20) | ID único de la estación |
| `nombre` | String | Nombre de la estación |
| `categoria` | String | Tipo: Climatológica, Pluviométrica, Hidrológica |
| `latitud` | Float | Coordenada geográfica |
| `longitud` | Float | Coordenada geográfica |
| `altitud` | Float | Altitud en metros sobre el nivel del mar |
| `estado` | String | ACTIVA / INACTIVA |
| `municipio` | String | Municipio donde se ubica |

---

### 3. Precipitación IDEAM — Series Históricas

| Atributo | Valor |
|----------|-------|
| **Nombre oficial** | Datos de precipitación — Estaciones IDEAM |
| **Recurso ID** | `s54a-sgyg` |
| **URL API** | `https://www.datos.gov.co/resource/s54a-sgyg.json` |
| **Cobertura temporal** | 2015 – 2026 |
| **Granularidad** | Cada 10 minutos (agregada a mensual en el pipeline) |
| **Número de registros** | ~5 millones |
| **Variable clave** | `valor_mm`: precipitación en milímetros |

---

### 4. Variables Climáticas Combinadas IDEAM

| Atributo | Valor |
|----------|-------|
| **Nombre oficial** | Datos de estaciones IDEAM y terceros — Variables combinadas |
| **Recurso ID** | `57sv-p2fu` |
| **URL API** | `https://www.datos.gov.co/resource/57sv-p2fu.json` |
| **Cobertura temporal** | 2018 – 2026 |
| **Variables clave** | Temperatura máx/mín/media (°C), Humedad relativa (%), Brillo solar (horas/día) |

---

### 5. DIVIPOLA — División Político-Administrativa

| Atributo | Valor |
|----------|-------|
| **Nombre oficial** | DIVIPOLA — Códigos de municipios de Colombia |
| **Entidad** | DANE |
| **Recurso ID** | `gdxc-w37w` |
| **URL API** | `https://www.datos.gov.co/resource/gdxc-w37w.json` |
| **Número de registros** | 1.122 municipios + 32 departamentos |
| **Variable clave** | `cod_mpio` (CHAR 5): Código DIVIPOLA único por municipio |

---

## Fuentes de Datos Externas

---

### 6. Índice ENSO Mensual — NOAA

| Atributo | Valor |
|----------|-------|
| **Nombre** | ENSO Monthly Index — ONI (Oceanic Niño Index) |
| **Entidad** | NOAA — National Oceanic and Atmospheric Administration |
| **URL** | `https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt` |
| **Cobertura temporal** | 2000 – 2026 |
| **Formato** | Texto plano (ASCII) |
| **Frecuencia** | Mensual |
| **Variables clave** | `fase_enso` (El Niño / La Niña / Neutro), `oni_value` (índice numérico) |

**Lógica de clasificación:**
- **El Niño**: ONI ≥ +0.5°C por 5 trimestres consecutivos
- **La Niña**: ONI ≤ −0.5°C por 5 trimestres consecutivos
- **Neutro**: −0.5 < ONI < +0.5

---

### 7. Precios Mayoristas Agrícolas — SIPSA (DANE)

| Atributo | Valor |
|----------|-------|
| **Nombre** | Sistema de Información de Precios y Abastecimiento del Sector Agropecuario (SIPSA) |
| **Entidad** | DANE |
| **Acceso** | Microdatos DANE — Catálogo 776 |
| **URL** | `https://microdatos.dane.gov.co/index.php/catalog/776` |
| **Cobertura temporal** | 2013 – 2025 |
| **Cobertura geográfica** | 52 centrales de abasto en 32 ciudades de Colombia |
| **Variables clave** | `precio_min_cop_kg`, `precio_max_cop_kg`, `precio_promedio_cop_kg`, `volumen_abastecimiento_ton` |

---

### 8. Aptitud Agrícola del Suelo — SIPRA (UPRA)

| Atributo | Valor |
|----------|-------|
| **Nombre** | Sistema de Información para la Planificación Rural Agropecuaria (SIPRA) |
| **Entidad** | UPRA (Unidad de Planificación Rural Agropecuaria) |
| **URL GeoServer** | `https://sipra.upra.gov.co/geoserver/ows` |
| **Formato** | GeoJSON (WFS) |
| **Variables clave** | `clase_aptitud`: alta / moderada / marginal / no_apta |
| **Granularidad** | Por municipio × cultivo |

**Escala de aptitud utilizada en el modelo:**

| Clase | Score ML |
|-------|----------|
| Alta | 3 |
| Moderada | 2 |
| Marginal | 1 |
| No apta | 0 |

---

### 9. Clima Diario Satelital — NASA POWER (MERRA-2)

| Atributo | Valor |
|----------|-------|
| **Nombre** | NASA Prediction Of Worldwide Energy Resources (POWER) |
| **Entidad** | NASA Goddard Space Flight Center |
| **URL API** | `https://power.larc.nasa.gov/api/temporal/daily/point` |
| **Formato** | JSON |
| **Cobertura temporal** | 1984 – presente (MERRA-2 reanalysis) |
| **Granularidad** | Diaria por punto geográfico (lat/lon) |
| **Autenticación** | Pública, sin API Key |
| **Variables clave** | `PRECTOTCORR` (precipitación), `T2M` (temperatura a 2m), `RH2M` (humedad) |
| **Uso en el proyecto** | Validación cruzada de datos IDEAM y completado de brechas espaciales |

---

### 10. Precios de Insumos Agropecuarios — DANE SIPSA Insumos

| Atributo | Valor |
|----------|-------|
| **Nombre** | SIPSA Insumos — Índice de Precios de Insumos Agropecuarios |
| **Entidad** | DANE |
| **Cobertura temporal** | 2018 – 2025 |
| **Cobertura geográfica** | Por región natural |
| **Variables clave** | `tipo_insumo` (fertilizante/agroquimico/semilla/combustible/mano_de_obra), `precio_cop_unidad`, `unidad_medida` |

---

## Resumen Integrado de Datasets

| # | Dataset | Entidad | Tipo | Registros aprox. | Datos.gov.co |
|---|---------|---------|------|-----------------|:------------:|
| 1 | Producción EVA | DANE | CSV/JSON | 200.000 | ✅ |
| 2 | Estaciones IDEAM | IDEAM | JSON | 991 | ✅ |
| 3 | Precipitación IDEAM | IDEAM | JSON | 5.000.000+ | ✅ |
| 4 | Variables Combinadas IDEAM | IDEAM | JSON | 2.000.000+ | ✅ |
| 5 | DIVIPOLA | DANE | JSON | 1.122 | ✅ |
| 6 | Índice ENSO | NOAA | TXT | 3.000 | ❌ (externo) |
| 7 | Precios Mayoristas SIPSA | DANE | Excel | 800.000 | ❌ (microdatos) |
| 8 | Aptitud Suelo SIPRA | UPRA | GeoJSON | 50.000 | ❌ (GeoServer) |
| 9 | Clima Satelital NASA | NASA | JSON | Continuo | ❌ (externo) |
| 10 | Insumos SIPSA | DANE | Excel | 500.000 | ❌ (microdatos) |

---

*Versión del documento: 1.0 — Julio 2026*
