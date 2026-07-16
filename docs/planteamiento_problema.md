# Planteamiento del Problema — AgroIA Colombia

## Contexto

Colombia es uno de los países con mayor biodiversidad agroclimática del mundo. Su territorio de 1.141.748 km² alberga cinco regiones naturales distintas (Andina, Caribe, Pacífico, Orinoquía y Amazonía), cada una con regímenes de precipitación, temperatura y tipos de suelo radicalmente diferentes. Esta diversidad geográfica, que es una fortaleza para la variedad de cultivos posibles, también constituye el principal reto para la gestión agrícola: las mismas condiciones que permiten producir café en los Andes, arroz en el Tolima y plátano en el Chocó, hacen que los impactos de los fenómenos climáticos sean altamente heterogéneos y difíciles de predecir sin datos integrados.

La agricultura colombiana representa aproximadamente el **7,3% del PIB nacional** y genera empleo directo para más de **3,5 millones de colombianos**. Con más de **1.122 municipios** donde alguna actividad agropecuaria es económicamente significativa, el sector rural es el sostén de la seguridad alimentaria interna y una fuente creciente de exportaciones no tradicionales.

## Problema Central

A pesar de su importancia estratégica, la agricultura colombiana enfrenta tres brechas críticas e interconectadas:

### Brecha 1: Impacto del Cambio Climático sin Herramientas de Anticipación

Los fenómenos del **ENSO (El Niño — Oscilación del Sur)** son el principal driver de variabilidad climática interanual en Colombia. Durante **El Niño**, las regiones Caribe y Andina experimentan sequías severas que reducen los rendimientos de maíz, papa y arroz entre un **15% y un 40%**. Durante **La Niña**, el exceso hídrico genera inundaciones, enfermedades fungosas y pérdida de cosechas en la Orinoquía y el Pacífico.

El problema no es la ocurrencia del fenómeno —que puede anticiparse con meses de antelación gracias al monitoreo del NOAA— sino la **ausencia de herramientas que conecten esa señal climática con predicciones de rendimiento específicas por municipio y cultivo**, para que los productores y las entidades públicas puedan actuar preventivamente.

### Brecha 2: Fragmentación de la Información Agropecuaria

Los datos necesarios para tomar decisiones informadas en el sector agrícola están dispersos en al menos **8 sistemas institucionales** independientes:

- **IDEAM**: Series climáticas de 991 estaciones terrestres (sin integración con datos de producción)
- **DANE**: Estadísticas de producción agrícola (Encuesta de Área y Producción - EVA)
- **DANE - SIPSA**: Precios mayoristas de alimentos en centrales de abasto
- **DANE - SIPSA Insumos**: Precios de fertilizantes, semillas y agroquímicos
- **UPRA - SIPRA**: Aptitud física y económica del suelo por municipio y cultivo
- **NOAA**: Índices ENSO mensuales
- **NASA POWER**: Datos climáticos satelitales de respaldo (MERRA-2)
- **ANT / Finagro**: Datos de tenencia de tierra y crédito agropecuario

Ninguna de estas fuentes habla con las otras. Un productor o funcionario que quiera combinar datos de clima, precio y suelos para tomar una decisión debe consultar 8 portales distintos, descargar archivos en múltiples formatos y procesarlos manualmente, lo cual es inviable en la práctica.

### Brecha 3: Ausencia de Modelos Predictivos Accesibles

Aunque existen investigaciones académicas sobre predicción de rendimientos agrícolas en Colombia, estas están publicadas en revistas especializadas y no se traducen en **herramientas operativas y accesibles** para productores, asesores técnicos y funcionarios públicos. El estado del arte en machine learning agrícola (modelos de gradient boosting, series temporales, redes neuronales) no ha llegado al campo colombiano de manera práctica.

## Objetivos del Proyecto

### Objetivo General
Desarrollar una plataforma de inteligencia agro-climática que integre datos de múltiples fuentes oficiales colombianas e internacionales para predecir rendimientos agrícolas y detectar riesgos climáticos en los municipios de Colombia.

### Objetivos Específicos

1. **Construir un pipeline ETL robusto** que extraiga, limpie e integre automáticamente datos de IDEAM, DANE, UPRA, NOAA y NASA en una base de datos PostgreSQL con esquema estrella.

2. **Entrenar un modelo de regresión XGBoost** capaz de predecir el rendimiento de cualquier cultivo en cualquier municipio colombiano, dado el municipio, el año, la fase ENSO y la lluvia esperada.

3. **Implementar análisis estadístico ANOVA** para validar con evidencia matemática la relación entre variables climáticas (fase ENSO, estacionalidad, geografía) y la producción agrícola.

4. **Desarrollar una aplicación web interactiva** que presente dashboards, predicciones y alertas climáticas de forma accesible para productores, analistas y entidades públicas.

5. **Generar explicabilidad con SHAP** para que el modelo no sea una caja negra: cada predicción debe acompañarse de las variables que más la determinaron.

## Preguntas de Investigación

1. ¿En qué medida el fenómeno ENSO (El Niño/La Niña) afecta estadísticamente la precipitación en las distintas regiones naturales de Colombia?
2. ¿Cuál es el poder predictivo de las variables climáticas, de mercado y de suelos sobre el rendimiento agrícola por municipio?
3. ¿Qué variables tienen mayor importancia relativa en la predicción del rendimiento, según el análisis SHAP del modelo XGBoost?
4. ¿Existen diferencias estadísticamente significativas en los precios de los insumos agrícolas según su categoría (fertilizantes vs. semillas vs. agroquímicos)?

## Alcance

- **Cobertura geográfica**: 1.100+ municipios de Colombia con datos de producción e histórico climático disponible.
- **Cultivos**: 50+ cultivos analizados (transitorios y permanentes), incluyendo maíz, papa, arroz, café, plátano, caña, soya, tomate y otros.
- **Período histórico**: Producción 2007–2025; Clima 2018–2026.
- **Horizonte de predicción**: Año siguiente al último dato disponible, con intervalos de confianza.

## Relevancia y Novedad

Lo que hace único a AgroIA Colombia es la **integración horizontal** de todas estas fuentes en un único sistema analítico, combinada con:
- Un modelo de machine learning con **explicabilidad SHAP** que permite entender el "por qué" de cada predicción.
- Análisis estadístico **ANOVA riguroso** que valida científicamente las hipótesis del sistema.
- Una interfaz web moderna y accesible para usuarios no técnicos.
- Un pipeline ETL reproducible y documentado que puede ser auditado y extendido.

---

Para más información sobre la metodología, ver [marco_metodologico.md](marco_metodologico.md).
