/* Contenido editorial de la web. Sale de docs/planteamiento_problema.md,
   docs/conclusiones.md y del README; se centraliza aquí para que Inicio y
   Cómo funciona cuenten la misma historia con las mismas cifras. */

export const REPO_URL = "https://github.com/Jacklb19/AgroH";
export const DOCS_URL = `${REPO_URL}/tree/main/docs`;

export const PROBLEMA = [
  {
    icon: "cloudRain",
    tono: "amber",
    cifra: "El Niño",
    titulo: "El clima golpea sin aviso útil",
    texto:
      "El Niño y La Niña cambian cuánto llueve en el país (nuestros datos lo confirman) y con ello el riesgo de perder cosechas. La señal se conoce con meses de antelación, pero casi nunca se traduce en cifras por municipio y cultivo.",
  },
  {
    icon: "puzzle",
    tono: "blue",
    cifra: "8+ portales",
    titulo: "La información está dispersa",
    texto:
      "Clima, producción, precios y suelos viven en portales distintos (IDEAM, DANE, UPRA, NOAA, NASA…) que no se hablan entre sí. Cruzarlos a mano es inviable para un productor o un funcionario.",
  },
  {
    icon: "compass",
    tono: "green",
    cifra: "Sin acceso",
    titulo: "Las decisiones se toman a ojo",
    texto:
      "Los modelos predictivos existen en artículos académicos, pero no en herramientas que un productor, un asesor técnico o una alcaldía puedan usar para decidir qué y cuándo sembrar.",
  },
];

/* Pruebas ANOVA: `clave` se cruza con la columna "Prueba" de anova_data.json. */
export const ANOVA_TESTS = [
  {
    id: 1,
    clave: "ENSO",
    imagen: "anova_precipitacion_enso.png",
    titulo: "El Niño y La Niña cambian cuánto llueve",
    hallazgo: "Durante La Niña llueve bastante más que durante El Niño.",
    pregunta: "¿Llueve diferente según el estado del clima global?",
    explicacion:
      "Comparamos la lluvia mensual en tres fases: El Niño (Pacífico más caliente), La Niña (más frío) y años neutros. La diferencia es real, no casualidad: por eso los años de El Niño suelen traer sequía y pérdidas de cosecha.",
    fuente: "IDEAM · 47.819 registros",
    tono: "blue",
  },
  {
    id: 2,
    clave: "Trimestre",
    imagen: "anova_precipitacion_trimestre.png",
    titulo: "Colombia tiene dos temporadas de lluvia",
    hallazgo: "Abril–junio y octubre–diciembre son los trimestres más lluviosos.",
    pregunta: "¿Hay meses con más lluvia que otros?",
    explicacion:
      "El país tiene un régimen bimodal: dos épocas húmedas al año. Al comparar los cuatro trimestres, las diferencias son estadísticamente significativas. Conocer este patrón ayuda a programar siembra y cosecha.",
    fuente: "IDEAM · 47.819 registros",
    tono: "green",
  },
  {
    id: 3,
    clave: "Tipo",
    imagen: "anova_precio_tipo_insumo.png",
    titulo: "Los insumos no cuestan lo mismo",
    hallazgo: "El fertilizante es, en promedio, el insumo más costoso.",
    pregunta: "¿Cuestan igual fertilizantes, semillas y agroquímicos?",
    explicacion:
      "Comparamos cinco categorías de insumos (fertilizantes, agroquímicos, semillas, combustible y mano de obra). Los precios difieren de forma significativa, un dato clave para planificar los costos de cada siembra.",
    fuente: "DANE · SIPSA · 5.472 registros",
    tono: "amber",
  },
  {
    id: 4,
    clave: "NASA",
    imagen: "anova_precipitacion_nasa_municipios.png",
    titulo: "Cada región vive un clima distinto",
    hallazgo: "En Villavicencio llueve más que en Ibagué y Pasto, y el satélite lo confirma.",
    pregunta: "¿Llueve igual en Ibagué, Pasto y Villavicencio?",
    explicacion:
      "Con datos satelitales de la NASA (MERRA-2), independientes de las estaciones del IDEAM, comparamos la lluvia diaria de 2024 en tres ciudades. Villavicencio (Llanos) supera a Ibagué y Pasto, lo que valida que distintas fuentes capturan las diferencias regionales.",
    fuente: "NASA POWER · 1.098 registros diarios",
    tono: "violet",
  },
];

export const PARA_QUIEN = [
  {
    icon: "sprout",
    titulo: "Productores y asistentes técnicos",
    texto: "Saber qué rendimiento esperar y qué riesgo corre la temporada antes de invertir en la siembra.",
  },
  {
    icon: "bank",
    titulo: "Gremios y crédito agropecuario",
    texto: "Finagro, Fedearroz o la FNC pueden evaluar mejor el riesgo y acompañar a sus afiliados.",
  },
  {
    icon: "building",
    titulo: "Gobernaciones y alcaldías",
    texto: "Focalizar asistencia técnica y recursos en los municipios con mayor riesgo climático-productivo.",
  },
  {
    icon: "book",
    titulo: "Investigación y política pública",
    texto: "Una base de datos abierta y reproducible para estudios agronómicos y decisiones basadas en evidencia.",
  },
];

export const FUENTES_RESUMEN = ["MinAgricultura · EVA", "DANE", "IDEAM", "UPRA", "NOAA", "NASA"];

export const LIMITACIONES = [
  {
    titulo: "Pocos años de historia",
    texto: "La producción oficial cargada va de 2019 a 2024. Con seis años, el modelo aprende bien la tendencia de cada municipio, pero no efectos que se repiten cada muchos años.",
  },
  {
    titulo: "El Niño aún no se refleja en las cosechas",
    texto: "El Niño y La Niña sí cambian la lluvia (lo prueban los datos), pero con seis años el modelo no logra medir un efecto consistente sobre el rendimiento. Por eso los escenarios suelen dar valores parecidos.",
  },
  {
    titulo: "Cifras repetidas en la fuente",
    texto: "Muchos municipios reportan el mismo rendimiento varios años seguidos a la Encuesta Agropecuaria. Eso hace difícil distinguir un año estable de un dato copiado.",
  },
  {
    titulo: "Eventos extremos puntuales",
    texto: "Un granizo, una helada o una inundación en un mes concreto pueden arruinar una cosecha sin que el modelo lo anticipe.",
  },
  {
    titulo: "Sin factores de cada finca",
    texto: "El modelo no conoce el acceso a crédito, las prácticas de cada productor ni choques repentinos del mercado.",
  },
  {
    titulo: "Clima local incompleto",
    texto: "Solo unos 120 municipios tienen series de clima de estaciones del IDEAM en la base de datos; en el resto el modelo trabaja sin clima local.",
  },
];

export const TRABAJO_FUTURO = [
  "Cargar más años de producción (2007–2018) para medir mejor el efecto de El Niño y La Niña.",
  "Integrar clima satelital (NASA POWER) para todos los municipios.",
  "Pronosticar por semestre para los cultivos transitorios.",
  "Actualizar los tableros de Power BI con los rendimientos corregidos.",
  "Alertas por WhatsApp o SMS a productores antes de cada temporada.",
];
