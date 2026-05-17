import pandera.pandas as pa

# Esquema base para clima (aplica a precipitación y otras variables)
clima_schema = pa.DataFrameSchema(
    {
        "codigoestacion": pa.Column(
            str,
            checks=[
                pa.Check(lambda s: s.str.len() >= 4, error="codigoestacion muy corto"),
                pa.Check(lambda s: s.str.isdigit(), error="codigoestacion debe ser numérico")
            ],
            nullable=False
        ),
        "fechaobservacion": pa.Column(
            "datetime64[ns]",
            nullable=False
        ),
        "valorobservado": pa.Column(
            float,
            nullable=True # Puede ser nulo si el sensor falló (parse_error atrapará si no es número)
        ),
        "descripcionsensor": pa.Column(
            str,
            nullable=False
        )
    },
    checks=[
        # Regla de negocio genérica: Lluvia y brillo no pueden ser negativos.
        # Temperatura sí puede ser negativa pero rara vez bajo -10 en Colombia.
        pa.Check(
            lambda df: ~((df["descripcionsensor"].str.contains("Precipitaci|Brillo", case=False, na=False)) & (df["valorobservado"] < 0)),
            error="Variables acumulativas (Lluvia, Brillo) no pueden ser negativas",
            name="business_rule_no_negatives",
            ignore_na=True
        ),
        # Temperatura razonable en Colombia: -10 a 50 grados
        pa.Check(
            lambda df: ~((df["descripcionsensor"].str.contains("Temperat", case=False, na=False)) & ((df["valorobservado"] < -10) | (df["valorobservado"] > 50))),
            error="Temperatura fuera del rango climático razonable de Colombia (-10 a 50)",
            name="business_rule_temperature_range",
            ignore_na=True
        ),
        # Humedad de 0 a 100
        pa.Check(
            lambda df: ~((df["descripcionsensor"].str.contains("Humedad", case=False, na=False)) & ((df["valorobservado"] < 0) | (df["valorobservado"] > 100))),
            error="Humedad relativa fuera de rango (0 a 100%)",
            name="business_rule_humidity_range",
            ignore_na=True
        )
    ]
)
