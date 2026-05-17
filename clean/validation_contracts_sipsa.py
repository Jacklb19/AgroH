import pandera.pandas as pa

sipsa_schema = pa.DataFrameSchema(
    {
        "fecha_registro": pa.Column(
            "datetime64[ns]",
            nullable=False
        ),
        "producto": pa.Column(
            str,
            checks=[
                pa.Check.str_length(min_value=2, error="Nombre de producto inválido")
            ],
            nullable=False
        ),
        "central": pa.Column(
            str,
            checks=[
                pa.Check.str_length(min_value=2, error="Nombre de central inválido")
            ],
            nullable=False
        ),
        "ciudad": pa.Column(
            str,
            nullable=False
        ),
        "precio_promedio_cop_kg": pa.Column(
            float,
            checks=[
                pa.Check.ge(0, error="El precio no puede ser negativo")
            ],
            nullable=False
        )
    },
    checks=[]
)
