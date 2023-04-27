from test import get_df
#---
import dask.dataframe as dd
# Cargamos la data para el procesamiento
ruta_parquet = "SE DEBE INDICAR LA RUTA DEL ARCHIVO"
df = dd.read_parquet(ruta_parquet)

df_secop = df.compute()
# Las lineas anteriores se pueden cambiar dependiendo como se vayan a leer los datos
#---

df_secop = df_secop.loc[(df_secop["ID Objeto a Contratar"]==95000000)]

# se describen los datos de tipo str
str_cols = df_secop.select_dtypes(include='object').columns

# limpiamos los datos que no esten definidos
df_secop = df_secop.loc[
    (df_secop["Identificacion del Contratista"]!="No Definido") &
    (df_secop["ID Familia"]!="N/D") & (df_secop["Valor Contrato con Adiciones"]!=0)
    ]

# eliminamos los valores nulos del contratista
df_secop = df_secop.dropna(subset=["Identificacion del Contratista", "Nom Razon Social Contratista"])

# Dimensiones
dim_DANE = get_df()

import pandas as pd
dim_departamento = pd.DataFrame()
new_column = pd.Series(df_secop["Departamento Entidad"].unique())
dim_departamento["Departamento"] = new_column
dim_departamento = dd.from_pandas(dim_departamento, npartitions=2)
dim_departamento.index = dim_departamento.index+1
dim_departamento["departamento_id"] = dim_departamento.index

dim_departamento = dim_departamento.compute()

columns = ['ID Familia', 'Nombre Familia', 'Nombre Clase']
dim_familia = pd.DataFrame()
dim_familia = df_secop.groupby(columns).count().reset_index()[columns]
dim_familia = dd.from_pandas(dim_familia, npartitions=3)
dim_familia.index = dim_familia.index+1
dim_familia["familia_id"] = dim_familia.index

dim_familia = dim_familia.compute()

columns = ['Identificacion del Contratista', 'Tipo Identifi del Contratista',
       'Nom Razon Social Contratista']
dim_contratista = pd.DataFrame()
dim_contratista = df_secop.groupby(columns).count().reset_index()[columns]
dim_contratista = dd.from_pandas(dim_contratista, npartitions=3)
dim_contratista.index = dim_contratista.index+1
dim_contratista["contratista_id"] = dim_contratista.index

dim_contratista = dim_contratista.compute()

columns = ['ID Regimen de Contratacion', 'Nombre Regimen de Contratacion', 'Tipo De Contrato']
dim_regimen_contratacion = pd.DataFrame()
dim_regimen_contratacion = df_secop.groupby(columns).count().reset_index()[columns]
dim_regimen_contratacion = dd.from_pandas(dim_regimen_contratacion, npartitions=3)
dim_regimen_contratacion.index = dim_regimen_contratacion.index+1
dim_regimen_contratacion["regimen_contratacion_id"] = dim_regimen_contratacion.index
dim_regimen_contratacion = dim_regimen_contratacion.compute()

# Se dimension con data del DANE
aux_dane = pd.DataFrame()
aux_departamento = dim_departamento.loc[dim_departamento["Departamento"]!="Colombia"]

aux_dane["dane_departamento_id"] = aux_departamento.sort_values("Departamento", ascending=True)["departamento_id"]
aux_dane["Departamento"] = dim_DANE.dropna()["Departamento"].unique()

dim_DANE.index = dim_DANE.index+1
dim_DANE["dane_id"] = dim_DANE.index
dim_DANE = pd.merge(dim_DANE, aux_dane)

merged_df = pd.merge(df_secop, dim_departamento, left_on="Departamento Entidad", right_on="Departamento")
merged_df = pd.merge(merged_df, dim_familia)
merged_df = pd.merge(merged_df, dim_contratista)
merged_df = pd.merge(merged_df, dim_regimen_contratacion)
# 69507 merged_df = pd.merge(merged_df, dim_DANE, left_on=["Anno Cargue SECOP", "departamento_id"], right_on=["anho", "dane_departamento_id"], how="left")

columns_fact = ["departamento_id", "familia_id", "contratista_id", "regimen_contratacion_id", "Anno Firma Contrato", 
                "Detalle del Objeto a Contratar", "Estado del Proceso", "Valor Contrato con Adiciones", "Cuantia Contrato"]
fact_adicciones = merged_df[columns_fact]
fact_adicciones["Valor de Todas las Adicciones"] = fact_adicciones["Valor Contrato con Adiciones"] - fact_adicciones["Cuantia Contrato"]

# Para evitar reduncia simplificaremos las columnas en la dimension del DANE
dim_DANE = dim_DANE[["dane_id", "dane_departamento_id", "pib", "Personas total"]]


# TODO: Solo ejecutar si desea reescribir los datos
fact_adicciones.to_parquet("output/fact_adicciones.parquet")

dim_departamento.to_parquet("output/dim_departamento.parquet")
dim_familia.to_parquet("output/dim_familia.parquet")
dim_contratista.to_parquet("output/dim_contratista.parquet")
dim_regimen_contratacion.to_parquet("output/dim_regimen_contratacion.parquet")
dim_DANE.to_parquet("output/dim_DANE.parquet")
