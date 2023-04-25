import pandas as pd
import numpy as np

ruta = ""
ruta_habitantes = ""

df_pib = pd.read_excel(ruta, sheet_name="Cuadro 1")
df_habitantes = pd.read_excel(ruta_habitantes, sheet_name="")


def delete_row_NaN(df) -> None:
    """
    Esta funcion elimina las filas vacias que hay al inicio del DataFrame
    """
    for i in range(len(df)):
        if type(df.iloc[i, 1]) == str:
            df.drop(df.index[0:i], inplace=True)
            break

    return df

def clean_rows(df):
    """
    Esta funcion se encarga de limpiar las filas vacias del DataFrame
    y de crear los nuevos nombres de las columnas
    """
    df = delete_row_NaN(df) # se eliminan filas vacias al inicio del df
    row_init = df.iloc[0].to_numpy() # obtenemos la primer fila
    df.columns = row_init # definimos la fila antes obtenida, como los nombres de las columnas
    df.drop(df.index[0], inplace=True) # Eliminamos la primer fila, ya que no la necesitamos (recordar que esta fila es ahora la fila de los nombres de las columnas, la cual se hizo en el paso anterior)
    df = df.reset_index(drop=True) # se resetea el indice para que inicie desde 0
    return df

def get_df():
    """
    """
    
    global df_pib, df_habitantes

    df_pib = clean_rows(df_pib)
    df_habitantes = clean_rows(df_habitantes)
    
    cantidad_departamentos = 33
    new_df = df_pib[["Código Departamento (DIVIPOLA)", "DEPARTAMENTOS", 2018.0]].iloc[1:cantidad_departamentos+1, [0,2,4]]
    new_df = new_df.rename(columns={
        2018.0: "pib",
        "Código Departamento (DIVIPOLA)": "Codigo",
        "DEPARTAMENTOS": "Departamento"
    })

    new_df["pib"] *= 1000000000
    new_df["Codigo"] = new_df["Codigo"].astype(float).astype(int)


    df_habitantes = df_habitantes[["Código DIVIPOLA", "NOMBRE DEPARTAMENTO", "Personas total"]]
    df_habitantes = df_habitantes.rename(columns={
        "Código DIVIPOLA": "Codigo",
    })
    cantidad_departamentos = 33
    df_habitantes = df_habitantes.iloc[1:cantidad_departamentos+1].iloc[:, :3]

    df_habitantes["Codigo"] = df_habitantes["Codigo"].astype(int)
    merge_df = pd.merge(new_df, df_habitantes)
    merge_df = merge_df.drop(columns=["NOMBRE DEPARTAMENTO"])
    return merge_df
