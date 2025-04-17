import psycopg2
import pandas as pd
import json
from sqlalchemy import create_engine

import psycopg2


def conectar_postgresql(credentials):
    """
    Crea una conexión a una base de datos PostgreSQL.

    Parámetros:
    - host (str): dirección del servidor (ej. 'localhost' o IP)
    - dbname (str): nombre de la base de datos
    - user (str): nombre de usuario
    - password (str): contraseña del usuario
    - port (int): puerto de conexión (por defecto es 5432)

    Retorna:
    - conn: objeto de conexión a PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            host=credentials["host"],
            dbname=credentials["dbname"],
            user=credentials["user"],
            password=credentials["password"],
            port=credentials["port"],
        )
        print("✅ Conexión exitosa a PostgreSQL.")
        return conn
    except Exception as e:
        print("❌ Error al conectar a la base de datos:")
        print(e)
        return None


def ejecutar_query(conn, query):
    """
    Ejecuta una consulta SQL en una conexión PostgreSQL y retorna los resultados en un DataFrame.

    Parámetros:
    - conn: objeto de conexión a PostgreSQL (creado con psycopg2.connect)
    - query (str): consulta SQL a ejecutar

    Retorna:
    - df: DataFrame con los resultados del query
    """
    try:
        df = pd.read_sql_query(query, conn)
        print("✅ Consulta ejecutada exitosamente.")
        return df
    except Exception as e:
        print("❌ Error al ejecutar la consulta:")
        print(e)
        return None


def cargar_dataframe_postgresql(df, table_name, credentials):
    """
    Carga un DataFrame a PostgreSQL. Si la tabla ya existe, la sobreescribe completamente.

    Parámetros:
    - df: DataFrame de pandas con los datos a cargar.
    - table_name (str): nombre de la tabla en PostgreSQL.
    - host, dbname, user, password, port: credenciales de conexión a PostgreSQL.

    Retorna:
    - True si la operación fue exitosa, False en caso de error.
    """
    try:
        # Crear engine de SQLAlchemy

        user = credentials["user"]
        password = credentials["password"]
        host = credentials["host"]
        port = credentials["port"]
        dbname = credentials["dbname"]

        engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        )

        # Usar .to_sql con if_exists='replace' para sobrescribir la tabla
        df.to_sql(table_name, engine, index=False, if_exists="replace")
        print(f"✅ Tabla '{table_name}' creada o reemplazada exitosamente.")
        return True

    except Exception as e:
        print("❌ Error al cargar los datos en PostgreSQL:")
        print(e)
        return False


def read_json_file(file_path):
    """
    Lee un archivo .json y retorna su contenido como un diccionario de Python.

    Parámetros:
        file_path (str): Ruta del archivo .json

    Retorna:
        dict: Contenido del archivo en formato de diccionario
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data
    except Exception as e:
        print(f"Error al leer el archivo JSON: {e}")
        return None


def limpieza_general(df):
    """
    Realiza una limpieza general de un DataFrame:
    - Normaliza nombres de columnas.
    - Elimina espacios innecesarios en strings.
    - Intenta convertir columnas tipo fecha.
    - Elimina duplicados.
    - Reporta tipos de datos y nulos.

    Retorna:
    - df_limpio: DataFrame limpio.
    """
    df = df.copy()  # Evitar modificar el original

    # 1. Nombres de columnas estandarizados
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # 2. Limpiar espacios en columnas tipo texto
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # 3. Intentar convertir columnas que parecen fechas
    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_datetime(df[col], errors="raise")
                print(f"📅 Columna '{col}' convertida a fecha.")
            except:
                continue

    # 4. Eliminar duplicados
    df = df.drop_duplicates()

    # 5. Reporte de tipos y nulos
    print("\n📊 Tipos de datos:")
    print(df.dtypes)
    print("\n🔍 Valores nulos por columna:")
    print(df.isnull().sum())

    print("\n✅ Limpieza general completada.")
    return df
