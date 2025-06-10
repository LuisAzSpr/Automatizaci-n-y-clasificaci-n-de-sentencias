from fastapi import FastAPI, HTTPException, Query
from google.cloud import storage
from datetime import timedelta
import psycopg2
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
from typing import Optional, List

load_dotenv()
app = FastAPI()

# Configuración de Google Cloud Storage
credentials = service_account.Credentials.from_service_account_file('credenciales.json')
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket("automatizacion-casillero")

# Función auxiliar para abrir conexión a PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="casillero-judicial",
        user=os.getenv("username-db"),
        password=os.getenv("password-db")
    )

@app.get("/descargar/{ndetalle}")
def generar_url(ndetalle: str):
    """
    Endpoint existente: recibe un ndetalle, busca su URL en la tabla sentencias_y_autos
    y devuelve un signed URL para descargar el PDF en GCS.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT url FROM sentencias_y_autos WHERE ndetalle = %s", (ndetalle,))
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result or result[0] is None:
        raise HTTPException(status_code=404, detail="Archivo no encontrado")

    blob_name = result[0]
    blob = bucket.blob(blob_name)

    signed_url = blob.generate_signed_url(
        version="v4",
        expiration=timedelta(minutes=15),
        method="GET"
    )

    return {"url": signed_url}


@app.get("/filters")
def obtener_filtros():
    """
    Devuelve todos los valores únicos para:
      - codigo_organo
      - codigo_recurso
      - especialidad_expe
      - organo_detalle
      - nombre_juez (a través de join con sentencias_jueces y jueces)
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. Valores únicos de sentencias_y_autos
    cur.execute("SELECT DISTINCT codigo_organo FROM sentencias_y_autos WHERE codigo_organo IS NOT NULL;")
    lista_organo = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT DISTINCT codigo_recurso FROM sentencias_y_autos WHERE codigo_recurso IS NOT NULL;")
    lista_recurso = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT DISTINCT especialidad_expe FROM sentencias_y_autos WHERE especialidad_expe IS NOT NULL;")
    lista_especialidad = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT DISTINCT organo_detalle FROM sentencias_y_autos WHERE organo_detalle IS NOT NULL;")
    lista_organo_detalle = [row[0] for row in cur.fetchall()]

    # 2. Valores únicos de nombre_juez (a partir de la unión de sentencias_jueces → jueces)
    cur.execute("""
        SELECT DISTINCT j.nombre_juez
        FROM jueces j
        JOIN sentencias_jueces sj ON sj.codigo = j.codigo
        JOIN sentencias_y_autos s ON s.ndetalle = sj.ndetalle
        WHERE j.nombre_juez IS NOT NULL;
    """)
    lista_nombre_juez = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return {
        "codigo_organo": sorted(lista_organo),
        "codigo_recurso": sorted(lista_recurso),
        "especialidad_expe": sorted(lista_especialidad),
        "organo_detalle": sorted(lista_organo_detalle),
        "nombre_juez": sorted(lista_nombre_juez)
    }


@app.get("/search")
def buscar_sentencias(
    codigo_organo: Optional[str] = Query(None),
    codigo_recurso: Optional[str] = Query(None),
    especialidad_expe: Optional[str] = Query(None),
    organo_detalle: Optional[str] = Query(None),
    nombre_juez: Optional[str] = Query(None),
    limit: int = 10,
    offset: int = 0
):
    """
    Endpoint para buscar sentencias/apelaciones de acuerdo a los filtros seleccionados.
    Devuelve:
      - total_count: cantidad total de PDFs que coinciden
      - items: lista de hasta 'limit' elementos con su campo 'ndetalle'
    Se podrá agregar pagination futura usando offset/limit.
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Construcción dinámica de la cláusula WHERE
    filtros_where = []
    params: List[str] = []

    if codigo_organo:
        filtros_where.append("s.codigo_organo = %s")
        params.append(codigo_organo)
    if codigo_recurso:
        filtros_where.append("s.codigo_recurso = %s")
        params.append(codigo_recurso)
    if especialidad_expe:
        filtros_where.append("s.especialidad_expe = %s")
        params.append(especialidad_expe)
    if organo_detalle:
        filtros_where.append("s.organo_detalle = %s")
        params.append(organo_detalle)
    if nombre_juez:
        filtros_where.append("j.nombre_juez = %s")
        params.append(nombre_juez)

    # Si hay filtro sobre nombre_juez, será necesario hacer join; en caso contrario, no se utiliza j ni sj.
    necesita_join_jueces = nombre_juez is not None

    # Construir la parte FROM / JOIN dinámica
    from_clause = "sentencias_y_autos s"
    if necesita_join_jueces:
        from_clause += " JOIN sentencias_jueces sj ON sj.ndetalle = s.ndetalle JOIN jueces j ON j.codigo = sj.codigo"
    else:
        # Para mantener la interfaz homogénea (en caso de que luego queramos ordenar o incluir nombre_juez aunque no se filtre)
        from_clause += " LEFT JOIN sentencias_jueces sj ON sj.ndetalle = s.ndetalle LEFT JOIN jueces j ON j.codigo = sj.codigo"

    # Concatenar cláusula WHERE
    where_sql = ""
    if filtros_where:
        where_sql = "WHERE " + " AND ".join(filtros_where)

    # 1) Obtener conteo total de ndetalle distintos
    count_query = f"""
        SELECT COUNT(DISTINCT s.ndetalle)
        FROM {from_clause}
        {where_sql};
    """
    cur.execute(count_query, tuple(params))
    total_count = cur.fetchone()[0]

    # 2) Obtener primeros 'limit' ndetalle (distintos)
    select_query = f"""
        SELECT DISTINCT s.ndetalle, s.url
        FROM {from_clause}
        {where_sql}
        ORDER BY s.ndetalle
        LIMIT %s OFFSET %s;
    """
    # Adicionar parámetros de limit y offset al final
    params_with_pagination = params + [limit, offset]
    cur.execute(select_query, tuple(params_with_pagination))
    filas = cur.fetchall()
    lista_ndetalle = {row[0]:row[1] for row in filas}

    cur.close()
    conn.close()

    return {
        "total_count": total_count,
        "items": lista_ndetalle
    }

if __name__=='__main__':
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000)