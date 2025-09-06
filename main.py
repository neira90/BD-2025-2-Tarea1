from sqlalchemy import create_engine
import psycopg2
import pandas 
import os
import random
from dateutil.parser import parse
from psycopg2 import sql
from dotenv import load_dotenv
from collections import defaultdict
load_dotenv()



DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")




#CREAR DB SI NO EXISTE
def createDB(defaultDB:str):
    
    #Establece conexion con DB por defecto
    conn = psycopg2.connect(
        dbname=defaultDB,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
    print(f"Conectado a {defaultDB}.")
    #Verifica si la DB a crear existe
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
    exists = cur.fetchone()
    if not exists:
        cur.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"Base de datos '{DB_NAME}' creada.")
    else:
        print(f"La base de datos '{DB_NAME}' ya existe.")
        cur.close()
        conn.close()
        print("Conexion cerrada.")
        return
    cur.close()
    conn.close()
    print("Conexion cerrada.")
    #ESTABLECER CONEXION A LA DB CREADA
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
    print(f"Conectado a {DB_NAME}.")
    with open("sql/createTables.sql", "r") as sql_file:
        cur.execute(sql_file.read())
        print("Tablas creadas desde 'createTables.sql'.")
    cur.close()
    conn.close()
    print("Conexion cerrada.")

## poblar datos con archivos .csv ##

def poblarDatos(conn):
    try:
        cur = conn.cursor()

        # POBLAR USUARIOS #
        for _, row in pandas.read_csv("csv/usuarios.csv")[['RUT','nombUsuario','email']].drop_duplicates().iterrows():
            cur.execute("""
                        INSERT INTO USUARIO(RUT,nombUsuario,email)
                        VALUES (%s,%s,%s)
                        """, (
                            str(row['RUT']).strip(),
                            row['nombUsuario'],
                            row['email']
                        ))

        # poblar Ingenieros #
        for _, row in pandas.read_csv("csv/ingenieros.csv")[['RUT','nombre','email']].drop_duplicates().iterrows():
            cur.execute("""
                        INSERT INTO INGENIERO(RUT,nombre,email)
                        VALUES (%s,%s,%s)
                        """, (
                            str(row['RUT']).strip(),
                            row['nombre'],
                            row['email']
                           ))


        # poblar tabla de topicos #
        for _, row in pandas.read_csv("csv/topicos.csv")[['idTopico','nomTopico']].drop_duplicates().iterrows():
            cur.execute("""
                        INSERT INTO TOPICO(idTopico,nomTopico)
                        VALUES (%s,%s)
                        """, (
                            int(row['idTopico']),
                            row['nomTopico']
                        ))


        ## poblar solicitud ##

        ## poblar Gestion errores ##
        errores_df = pandas.read_csv("csv/errores.csv")[[
            'idSolicitud', 'idTopicoSolicitud', 'estado',
            'titulo', 'fechaPublicacion', 'descripcion'
        ]]

        for _, row in errores_df.drop_duplicates().iterrows():
            try:
                fecha = parse(row['fechaPublicacion'], dayfirst=True).date()
                cur.execute("""
                    INSERT INTO SOLICITUD (
                        idSolicitud, idTopicoSolicitud, estado, tipo,
                        titulo, fechaPublicacion, descripcion
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(row['idSolicitud']),
                    int(row['idTopicoSolicitud']),
                    row['estado'],
                    'gestion_error',
                    row['titulo'],
                    fecha,
                    row['descripcion']
                ))
            except Exception as e:
                print(f"error insertando gestion_error: {e}")

        ## poblar funcionalidad ##
        funci_df = pandas.read_csv("csv/funcionalidad.csv")

        # Filtrar por títulos válidos
        funci_filtrado = funci_df[funci_df['titulo'].str.len() >= 20]

        for _, row in funci_filtrado.drop_duplicates().iterrows():
            try:
                cur.execute("""
                    INSERT INTO SOLICITUD (
                        idSolicitud, idTopicoSolicitud, estado, tipo,
                        titulo, ambiente, resumen
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    int(row['idSolicitud']),
                    int(row['idTopicoSolicitud']),
                    row['estado'],
                    'funcionalidad',
                    row['titulo'],
                    row['ambiente'],
                    row['resumen']
                ))
            except Exception as e:
                print(f"Error insertando funcionalidad: {e}")


        conn.rollback()
    except Exception as e:
        print(f"error en la ejecución general en seccion datos normales: {e}")

        

def poblarDetalles(conn):
    try:
        cur = conn.cursor()

        
        # Leer usuarios desde la tabla USUARIO
        usuarios_df = pandas.read_sql("SELECT RUT FROM USUARIO", engine)

        ingenieros_df = pandas.read_csv("csv/ingenieros.csv")[['RUT','especialidad','nombre','email']].drop_duplicates().iterrows()


        ## Poblar detalle_topicos ##
        ## se asegura que hayan solo 3 ingenieros con un topico ##
        try:
            asignaciones_topico = {}
            for _,row in ingenieros_df:
                
                especialidades = row['especialidad'].strip().split(";")
                rut = row['RUT']

                for topico_id in especialidades:
                    topico_id = int(topico_id.strip())
                    
                    try:
                        cur.execute("""
                            INSERT INTO DETALLE_TOPICO (idTopicoDetalle, rutINGENIERO)
                            VALUES (%s, %s)
                        """, (topico_id, rut))
                        asignaciones_topico[topico_id] = asignaciones_topico.get(topico_id, 0) + 1
                    except Exception as e:
                        print(f" Error insertando DETALLE_TOPICO ({topico_id}, {rut}): {e}")
        except Exception as e:
                print(f"Error poblar Detalle_Topico {e}")


        ## poblar detalle_ingeniero ##
        ## asegura que hayan tres ingenieros por solicitud ##
        ## max 20 solicitudes por ing ##
        try:
            # 1. Obtener las especialidades de ingenieros
            cur.execute("""
                SELECT idTopicoDetalle, rutINGENIERO
                FROM DETALLE_TOPICO
            """)
            rows = cur.fetchall()

            # Mapa: topico → [ingenieros especializados]
            ingenieros_por_topico = defaultdict(list)
            asignaciones_por_ingeniero = defaultdict(int)

            for id_topico, rut in rows:
                ingenieros_por_topico[id_topico].append(rut)

            # 2. Obtener todas las solicitudes (heredadas) con idTopicoSolicitud
            cur.execute("""
                SELECT idSolicitud, idTopicoSolicitud FROM SOLICITUD
                WHERE idTopicoSolicitud IS NOT NULL
            """)
            solicitudes = cur.fetchall()

            # 3. Asignar 3 ingenieros por solicitud
            inserts = []

            for idSolicitud, idTopico in solicitudes:
                posibles_ingenieros = [
                    rut for rut in ingenieros_por_topico.get(idTopico, [])
                    if asignaciones_por_ingeniero[rut] < 20
                ]

                if len(posibles_ingenieros) < 3:
                    print(f"No hay suficientes ingenieros especializados en tópico {idTopico} para solicitud {idSolicitud}.")
                    continue

                seleccionados = random.sample(posibles_ingenieros, 3)

                for rut in seleccionados:
                    asignaciones_por_ingeniero[rut] += 1
                    inserts.append((rut, idSolicitud))

            # 4. Insertar en DETALLE_INGENIERO
            for rut, idSolicitud in inserts:
                try:
                    cur.execute("""
                        INSERT INTO DETALLE_INGENIERO (rutINGENIERO, idSolicitud)
                        VALUES (%s, %s)
                    """, (rut, idSolicitud))
                except Exception as e:
                    print(f"Error insertando ({rut}, {idSolicitud}): {e}")

        except Exception as e:
                print(f"Error zona detalle_ingeniero: {e}")
        
        ## Poblar Detalle Solicitud ##
        try:
            cur.execute("SELECT RUT FROM USUARIO")
            usuarios = [row[0] for row in cur.fetchall()]

            # 3. Crear un diccionario para contar las solicitudes asignadas por usuario y tipo
            solicitudes_asignadas = {usuario: {'gestion_error': 0, 'funcionalidad': 0} for usuario in usuarios}

            # 4. Preparar una lista para insertar en DETALLE_SOLICITUD
            insert_values = []

            # 5. Asignar solicitudes a usuarios
            cur.execute("""
                SELECT idSolicitud, tipo 
                FROM SOLICITUD
                WHERE tipo IN ('gestion_error', 'funcionalidad')
            """)
            solicitudes = cur.fetchall()
            try:
                for id_solicitud, tipo in solicitudes:
                    # Intentamos asignar una solicitud a un usuario que no haya superado el límite
                    try:
                        for usuario in usuarios:
                            if solicitudes_asignadas[usuario][tipo] < 25:
                                # Asignamos la solicitud al usuario
                                insert_values.append((usuario, id_solicitud))
                                solicitudes_asignadas[usuario][tipo] += 1
                                break  # Salimos del ciclo una vez que hemos asignado la solicitud a un usuario
                    except Exception as e:
                        print(f"Error analizando solicitudes : {e}")
            except Exception as e:
                 print(f"Error solicitudes usuarios: {e}")

            # 6. Insertar las relaciones en DETALLE_SOLICITUD

            try:
                if insert_values:
                    cur.executemany("""
                        INSERT INTO DETALLE_SOLICITUD (rutUsuario, idSolicitud)
                        VALUES (%s, %s)
                    """, insert_values)

                    conn.commit()
                    print(f"Se han insertado {len(insert_values)} relaciones en DETALLE_SOLICITUD.")

                else:
                    print("No se insertaron relaciones en DETALLE_SOLICITUD.") 
            except Exception as e:
                print(f"Error en inserción detalle_solicitud: {e}")

        except Exception as e:
                print(f"Error zona detalle_solicitudes: {e}")

        ## poblar detalle_solucion ##



    except Exception as e:
        print(f"Error en la ejecución general seccion detalles: {e}")

    
def main():
    createDB("postgres")

    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    
    conn.autocommit = True
    cur = conn.cursor()
    print(f"Conectado a {DB_NAME}.")
    
    poblarDatos(conn)
    poblarDetalles(conn)

    cur.close()
    conn.close()
    print(f"Conexion a {DB_NAME} cerrada.")
    
if __name__ == "__main__":
    main()


    
               






