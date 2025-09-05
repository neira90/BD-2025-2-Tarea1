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

        # poblar sol_gestion_errore #
        error_dv = pandas.read_csv("csv/errores.csv")[['idSolicitud', 'idTopicoSolicitud', 'estado', 'titulo', 'fechaPublicacion', 'descripcion']]
        for _, row in pandas.read_csv("csv/errores.csv")[['idSolicitud', 'idTopicoSolicitud', 'estado', 'titulo', 'fechaPublicacion', 'descripcion']].drop_duplicates().iterrows():
            try:
                
                fecha = parse(row['fechaPublicacion'], dayfirst=True).date()
                #print("Valor idSolicitud crudo:", repr(row['idSolicitud']))
                #print("→ Insertando en GESTION_ERROR...")
                #print("→ Datos a insertar:", tuple(row))
                #print("→ Tipo de fecha:", type(fecha))
                ##print(cur.mogrify("""
                ##    INSERT INTO gestion_error (idSolicitud,idTopicoSolicitud, estado, titulo,fechaPublicacion , descripcion)
                ##    VALUES (%s, %s, %s, %s, %s, %s)
                #""", (
                #    int(row['idSolicitud']),
                #    int(row['idTopicoSolicitud']),
                #    row['estado'],
                #    row['titulo'],
                #    fecha,
                #    row['descripcion']
                #)).decode())
                cur.execute("""
                    INSERT INTO gestion_error (idSolicitud,idTopicoSolicitud, estado, titulo,fechaPublicacion, descripcion)
                    VALUES (%s, %s, %s, %s, %s,%s)
                """, (
                    int(row['idSolicitud']),
                    int(row['idTopicoSolicitud']),
                    row['estado'],
                    row['titulo'],
                    fecha,
                    row['descripcion']
                ))
                #print("→ Inserción exitosa")
            except Exception as e:
                print(f"Error en la ejecución gestion errores: {e}")
            
        ####
        # poblar sol_funcionalidades #

        funci = pandas.read_csv("csv/funcionalidad.csv")
        # Filtrar títulos con al menos 20 caracteres
        funci_filtrado = funci[funci['titulo'].str.len() >= 20]

        # Insertar en la tabla FUNCIONALIDAD
        for _, row in funci_filtrado.drop_duplicates().iterrows():
            try:
                cur.execute("""
                    INSERT INTO FUNCIONALIDAD (idSolicitud,idTopicoSolicitud, estado, titulo, ambiente, resumen)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    int(row['idSolicitud']),
                    int(row['idTopicoSolicitud']),
                    row['estado'],
                    row['titulo'],
                    row['ambiente'],
                    row['resumen']
                ))      
            except Exception as e:
                print(f"Error en la ejecución fucionalidad: {e}")

        conn.rollback()
    except Exception as e:
        print(f"Error en la ejecución general en seccion datos normales: {e}")

        

def poblarDetalles(conn):
    try:
        cur = conn.cursor()

        ## tarea : asegurar borrar los duplicados, pero revisarlos por separado ##
        
        # Leer usuarios desde la tabla USUARIO
        usuarios_df = pandas.read_sql("SELECT RUT FROM USUARIO", engine)

        ingenieros_df = pandas.read_csv("csv/ingenieros.csv")[['RUT','nombre','email']].drop_duplicates().iterrows()


        ## poblar detalle_solucion ##

        ## funcionalidad ##

        # Leer solicitudes válidas desde la tabla FUNCIONALIDAD
        #funcionalidades_df = pandas.read_sql('SELECT idSolicitud, titulo FROM FUNCIONALIDAD', engine)
        # Extraer IDs de solicitudes
        try:
            funcionalidades_df = pandas.read_sql('SELECT idSolicitud, titulo FROM FUNCIONALIDAD WHERE LENGTH(titulo) >= 20', engine)
            print(funcionalidades_df.head())
            print(funcionalidades_df.columns)

            solicitudes_disponibles_funci = funcionalidades_df['idsolicitud'].tolist()
            solicitud_index = 0
            total_solicitudes_funci = len(solicitudes_disponibles_funci)

        except Exception as e:
            print(f"Error en extraer info funci: {e}")

        # Insertar relaciones en DETALLE_SOLICITUD

        for _, usuario in usuarios_df.iterrows():
            try:
                rut = str(usuario['rut']).strip()
                cur = conn.cursor()

                for _ in range(25):
                    if solicitud_index >= total_solicitudes_funci:
                        print("⚠️ No hay suficientes solicitudes para todos los usuarios.")
                        break

                    id_sol = solicitudes_disponibles_funci[solicitud_index]
                    solicitud_index += 1

                    try:
                        cur.execute("""
                            INSERT INTO DETALLE_SOLICITUD (rutusuario, idSolicitud)
                            VALUES (%s, %s)
                        """, (rut, id_sol))
                    except Exception as e:
                        print(f"❌ Error insertando ({rut}, {id_sol}): {e}")
            except Exception as e:
                print(f"Error poblar detalle_solicitud: {e}")

        
        ## poblar detalle_solicitud - gestion_error ##

        errores_df = pandas.read_sql("""
            SELECT idSolicitud, titulo
            FROM GESTION_ERROR
            WHERE LENGTH(titulo) >= 20
        """, conn)
        # Extraer IDs de solicitudes
        solicitudes_disponibles_gerror = errores_df['idSolicitud'].tolist()
        solicitud_index = 0
        total_solicitudes_gerror = len(solicitudes_disponibles_gerror)

        # Insertar relaciones en DETALLE_SOLICITUD
        for _, usuario in usuarios_df.iterrows():
            rut = str(usuario['rut']).strip()
            cur = conn.cursor()

            for _ in range(25):
                if solicitud_index >= total_solicitudes_gerror:
                    print("⚠️ No hay suficientes solicitudes para todos los usuarios.")
                    break

                id_sol = solicitudes_disponibles_gerror[solicitud_index]
                solicitud_index += 1

                try:
                    cur.execute("""
                        INSERT INTO DETALLE_SOLICITUD (rutusuario, idsolicitud)
                        VALUES (%s, %s)
                    """, (rut, id_sol))
                except Exception as e:
                    print(f"❌ Error insertando ({rut}, {id_sol}): {e}")
        
        ## Poblar detalle_topicos ##
        ## se asegura que hayan solo 3 ingenieros con un topico ##

        asignaciones_topico = {}
        for _,row in ingenieros_df:
            
            especialidades = row['especialidad'].strip().split(";")
            
            for topico_id in especialidades:
                topico_id = int(topico_id.strip())
                
                # Verificar si el tópico ya tiene 3 ingenieros
                if asignaciones_topico.get(topico_id, 0) < 3:
                    try:
                        cur.execute("""
                            INSERT INTO DETALLE_TOPICO (idTopicoDetalle, rutINGENIERO)
                            VALUES (%s, %s)
                        """, (topico_id, rut))
                        asignaciones_topico[topico_id] = asignaciones_topico.get(topico_id, 0) + 1
                    except Exception as e:
                        print(f" Error insertando DETALLE_TOPICO ({topico_id}, {rut}): {e}")
                else:
                    print(f" Tópico {topico_id} ya tiene 3 ingenieros. No se asignó a {rut}.")


        ## poblar detalle_ingeniero ##
        ## asegura que hayan tres ingenieros por solicitud ##
        ## max 20 solicitudes por ing ##

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
                print(f"⛔ No hay suficientes ingenieros especializados en tópico {idTopico} para solicitud {idSolicitud}.")
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
                print(f"❌ Error insertando ({rut}, {idSolicitud}): {e}")
    except Exception as e:
        print(f"Error en la ejecución general: {e}")

    
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


    
               






