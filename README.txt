Martin Aguayo Ogaz 202473099-1
Tomás Rivadeneira 202373030-0
Grupo_10

## OJO ##
> las consultas query se haran en la defensa de manera manual suando PGadmin
> se entrega unos archivos csv con pocos datos debido a lo pesado que lelga ser guardar todo y debido que demora generar datos
  consistentes, en el momento de la defesa se tendra los csv con los 600 solicitudees, 50 ingenieros, 50 usuarios

# prerequisitos #

se uso postgreSQL, python3, pandas,
> adaptador de sql a python3 : psycopg2 ; sqlalchemy
> proseca informacion de .env 
> tener un super usuario en postgreSQL

# como conectar a una terminal postgreSQL desde ubuntu con autentificacion por par
> sudo -i -u postgres
> psql

# como correr #
1- añadir informacion del super usuario en el .env
2- dar a correr a main.py 

# para cerrar la database #
1- abrir terminal
> psql -U postgres
2- verificar la BD 
> \l (salir con q)
3- cambiar de usuario a postgres
> \c 
4- eliminar BD
> DROP DATABASE tarea1
5- revisar
> \l

