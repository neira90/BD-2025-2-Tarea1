Tomás Rivadeneira 202373030-0


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
1-) abrir terminal
> psql -U postgres
2- verificar la BD 
> \l (salir con q)
3- cambiar de usuario a postgres
> \c 
4- eliminar BD
> DROP DATABASE tarea1
5- revisar
> \l