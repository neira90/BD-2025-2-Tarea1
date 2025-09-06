CREATE TABLE INGENIERO
(
    RUT VARCHAR(12) PRIMARY KEY,

    nombre VARCHAR(60) NOT NULL,
    email VARCHAR(35) NOT NULL
);

CREATE TABLE USUARIO
(
    RUT VARCHAR(12) PRIMARY KEY,

    nombUsuario VARCHAR(35) NOT NULL,
    email VARCHAR(35) NOT NULL
);



CREATE TABLE TOPICO
(
    idTopico INT PRIMARY KEY,
    nomTopico VARCHAR(20) NOT NULL --  Backend, Seguridad, UX/UI
);

CREATE TABLE SOLICITUD (
    idSolicitud SERIAL PRIMARY KEY,
    idTopicoSolicitud INT NOT NULL,
    estado VARCHAR(10),         -- Abierto, En Progreso, Resuelto, Cerrado
    tipo VARCHAR(20) NOT NULL,  -- "funcionalidad", "gestion_error"
    titulo VARCHAR(64),         -- funcionalidad:[20:64]
    ambiente VARCHAR(5),        -- Funcionabilidad : Web/Movil
    resumen VARCHAR(150),       -- Funcionalidad
    fechaPublicacion DATE,      -- Error
    descripcion VARCHAR(200)    -- Error
);


CREATE TABLE DETALLE_SOLICITUD
(
    rutUsuario VARCHAR(12),
    idSolicitud INT,
    FOREIGN KEY (rutUsuario) REFERENCES USUARIO(RUT),
    FOREIGN KEY (idSolicitud) REFERENCES SOLICITUD(idSolicitud)
);

CREATE TABLE DETALLE_INGENIERO
(
    rutIngeniero VARCHAR(12),
    idSolicitud INT,
    FOREIGN KEY (rutIngeniero) REFERENCES INGENIERO(RUT),
    FOREIGN KEY (idSolicitud) REFERENCES SOLICITUD(idSolicitud)
);

CREATE TABLE DETALLE_TOPICO
(
    idTopicoDetalle INT,
    rutIngeniero VARCHAR(12),
    FOREIGN KEY (idTopicoDetalle) REFERENCES TOPICO(idTopico),
    FOREIGN KEY (rutIngeniero) REFERENCES INGENIERO(RUT)
);

