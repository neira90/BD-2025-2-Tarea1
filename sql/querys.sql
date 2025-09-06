
-- 1
SELECT i.nombre, COUNT(di.idSolicitud) AS total_solicitudes
FROM INGENIERO i
JOIN DETALLE_INGENIERO di ON i.RUT = di.rutIngeniero
GROUP BY i.nombre
HAVING COUNT(di.idSolicitud) > 5;

-- 2
SELECT 
    s.titulo,
    s.fechaPublicacion,
    u.nombUsuario AS autor
FROM SOLICITUD s
JOIN DETALLE_SOLICITUD ds ON s.idSolicitud = ds.idSolicitud
JOIN USUARIO u ON ds.rutUsuario = u.RUT
WHERE s.tipo = 'gestion_error'
ORDER BY s.fechaPublicacion ASC
LIMIT 10;

--3
SELECT 
    s.titulo,
    t.nomTopico AS topico,
    u.nombUsuario AS usuario
FROM SOLICITUD s
JOIN DETALLE_SOLICITUD ds ON s.idSolicitud = ds.idSolicitud
JOIN USUARIO u ON ds.rutUsuario = u.RUT
JOIN TOPICO t ON s.idTopicoSolicitud = t.idTopico
WHERE s.tipo = 'funcionalidad'
AND s.ambiente = 'Movil';

--4
SELECT 
    t.nomTopico,
    COUNT(s.idSolicitud) AS total_errores
FROM SOLICITUD s
JOIN TOPICO t ON s.idTopicoSolicitud = t.idTopico
WHERE s.tipo = 'gestion_error'
GROUP BY t.nomTopico
HAVING COUNT(s.idSolicitud) > 10;

--5
SELECT DISTINCT 
    s.titulo,
    u.nombUsuario AS solicitante,
    t.nomTopico AS topico
FROM SOLICITUD s
JOIN DETALLE_SOLICITUD ds ON s.idSolicitud = ds.idSolicitud
JOIN USUARIO u ON ds.rutUsuario = u.RUT
JOIN TOPICO t ON s.idTopicoSolicitud = t.idTopico
WHERE s.tipo = 'funcionalidad'
  AND EXISTS (
      SELECT 1
      FROM SOLICITUD s2
      JOIN DETALLE_SOLICITUD ds2 ON s2.idSolicitud = ds2.idSolicitud
      WHERE s2.tipo = 'gestion_error'
        AND s2.idTopicoSolicitud = s.idTopicoSolicitud
        AND ds2.rutUsuario = ds.rutUsuario
        AND s2.fechaPublicacion < s.fechaPublicacion
  );

--6
UPDATE SOLICITUD
SET estado = 'Archivado'
WHERE tipo = 'funcionalidad'
AND fechaPublicacion < (CURRENT_DATE - INTERVAL '3 years');

--7
SELECT 
    i.nombre AS ingeniero,
    t.nomTopico AS topico
FROM INGENIERO i
JOIN DETALLE_TOPICO dt ON i.RUT = dt.rutIngeniero
JOIN TOPICO t ON dt.idTopicoDetalle = t.idTopico
WHERE t.nomTopico = 'Seguridad';

--8
SELECT 
    u.nombUsuario,
    COUNT(s.idSolicitud) AS total_solicitudes
FROM USUARIO u
JOIN DETALLE_SOLICITUD ds ON u.RUT = ds.rutUsuario
JOIN SOLICITUD s ON ds.idSolicitud = s.idSolicitud
GROUP BY u.nombUsuario
ORDER BY total_solicitudes DESC;

--9
SELECT STRING_AGG(t.nomTopico || ': ' || COUNT(i.RUT), ', ') AS resultado
FROM INGENIERO i
JOIN DETALLE_TOPICO dt ON i.RUT = dt.rutIngeniero
JOIN TOPICO t ON dt.idTopicoDetalle = t.idTopico
GROUP BY t.nomTopico;

--10
SELECT idSolicitud, titulo, fechaPublicacion, estado
FROM SOLICITUD
WHERE tipo = 'gestion_error'
AND fechaPublicacion < (CURRENT_DATE - INTERVAL '5 years');
