# Hadoop Distributed File System (HDFS)

## Introducción

El Hadoop Distributed File System (HDFS) es un sistema de archivos distribuido diseñado para ejecutarse en hardware commodity. Fue desarrollado como parte del proyecto Apache Hadoop, inspirado en el Google File System (GFS). HDFS está optimizado para almacenar grandes cantidades de datos (del orden de terabytes o petabytes) y proporcionar alta disponibilidad y tolerancia a fallos.

## Historia y Origen

HDFS surge como una implementación de código abierto basada en el paper del Google File System (GFS) publicado en 2003. Mientras GFS era un sistema propietario de Google, HDFS fue desarrollado como parte del proyecto Apache Hadoop, con Yahoo! contribuyendo aproximadamente el 80% del núcleo de Hadoop (HDFS y MapReduce).

## Arquitectura Fundamental

La arquitectura de HDFS se basa en un modelo maestro-esclavo similar al de GFS:

- **NameNode**: Servidor maestro que gestiona el espacio de nombres del sistema de archivos y regula el acceso a los archivos.
- **DataNodes**: Servidores esclavos que almacenan los datos reales en forma de bloques.
- **Clientes**: Aplicaciones que acceden al sistema de archivos.

![Arquitectura HDFS](https://hadoop.apache.org/docs/r1.2.1/images/hdfsarchitecture.gif)

## Características Principales

1. **Diseñado para hardware commodity**: Funciona en hardware de bajo costo, asumiendo que los fallos son comunes.
2. **Alta tolerancia a fallos**: Los datos se replican en múltiples nodos (por defecto 3 réplicas).
3. **Optimizado para grandes archivos**: Funciona mejor con archivos grandes (gigabytes o terabytes) que se leen secuencialmente.
4. **Modelo "write-once, read-many"**: Los archivos una vez escritos no se modifican (excepto para anexar datos).
5. **Procesamiento "data-local"**: Mueve la computación a los datos en lugar de los datos a la computación.

## Funcionamiento Básico

### División en Bloques

- Los archivos se dividen en bloques de tamaño fijo (típicamente 128 MB, configurable por archivo)
- Cada bloque se identifica con un ID único
- Los bloques se replican en múltiples DataNodes para garantizar la tolerancia a fallos

### Metadatos

El NameNode mantiene tres tipos principales de metadatos:
- Espacio de nombres de archivos y directorios (jerarquía)
- Mapeo de archivos a bloques
- Ubicaciones de cada réplica de bloque

Los dos primeros tipos se mantienen persistentes mediante un registro de operaciones (journal) y puntos de control (checkpoints), mientras que la información de ubicación de bloques se obtiene dinámicamente de los DataNodes.

### Proceso de Lectura

1. El cliente contacta al NameNode para obtener información sobre los bloques que componen el archivo
2. El NameNode proporciona las ubicaciones de los bloques, ordenadas por proximidad al cliente
3. El cliente contacta directamente a los DataNodes para obtener los datos de los bloques
4. Si un bloque no está disponible o está corrupto, el cliente intenta leerlo de otra réplica

### Proceso de Escritura

1. El cliente solicita al NameNode crear un nuevo archivo
2. El NameNode verifica permisos y crea una entrada en el espacio de nombres
3. Cuando el cliente comienza a escribir, solicita al NameNode la asignación de bloques
4. El NameNode selecciona DataNodes para almacenar las réplicas según la política de ubicación
5. El cliente envía los datos directamente a los DataNodes, que forman una tubería (pipeline)
6. Los DataNodes confirman la recepción de los datos
7. El proceso se repite para cada bloque hasta que se completa el archivo

## Políticas de Ubicación de Bloques

HDFS implementa una política de ubicación de bloques que equilibra:
- Fiabilidad de los datos
- Disponibilidad de los datos
- Utilización del ancho de banda de la red

La política por defecto coloca:
- Primera réplica: en el nodo donde se encuentra el escritor (si es posible)
- Segunda y tercera réplicas: en dos nodos diferentes de un rack distinto
- Réplicas adicionales: distribuidas aleatoriamente con restricciones

Esta política minimiza el tráfico de escritura entre racks y mejora el rendimiento de lectura al distribuir los bloques.

## Tolerancia a Fallos

HDFS implementa varios mecanismos para garantizar la tolerancia a fallos:

### Replicación de Datos
- Cada bloque se replica en múltiples DataNodes (factor de replicación configurable)
- Si un nodo falla, los datos siguen disponibles en otras réplicas

### Heartbeat y Blockreport
- Los DataNodes envían heartbeats periódicos al NameNode
- Si un DataNode no envía heartbeats durante un tiempo determinado, se considera fuera de servicio
- Los Blockreports informan al NameNode sobre los bloques almacenados en cada DataNode

### Rebalanceo de Bloques
- El NameNode monitorea la distribución de bloques
- Si un bloque está sub-replicado, se programa la creación de nuevas réplicas
- Si un bloque está sobre-replicado, se eliminan réplicas excedentes

### Verificación de Integridad
- HDFS genera y almacena checksums para cada bloque
- Los checksums se verifican durante la lectura para detectar corrupción de datos

## Componentes Adicionales

### CheckpointNode
- Crea periódicamente puntos de control combinando el checkpoint existente y el journal
- Permite truncar el journal y acelerar el reinicio del NameNode

### BackupNode
- Mantiene una imagen en memoria del espacio de nombres sincronizada con el NameNode
- Puede crear checkpoints sin descargar datos del NameNode activo
- Proporciona redundancia para el NameNode

## Limitaciones y Desafíos

1. **Punto único de fallo**: El NameNode es un punto único de fallo, aunque existen soluciones como el BackupNode y HDFS Federation
2. **No adecuado para archivos pequeños**: El almacenamiento de muchos archivos pequeños es ineficiente debido a la sobrecarga de metadatos
3. **No optimizado para acceso aleatorio**: HDFS está diseñado para lecturas secuenciales, no para acceso aleatorio
4. **Latencia relativamente alta**: No es adecuado para aplicaciones que requieren baja latencia

## Evolución y Escalabilidad

Para abordar los problemas de escalabilidad, HDFS ha evolucionado con características como:

- **HDFS Federation**: Permite múltiples espacios de nombres (y NameNodes) que comparten el almacenamiento físico dentro de un clúster
- **HDFS HA (High Availability)**: Proporciona redundancia para el NameNode mediante nodos en espera activa
- **Archivos de almacenamiento en caché**: Mejora el rendimiento para conjuntos de datos de acceso frecuente

## Comparación con GFS

Aunque HDFS se inspiró en GFS, existen algunas diferencias:

- HDFS es de código abierto, mientras que GFS es propietario de Google
- HDFS utiliza Java, mientras que GFS está implementado en C++
- HDFS tiene un tamaño de bloque predeterminado de 128 MB, mientras que GFS usa 64 MB
- HDFS ha evolucionado con características como Federation y HA que no estaban en el diseño original de GFS
- GFS evolucionó a Colossus, mientras que HDFS sigue desarrollándose como proyecto de código abierto

## Conclusión

HDFS representa una implementación robusta y probada de un sistema de archivos distribuido para el procesamiento de grandes volúmenes de datos. Su diseño, inspirado en GFS, ha demostrado ser eficaz para soportar aplicaciones de big data y análisis a gran escala. A pesar de sus limitaciones, sigue siendo uno de los componentes fundamentales del ecosistema Hadoop y la base de numerosas soluciones de big data en la industria.
