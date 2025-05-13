# Google File System (GFS)

## Introducción

El Sistema de Archivos Google (GFS) es un sistema de archivos distribuido propietario desarrollado por Google para soportar su infraestructura de procesamiento de información en la nube. Está diseñado específicamente para proporcionar eficiencia y fiabilidad en el acceso a datos utilizando sistemas masivos de clústeres de procesamiento en paralelo.

## Arquitectura Fundamental

La arquitectura de GFS se basa en un modelo maestro-esclavo:

- **Nodo Maestro (Master)**: Un único servidor que coordina todo el sistema y mantiene los metadatos.
- **Servidores de Fragmentos (Chunkservers)**: Múltiples servidores que almacenan los datos reales.
- **Clientes**: Aplicaciones que acceden al sistema de archivos.

![Arquitectura GFS](https://upload.wikimedia.org/wikipedia/commons/3/3a/GFS_architecture.svg)

## Principios de Diseño

GFS fue diseñado considerando las siguientes premisas:

1. **Los fallos de componentes son la norma, no la excepción**
   - El sistema está construido con hardware commodity de bajo costo
   - Se asume que habrá fallos constantes en los componentes
   - Incorpora monitorización, detección de errores y recuperación automática

2. **Los archivos son enormes**
   - Archivos de varios GB son comunes
   - El sistema está optimizado para manejar archivos muy grandes

3. **La mayoría de las modificaciones son por anexado (append)**
   - Las escrituras aleatorias dentro de un archivo son prácticamente inexistentes
   - Una vez escritos, los archivos generalmente solo se leen secuencialmente

4. **Co-diseño de aplicaciones y API del sistema de archivos**
   - Flexibilidad para optimizar el rendimiento global

## Funcionamiento Básico

### División en Fragmentos (Chunks)

- Los archivos se dividen en fragmentos de tamaño fijo de 64 MB
- Cada fragmento se identifica con un identificador único de 64 bits
- Los fragmentos se replican en múltiples servidores (por defecto, 3 réplicas)

### Metadatos

El nodo maestro almacena tres tipos principales de metadatos:
- Espacios de nombres de archivos y fragmentos
- Mapeo de archivos a fragmentos
- Ubicaciones de las réplicas de cada fragmento

Los dos primeros tipos se mantienen persistentes mediante un registro de operaciones (log), mientras que la información de ubicación de fragmentos se obtiene de los servidores de fragmentos al iniciar el sistema.

### Proceso de Lectura

1. El cliente traduce el nombre del archivo y el desplazamiento de bytes en un índice de fragmento
2. Envía una solicitud al maestro con el nombre del archivo y el índice del fragmento
3. El maestro responde con el identificador del fragmento y las ubicaciones de las réplicas
4. El cliente almacena en caché esta información
5. El cliente envía una solicitud directamente a uno de los servidores de fragmentos (generalmente el más cercano)
6. Las lecturas posteriores del mismo fragmento no requieren interacción con el maestro hasta que la información en caché expire

### Proceso de Escritura

1. El cliente solicita al maestro información sobre el fragmento a modificar
2. El maestro otorga un "lease" (arrendamiento) a una de las réplicas, que se convierte en la réplica primaria
3. El maestro informa al cliente sobre la réplica primaria y las secundarias
4. El cliente envía los datos a todas las réplicas
5. Una vez que todas las réplicas han recibido los datos, el cliente envía una solicitud de escritura a la réplica primaria
6. La réplica primaria asigna un número de serie a la operación y la ejecuta
7. La réplica primaria reenvía la solicitud a las réplicas secundarias
8. Las réplicas secundarias ejecutan la operación en el mismo orden
9. Las réplicas secundarias responden a la primaria cuando han completado la operación
10. La réplica primaria responde al cliente

### Operación de Append (Anexado)

GFS proporciona una operación especial llamada "record append" que permite a múltiples clientes anexar datos al mismo archivo de forma concurrente, garantizando la atomicidad de cada anexo individual. Esta operación es útil para implementar colas productor-consumidor y resultados de fusión múltiple.

## Tolerancia a Fallos

GFS implementa varios mecanismos para garantizar la tolerancia a fallos:

- **Replicación**: Cada fragmento se replica en múltiples servidores
- **Rebalanceo**: El maestro reequilibra las réplicas cuando es necesario
- **Replicación de Maestro**: El registro de operaciones del maestro se replica en múltiples máquinas
- **Puntos de Control (Checkpoints)**: El maestro crea periódicamente puntos de control de su estado
- **Detección de Corrupción**: Uso de sumas de verificación (checksums) para detectar corrupción de datos

## Rendimiento

- Con un número pequeño de servidores (~15), el rendimiento de lectura es comparable al de un solo disco (80-100 MB/s)
- El rendimiento de escritura es menor (30 MB/s)
- La velocidad para añadir datos a archivos existentes es relativamente lenta (5 MB/s)
- Al aumentar el número de servidores (342 nodos), la velocidad de lectura aumenta significativamente (583 MB/s)

## Ventajas de la Escalabilidad

Al aumentar el número de servidores en un clúster GFS, se obtienen beneficios adicionales:

- Distribución de la carga de trabajo
- Mayor capacidad de almacenamiento
- Mejor redundancia y disponibilidad de datos
- Mayor tolerancia a fallos

## Evolución

La versión actual del Google File System tiene el nombre clave "Colossus" y representa una evolución del sistema original descrito en el paper académico.

## Conclusión

El Google File System representa un enfoque innovador para el almacenamiento distribuido, diseñado específicamente para las necesidades de Google. Sus principios de diseño y arquitectura han influido significativamente en el desarrollo de otros sistemas de archivos distribuidos y tecnologías de Big Data como Hadoop HDFS.
