# PRD: Sistema de Archivos Distribuidos por Bloques

## Visión del Producto

Desarrollar un sistema de archivos distribuidos por bloques minimalista que permita almacenar, recuperar y gestionar archivos distribuidos en múltiples nodos, garantizando replicación y alta disponibilidad.

## Orden de Ejecución

A continuación se presenta el orden recomendado para desarrollar el proyecto de manera incremental y eficiente:

### Fase 1: Infraestructura Básica y Comunicación
- [✓] 1.1 Definir interfaces y contratos de comunicación (REST y gRPC)
- [✓] 1.2 Implementar NameNode básico con gestión de metadatos
- [✓] 1.3 Implementar DataNode básico con almacenamiento local
- [✓] 1.4 Establecer comunicación NameNode-DataNode (canal de control)

### Fase 2: Operaciones Fundamentales
- [✓] 2.1 Implementar particionamiento de archivos en bloques
- [✓] 2.2 Desarrollar algoritmo de distribución de bloques
- [✓] 2.3 Implementar operaciones básicas en NameNode (registro de archivos y bloques)
- [✓] 2.4 Implementar operaciones básicas en DataNode (almacenar y recuperar bloques)

### Fase 3: Cliente y Operaciones de Archivos
- [✓] 3.1 Desarrollar cliente CLI básico
- [✓] 3.2 Implementar operación PUT (subir archivo)
- [✓] 3.3 Implementar operación GET (descargar archivo)
- [✓] 3.4 Implementar operaciones de directorio (ls, mkdir)

### Fase 4: Replicación y Tolerancia a Fallos
- [  ] 4.1 Implementar mecanismo de replicación Leader-Follower entre DataNodes
- [  ] 4.2 Desarrollar detección de fallos en DataNodes
- [  ] 4.3 Implementar re-replicación de bloques perdidos
- [  ] 4.4 Desarrollar NameNode Follower para redundancia

### Fase 5: Refinamiento y Funcionalidades Adicionales
- [  ] 5.1 Completar comandos CLI restantes (rm, rmdir, cd)
- [  ] 5.2 Optimizar rendimiento de transferencia de datos
- [  ] 5.3 Implementar balanceo de carga entre DataNodes
- [  ] 5.4 Añadir funcionalidades opcionales (autenticación básica)

## Componentes Esenciales

### NameNode
- [✓] **Gestión de Metadatos**
  - [✓] Estructura de directorios y archivos
  - [✓] Mapeo de archivos a bloques
  - [✓] Ubicación de bloques en DataNodes
  - [✓] Estado de replicación de bloques

- [✓] **API REST (Canal de Control)**
  - [✓] Endpoint para registro de DataNodes
  - [✓] Endpoint para operaciones de archivos (crear, listar, eliminar)
  - [✓] Endpoint para obtener ubicación de bloques
  - [✓] Endpoint para reportar estado de bloques
### DataNode
- [✓] **Almacenamiento de Bloques**
  - [✓] Sistema de archivos local para almacenar bloques
  - [✓] Identificación única de bloques
  - [✓] Gestión de espacio disponible

- [✓] **Servicio gRPC (Canal de Datos)**
  - [✓] Servicio para recibir bloques de clientes
  - [✓] Servicio para enviar bloques a clientes
  - [✓] Servicio para replicación entre DataNodes
  - [✓] Servicio para reportar estado al NameNode

- [  ] **Mecanismo Leader-Follower**
  - [  ] Protocolo de replicación de bloques
  - [  ] Gestión de roles (Leader/Follower) por bloque
  - [  ] Verificación de integridad de bloques

### Cliente CLI
- [✓] **Operaciones de Archivos**
  - [✓] `put`: Dividir archivo en bloques y coordinar subida
  - [✓] `get`: Obtener ubicación de bloques y reconstruir archivo
  - [  ] `rm`: Eliminar archivo y sus bloques asociados

- [✓] **Operaciones de Directorio**
  - [✓] `ls`: Listar contenido de directorio
  - [✓] `cd`: Cambiar directorio actual
  - [✓] `mkdir`: Crear directorio
  - [✓] `rmdir`: Eliminar directorio

- [✓] **Comunicación**
  - [✓] Interacción con NameNode vía REST API
  - [✓] Interacción con DataNodes vía gRPC

## Protocolos de Comunicación

### Canal de Control (REST API)
- [✓] **Endpoints NameNode**
  - [✓] `/files` - Gestión de archivos y metadatos
  - [✓] `/blocks` - Información sobre bloques y ubicaciones
  - [✓] `/datanodes` - Registro y estado de DataNodes
  - [✓] `/directories` - Gestión de estructura de directorios

### Canal de Datos (gRPC)
- [✓] **Servicios DataNode**
  - [✓] `TransferBlock` - Transferencia de bloques entre nodos
  - [✓] `StoreBlock` - Almacenamiento de bloques
  - [✓] `RetrieveBlock` - Recuperación de bloques
  - [✓] `ReplicateBlock` - Replicación de bloques

## Algoritmos Clave

- [✓] **Particionamiento de Archivos**
  - [✓] División de archivos en bloques de tamaño fijo
  - [✓] Asignación de identificadores únicos a bloques

- [✓] **Distribución de Bloques**
  - [✓] Selección de DataNodes basada en disponibilidad y carga
  - [✓] Asignación de roles Leader/Follower para cada bloque

- [  ] **Replicación de Bloques**
  - [  ] Protocolo de replicación Leader-Follower
  - [  ] Verificación de integridad de réplicas

- [  ] **Recuperación ante Fallos**
  - [  ] Detección de DataNodes caídos
  - [  ] Re-replicación de bloques comprometidos
  - [  ] Failover de NameNode Leader a Follower

## Entregables

- [  ] **Documentación**
  - [  ] Diseño detallado de arquitectura
  - [✓] Especificación de interfaces y protocolos
  - [  ] Manual de usuario para CLI
  - [  ] Guía de despliegue

- [  ] **Código Fuente**
  - [  ] Implementación de NameNode (Leader y Follower)
  - [  ] Implementación de DataNodes
  - [  ] Implementación de Cliente CLI
  - [  ] Pruebas unitarias e integración

- [  ] **Evidencias**
  - [  ] Capturas de pantalla de funcionamiento
  - [  ] Logs de operaciones clave
  - [  ] Métricas de rendimiento

- [  ] **Presentación**
  - [  ] Video de sustentación (máximo 30 minutos)
  - [  ] Plantilla de autoevaluación completada

### Protocolos de Comunicación

- [✓] **Canal de Control (REST API)**
  - [✓] Endpoints para gestión de metadatos
  - [✓] Comunicación Cliente-NameNode
  - [✓] Comunicación DataNode-NameNode

- [✓] **Canal de Datos (gRPC)**
  - [✓] Servicios para transferencia de bloques
  - [✓] Comunicación Cliente-DataNode
  - [✓] Comunicación DataNode-DataNode para replicación

## Requisitos Funcionales

### Gestión de Archivos

- [✓] **Particionamiento de Archivos**
  - [✓] División de archivos en bloques de tamaño configurable
  - [✓] Asignación de identificadores únicos a cada bloque

- [  ] **Almacenamiento de Archivos**
  - [✓] Algoritmo para distribución de bloques entre DataNodes
  - [  ] Proceso de escritura directa Cliente-DataNode
  - [  ] Replicación de bloques entre DataNodes (Leader-Follower)

- [  ] **Recuperación de Archivos**
  - [  ] Obtención de lista de bloques desde NameNode
  - [  ] Recuperación paralela de bloques desde DataNodes
  - [  ] Reconstrucción del archivo original

- [  ] **Operaciones de Directorio**
  - [  ] Creación y eliminación de directorios
  - [  ] Navegación por la estructura de directorios
  - [  ] Listado de contenidos de directorios

### Tolerancia a Fallos

- [  ] **Replicación de Datos**
  - [  ] Garantía de al menos dos copias de cada bloque
  - [  ] Mecanismo de replicación Leader-Follower

- [  ] **Detección de Fallos**
  - [  ] Monitoreo de estado de DataNodes
  - [  ] Detección de bloques perdidos o corruptos

- [  ] **Recuperación ante Fallos**
  - [  ] Re-replicación de bloques cuando se detecta pérdida
  - [  ] Failover de NameNode Leader a Follower

## Requisitos No Funcionales

- [  ] **Rendimiento**
  - [  ] Optimización de transferencia de datos
  - [  ] Selección eficiente de DataNodes para escritura/lectura

- [  ] **Escalabilidad**
  - [  ] Soporte para múltiples DataNodes
  - [  ] Distribución balanceada de carga

- [  ] **Seguridad (Opcional)**
  - [  ] Autenticación básica (usuario/contraseña)
  - [  ] Aislamiento de archivos por usuario

## Interfaz de Usuario

### Cliente CLI

- [  ] **Comandos Básicos**
  - [  ] `ls`: Listar contenido de directorio
  - [  ] `cd`: Cambiar directorio actual
  - [  ] `mkdir`: Crear directorio
  - [  ] `rmdir`: Eliminar directorio
  - [  ] `put`: Subir archivo al sistema
  - [  ] `get`: Descargar archivo del sistema
  - [  ] `rm`: Eliminar archivo

- [  ] **Comandos Adicionales**
  - [  ] `info`: Mostrar información de un archivo (bloques, ubicaciones)
  - [  ] `status`: Mostrar estado del sistema (DataNodes activos, espacio disponible)

## Plan de Implementación

### Fase 1: Diseño y Configuración

- [  ] Diseño detallado de la arquitectura
- [✓] Definición de interfaces y protocolos
- [  ] Configuración del entorno de desarrollo

### Fase 2: Implementación de Componentes Básicos

- [  ] Implementación del NameNode Leader
- [  ] Implementación de DataNodes básicos
- [  ] Implementación del Cliente CLI básico

### Fase 3: Implementación de Funcionalidades Principales

- [  ] Particionamiento y distribución de archivos
- [  ] Replicación de bloques
- [  ] Operaciones básicas de archivos y directorios

### Fase 4: Implementación de Tolerancia a Fallos

- [  ] Implementación del NameNode Follower
- [  ] Mecanismos de detección y recuperación de fallos
- [  ] Pruebas de resiliencia

### Fase 5: Optimización y Funcionalidades Adicionales

- [  ] Optimización de rendimiento
- [  ] Implementación de funcionalidades opcionales (seguridad, etc.)
- [  ] Pruebas exhaustivas del sistema

## Entregables

- [  ] Documento con el diseño detallado y especificación de servicios
- [  ] Código fuente del sistema completo
- [  ] Documentación de instalación y uso
- [  ] Evidencias de funcionamiento (capturas de pantalla)
- [  ] Plantilla de autoevaluación completada
- [  ] Video de sustentación (máximo 30 minutos)

## Métricas de Éxito

- [  ] El sistema permite almacenar y recuperar archivos correctamente
- [  ] Los archivos se particionan en bloques y se distribuyen entre DataNodes
- [  ] Cada bloque se replica en al menos dos DataNodes
- [  ] El sistema continúa funcionando ante la caída de un DataNode
- [  ] La interfaz CLI implementa todos los comandos requeridos
- [  ] El rendimiento es aceptable para operaciones básicas
