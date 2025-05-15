# Pruebas del Sistema de Archivos Distribuido (DFS)

Este documento contiene todas las pruebas para verificar el correcto funcionamiento del sistema DFS implementado.

## Configuración del Servidor

El servidor NameNode debe estar ejecutándose en `http://localhost:8000`. Para iniciarlo:

```bash
python run_namenode.py
```

## Verificación de Estado

### Verificar que el servidor está funcionando

**Endpoint:** `GET /health`

**Curl:**
```bash
curl -X GET 'http://localhost:8000/health' > health_response.json
```

**Postman:**
- Método: GET
- URL: http://localhost:8000/health

## Gestión de DataNodes

### 1. Registrar un DataNode

**Endpoint:** `POST /datanodes/register`

**Curl:**
```bash
curl -X POST 'http://localhost:8000/datanodes/register' -H 'Content-Type: application/json' -d '{"hostname":"localhost","port":9000,"storage_capacity":1073741824,"available_space":1073741824}' > datanode_register_response.json
```

**Postman:**
- Método: POST
- URL: http://localhost:8000/datanodes/register
- Headers: Content-Type: application/json
- Body (raw JSON):
```json
{
  "hostname": "localhost",
  "port": 9000,
  "storage_capacity": 1073741824,
  "available_space": 1073741824
}
```

### 2. Listar DataNodes registrados

**Endpoint:** `GET /datanodes/`

**Curl:**
```bash
curl -X GET 'http://localhost:8000/datanodes/' > datanodes_list_response.json
```

**Postman:**
- Método: GET
- URL: http://localhost:8000/datanodes/

### 3. Obtener información de un DataNode específico

**Endpoint:** `GET /datanodes/{node_id}`

**Curl:**
```bash
curl -X GET 'http://localhost:8000/datanodes/[NODE_ID]' > datanode_info_response.json
```

**Postman:**
- Método: GET
- URL: http://localhost:8000/datanodes/[NODE_ID]

### 4. Enviar heartbeat desde un DataNode

**Endpoint:** `POST /datanodes/{node_id}/heartbeat`

**Curl:**
```bash
curl -X POST 'http://localhost:8000/datanodes/[NODE_ID]/heartbeat' -H 'Content-Type: application/json' -d '{"available_space":1073741824}' > heartbeat_response.json
```

**Postman:**
- Método: POST
- URL: http://localhost:8000/datanodes/[NODE_ID]/heartbeat
- Headers: Content-Type: application/json
- Body (raw JSON):
```json
{
  "available_space": 1073741824
}
```

## Gestión de Directorios

### 1. Crear un directorio

**Endpoint:** `POST /directories/`

**Curl:**
```bash
curl -X POST 'http://localhost:8000/directories/' -H 'Content-Type: application/json' -d '{"name":"test_dir","path":"/test_dir","type":"DIRECTORY","size":0,"blocks":[],"owner":"test_user"}' > directory_create_response.json
```

**Postman:**
- Método: POST
- URL: http://localhost:8000/directories/
- Headers: Content-Type: application/json
- Body (raw JSON):
```json
{
  "name": "test_dir",
  "path": "/test_dir",
  "type": "DIRECTORY",
  "size": 0,
  "blocks": [],
  "owner": "test_user"
}
```

### 2. Listar el contenido de un directorio

**Endpoint:** `GET /directories/{path}`

**Curl:**
```bash
# Para listar el directorio raíz
curl -X GET 'http://localhost:8000/directories/' > root_directory_list_response.json

# Para listar un directorio específico
curl -X GET 'http://localhost:8000/directories/%2Ftest_dir' > test_dir_list_response.json
```

**Postman:**
- Método: GET
- URL: http://localhost:8000/directories/
- URL: http://localhost:8000/directories/%2Ftest_dir (para el directorio test_dir)

### 3. Eliminar un directorio

**Endpoint:** `DELETE /directories/{path}`

**Curl:**
```bash
curl -X DELETE 'http://localhost:8000/directories/%2Ftest_dir' > directory_delete_response.json
```

**Postman:**
- Método: DELETE
- URL: http://localhost:8000/directories/%2Ftest_dir

## Gestión de Archivos

### 1. Crear un archivo

**Endpoint:** `POST /files/`

**Curl:**
```bash
curl -X POST 'http://localhost:8000/files/' -H 'Content-Type: application/json' -d '{"name":"test_file.txt","path":"/test_dir/test_file.txt","type":"FILE","size":0,"blocks":[],"owner":"test_user"}' > file_create_response.json
```

**Postman:**
- Método: POST
- URL: http://localhost:8000/files/
- Headers: Content-Type: application/json
- Body (raw JSON):
```json
{
  "name": "test_file.txt",
  "path": "/test_dir/test_file.txt",
  "type": "FILE",
  "size": 0,
  "blocks": [],
  "owner": "test_user"
}
```

### 2. Obtener información de un archivo por ID

**Endpoint:** `GET /files/{file_id}`

**Curl:**
```bash
curl -X GET 'http://localhost:8000/files/[FILE_ID]' > file_info_response.json
```

**Postman:**
- Método: GET
- URL: http://localhost:8000/files/[FILE_ID]

### 3. Obtener información de un archivo por ruta

**Endpoint:** `GET /files/path/{path}`

**Curl:**
```bash
curl -X GET 'http://localhost:8000/files/path/%2Ftest_dir%2Ftest_file.txt' > file_by_path_response.json
```

**Postman:**
- Método: GET
- URL: http://localhost:8000/files/path/%2Ftest_dir%2Ftest_file.txt

### 4. Eliminar un archivo

**Endpoint:** `DELETE /files/{file_id}`

**Curl:**
```bash
curl -X DELETE 'http://localhost:8000/files/[FILE_ID]' > file_delete_response.json
```

**Postman:**
- Método: DELETE
- URL: http://localhost:8000/files/[FILE_ID]

## Gestión de Bloques

### 1. Reportar estado de bloques

**Endpoint:** `POST /blocks/report`

**Curl:**
```bash
curl -X POST 'http://localhost:8000/blocks/report' -H 'Content-Type: application/json' -d '[{"block_id":"block1","file_id":"[FILE_ID]","size":1024,"locations":[{"block_id":"block1","datanode_id":"[DATANODE_ID]","is_leader":true}],"checksum":"abcdef"}]' > block_report_response.json
```

**Postman:**
- Método: POST
- URL: http://localhost:8000/blocks/report
- Headers: Content-Type: application/json
- Body (raw JSON):
```json
[
  {
    "block_id": "block1",
    "file_id": "[FILE_ID]",
    "size": 1024,
    "locations": [
      {
        "block_id": "block1",
        "datanode_id": "[DATANODE_ID]",
        "is_leader": true
      }
    ],
    "checksum": "abcdef"
  }
]
```

### 2. Obtener información de un bloque

**Endpoint:** `GET /blocks/{block_id}`

**Curl:**
```bash
curl -X GET 'http://localhost:8000/blocks/block1' > block_info_response.json
```

**Postman:**
- Método: GET
- URL: http://localhost:8000/blocks/block1

### 3. Obtener bloques de un archivo

**Endpoint:** `GET /blocks/file/{file_id}`

**Curl:**
```bash
curl -X GET 'http://localhost:8000/blocks/file/[FILE_ID]' > file_blocks_response.json
```

**Postman:**
- Método: GET
- URL: http://localhost:8000/blocks/file/[FILE_ID]

## Flujo de prueba completo

Para probar completamente el sistema, sigue estos pasos en orden:

1. Verificar que el servidor está funcionando
2. Registrar un DataNode
3. Crear un directorio
4. Crear un archivo en ese directorio
5. Reportar un bloque para ese archivo
6. Verificar la información del archivo y sus bloques
7. Enviar un heartbeat desde el DataNode
8. Verificar el estado actualizado del DataNode
9. Eliminar el archivo
10. Eliminar el directorio
11. Verificar que todo se ha eliminado correctamente

## Notas importantes

- Reemplaza `[NODE_ID]`, `[FILE_ID]` y `[DATANODE_ID]` con los IDs reales obtenidos en pasos anteriores.
- Todos los comandos curl redirigen la salida a archivos JSON para facilitar la revisión.
- En las URLs, `%2F` representa el carácter `/` codificado para URL.
