# RutaExpress · Evaluación 2

RutaExpress permite registrar envíos, consultar servicios y gestionar estados de entrega. La aplicación Angular autentica al usuario con Microsoft Entra ID. Un BFF en Spring Boot recibe las llamadas del navegador, los microservicios guardan los datos en Oracle y RabbitMQ distribuye el trabajo de cada envío entre tres consumidores.

Esta guía corresponde a la rama `fix/rabbitmq-ev2`, con el frontend y el backend probados juntos en el commit `d12d3a5`.

## Componentes del laboratorio

| Componente | Puerto | Responsabilidad |
|---|---|---|
| `frontend-ev2` | 4200 en la EC2 → 80 del contenedor | Nginx sirve Angular y reenvía las solicitudes `/api/` al BFF. El navegador entra mediante túnel SSH. |
| `bff-caso3` | 8081 | Valida el JWT para solicitudes protegidas y comunica al frontend con catálogo y envíos. |
| `ms-catalog` | 9091 | Gestiona los servicios de envío y la capacidad disponible. Escucha el evento de creación en `shipments.queue`. |
| `ms-shipments` | 9092 | Guarda y actualiza envíos en Oracle y publica los mensajes de creación. En esta EC2 usa la red Docker `host`. |
| `ms-notify` | 9093 | Consume los comandos de correo, picking y documentos. Persiste los resultados en el volumen `rutaexpress-resultados`. |
| `ms-admin` | 9094 | Expone `/api/rabbitmq` para crear y eliminar colas, exchanges y bindings. |
| `rabbit1`, `rabbit2` | AMQP 5672/5673 · administración 15672/15673 | Clúster RabbitMQ con dos nodos en la EC2 de aplicaciones. |
| `oracle23c` | 1521 | Base Oracle en una segunda EC2. Los servicios usan la PDB `FREEPDB1`. |

## Arrancar el entorno que ya está desplegado

Estos pasos reutilizan los contenedores y volúmenes existentes. Se necesita acceso SSH a las dos EC2, Docker en ambas y una cuenta de Entra con un rol válido para entrar al dashboard. Si se detuvieron las instancias desde AWS, iniciarlas y consultar el DNS público actual de la EC2 de aplicaciones.

### 1. Iniciar Oracle en la EC2 de base de datos

En la terminal de `172.31.31.4`:

```bash
docker start oracle23c
docker exec -i oracle23c sqlplus -s / as sysdba <<'SQL'
SELECT name, open_mode FROM v$pdbs WHERE name = 'FREEPDB1';
EXIT;
SQL
```

Esperar hasta ver `FREEPDB1` en `READ WRITE`. Si Oracle acaba de arrancar, repetir la consulta tras unos segundos.

### 2. Iniciar RabbitMQ y los servicios en la EC2 de aplicaciones

En la terminal de `172.31.26.172`:

```bash
docker start rabbit1 rabbit2
docker exec rabbit1 rabbitmqctl cluster_status
```

Continuar cuando ambos nodos aparezcan bajo **Running Nodes**. Si el broker está arrancando, repetir la consulta. El [Compose de RabbitMQ](rabbitmq-cluster/docker-compose-cluster.yml) define ambos nodos y sus volúmenes. El script `init-cluster.sh` contiene un **reset de rabbit2** para la unión inicial de un nodo nuevo. En el arranque habitual se reutiliza el clúster existente.

```bash
docker start ms-catalog ms-notify ms-admin ms-shipments bff-caso3 frontend-ev2
docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
docker exec rabbit1 rabbitmqctl list_queues name consumers messages_ready messages_unacknowledged
curl -sS -w '\nHTTP %{http_code}\n' http://127.0.0.1:4200/api/public
```

Spring puede tardar en registrar los consumidores. Las tres colas `q.cmd.*` principales deben tener al menos un consumidor cada una. La última petición debe devolver `API funcionando correctamente` y HTTP 200. Así se comprueba también la comunicación de Nginx con el BFF.

### 3. Abrir Angular desde Windows

En PowerShell, desde la carpeta que contiene la llave:

```powershell
cd "$HOME\Downloads"
$servidor = Read-Host "DNS publico actual de la EC2 de aplicaciones"
ssh -i ".\ruta.pem" -o ExitOnForwardFailure=yes -L 127.0.0.1:4200:127.0.0.1:4200 -N "ec2-user@$servidor"
```

Dejar abierta esa terminal y entrar a **http://localhost:4200/**. El login retorna a esa dirección. Usar **Resumen de la Cuenta → Entrar a RutaExpress → Dashboard**. Con rol Cliente, el formulario está en **Mis Pedidos → Crear Nuevo Pedido**. La vista independiente `/pedidos` contiene datos de ejemplo.

## Cómo funciona a nivel de código

1. **Login y petición.** [AuthService](frontend/src/app/services/auth.service.ts) gestiona la sesión MSAL y obtiene el access token de Entra. El [dashboard](frontend/src/app/components/dashboard/dashboard.ts) toma el correo de la cuenta y los campos del formulario. [ApiService](frontend/src/app/services/api.service.ts) añade `Authorization: Bearer …` y envía `POST /api/shipments`. La URL de retorno y el scope están en [environment.ts](frontend/src/environments/environment.ts).
2. **Entrada al backend.** [Nginx](frontend/nginx.conf) conserva la ruta `/api/shipments` y la reenvía al BFF. [SecurityConfig](bff-caso3/src/main/java/cl/duoc/api_backend/Config/SecurityConfig.java) configura la seguridad. [BffController](bff-caso3/src/main/java/cl/duoc/api_backend/controller/BffController.java) delega en el servicio de envíos y devuelve **201 Created** al navegador.
3. **Persistencia y publicación.** [EnvioService](microservicio-shipments/src/main/java/com/rutaexpress/shipments/service/EnvioService.java) asigna estado `CREADO` y un código `REX-…`, guarda la entidad mediante el repositorio JPA y usa `RabbitTemplate` para publicar. Envía el evento `shipment.created` a `shipments.exchange` para catálogo y tres comandos a `cmd.direct`. En la implementación actual, una excepción de publicación queda registrada y el método puede devolver el envío guardado. Por eso se verifica la entrega con el mismo REX en los logs del consumidor.
4. **Procesamiento y confirmación.** [NotifyListener](microservicio-notify/src/main/java/com/rutaexpress/notify/listener/NotifyListener.java) lee el cuerpo JSON, valida los campos del dominio, escribe el resultado y ejecuta `basicAck`. Ante un fallo de procesamiento registra el error y ejecuta `basicNack` sin reencolar. El broker envía el mensaje a la DLQ correspondiente mediante `cmd.dead.dlx`. El conversor de notify permite leer el cuerpo sin cargar la clase Java `Envio` del productor.
5. **Cambios de estado.** Al pasar de `CREADO` a `ACEPTADO`, el servicio reduce la capacidad del servicio de catálogo. Para pasar a `EN_RUTA` exige `ACEPTADO` o `EN_BODEGA`. Los tres comandos de la tabla se publican al crear el envío.

| Routing key en `cmd.direct` | Cola principal | Resultado en `/app/resultados` | DLQ |
|---|---|---|---|
| `email.send` | `q.cmd.email` | `email/<REX>.txt`, aviso de correo local | `q.cmd.email.dlq` |
| `warehouse.ticket` | `q.cmd.warehouse` | `warehouse/<REX>.txt`, ticket de picking | `q.cmd.warehouse.dlq` |
| `label.gen` | `q.cmd.label` | `label/<REX>.pdf`, guía de despacho | `q.cmd.label.dlq` |

La topología también declara bindings en `cmd.topic` para `email.*`, `warehouse.#` y `label.*`. Los dos módulos empaquetan su propia copia de `CommandTopology.java` ([shipments](microservicio-shipments/src/main/java/com/rutaexpress/messaging/CommandTopology.java) y [notify](microservicio-notify/src/main/java/com/rutaexpress/messaging/CommandTopology.java)). Si cambia un nombre o routing key, actualizar ambas copias.

## Verificar un pedido de extremo a extremo

En Firefox, abrir **F12 → Red**, crear un pedido y copiar su `codigoSeguimiento` desde la respuesta **201** del BFF. En la EC2 de aplicaciones:

```bash
read -rp "Codigo REX del pedido: " codigo
docker logs ms-notify 2>&1 | grep -F "codigo=$codigo "
docker exec ms-notify ls -lh \
  "/app/resultados/email/$codigo.txt" \
  "/app/resultados/warehouse/$codigo.txt" \
  "/app/resultados/label/$codigo.pdf"
```

La prueba queda completa cuando se ven tres líneas ACK y tres archivos con el REX del navegador. En el despliegue EV2 se verificó con `REX-3DF89E8D`.

### Probar el rechazo hacia las DLQ

Desde la raíz del repositorio en la EC2 de aplicaciones, con `bash`, `curl`, `jq` y acceso a Docker:

```bash
(
  read -rp "Usuario RabbitMQ: " RABBIT_USER
  read -rsp "Clave RabbitMQ: " RABBIT_PASSWORD
  echo
  export RABBIT_USER RABBIT_PASSWORD
  bash pruebas/verificar-rabbit.sh
)
```

El [script](pruebas/verificar-rabbit.sh) publica un mensaje válido y otro con error por dominio. Busca el ACK del válido y comprueba el código de prueba, la cola de origen y `x-death.reason = rejected` en la DLQ del fallido.

## Reconstruir imágenes desde el código

Desde la raíz de la rama `fix/rabbitmq-ev2`, ejecutar el comando del componente modificado:

```bash
docker build -t rutaexpress-frontend:ev2-login ./frontend
docker build -t ms-shipments:ev2 ./microservicio-shipments
docker build -t ms-notify:ev2-json ./microservicio-notify
docker build -t ms-admin:ev2 ./microservicio-admin
docker build -t ms-catalog ./microservicio-catalog
docker build -t bff-caso3 ./bff-caso3
```

Un contenedor existente sigue asociado a la imagen con la que fue creado. Para desplegar un cambio se recrea únicamente ese componente conservando red, puertos, variables y volúmenes. El Compose incluido cubre los brokers. Para reproducir el laboratorio en máquinas distintas también hay que provisionar Oracle, configurar las direcciones de RabbitMQ, catálogo, envíos y BFF, y disponer del registro de aplicación, ámbito y redirección en Entra correspondientes al entorno.
