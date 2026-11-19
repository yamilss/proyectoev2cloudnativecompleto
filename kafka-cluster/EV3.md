# EV3 · RutaExpress

Base EV2: rama fix/rabbitmq-ev2, commit 93f9e11. Ejecutar en una copia separada del proyecto. En el laboratorio los tres brokers comparten un anfitrión. Las réplicas toleran la pérdida de un broker con ISR suficiente; la caída de la EC2 afecta a todo el clúster.

## 1. Infraestructura y administración

Revisar memoria disponible, espacio del volumen Docker y puertos antes del arranque. Los JVM de Kafka y ZooKeeper pueden usar hasta 2,25 GiB de heap en total, además de memoria nativa, caché, UI, administrador y servicios EV2. Ajustar la ubicación del clúster según capacidad. Este Compose supone el mismo anfitrión que shipments en red host.

```bash
cd kafka-cluster
docker compose config -q
docker compose up -d
bash topicos.sh
docker compose --profile app up -d --build kafka-admin
docker compose ps
bash probar-admin.sh
```

Ejecutar la prueba cuando el administrador responda en localhost:9095. Kafka UI queda en localhost:8080; usar un túnel SSH a la EC2 de aplicaciones con -L 8080:127.0.0.1:8080. El frontend conserva el túnel de EV2 al puerto 4200.

Los dos tópicos tienen tres particiones, tres réplicas y cleanup.policy=delete. envios-eventos conserva siete días de movimientos para reportería y envios-auditoria treinta para trazabilidad. Son valores elegidos para el laboratorio. topicos.sh crea y comprueba la configuración; si ya existe un tópico diferente, muestra el desajuste.

## 2. Preparar y desplegar los servicios

Desde la raíz del repositorio, compilar antes de intervenir los servicios actuales:

```bash
docker build -t ms-notify:ev3 microservicio-notify &&
docker build -t ms-shipments:ev3 microservicio-shipments
```

En la EC2, obtener del entorno autorizado las credenciales actuales de Oracle y RabbitMQ. Las imágenes nuevas aceptan ORACLE_USER, ORACLE_PASSWORD, RABBIT_USER y RABBIT_PASSWORD (o propiedades SPRING_DATASOURCE_* / SPRING_RABBITMQ_*). El Compose RabbitMQ para una instalación nueva requiere RABBITMQ_ERLANG_COOKIE, RABBITMQ_DEFAULT_USER y RABBITMQ_DEFAULT_PASS. El clúster RabbitMQ ya desplegado se reutiliza. Rotar por separado las credenciales históricas publicadas en EV2.

En una terminal Bash de la EC2 de aplicaciones, con los servicios EV2 funcionando y los tópicos listos:

```bash
(
  set -eu
  read -rp "Usuario RabbitMQ: " RABBIT_USER
  read -rsp "Clave RabbitMQ: " RABBIT_PASSWORD; echo
  read -rp "Usuario Oracle de shipments: " ORACLE_USER
  read -rsp "Clave Oracle: " ORACLE_PASSWORD; echo
  export RABBIT_USER RABBIT_PASSWORD ORACLE_USER ORACLE_PASSWORD
  umask 077
  tmpdir=$(mktemp -d)
  trap 'rm -rf "$tmpdir"' EXIT
  for service in ms-notify ms-shipments; do
    docker image inspect "$service:ev3" >/dev/null
    if docker inspect "$service-previo-kafka" >/dev/null 2>&1; then
      echo "Ya hay un respaldo $service-previo-kafka. Revisar docker ps -a."; exit 1
    fi
    docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$service" > "$tmpdir/$service"
  done
  docker network inspect rutaexpress-kafka >/dev/null
  for service in ms-notify ms-shipments; do
    docker stop "$service"
    docker rename "$service" "$service-previo-kafka"
    if [[ $service == ms-notify ]]; then
      docker run -d --name "$service" --network rutaexpress-kafka -p 9093:9093 \
        --env-file "$tmpdir/$service" -e RABBIT_USER -e RABBIT_PASSWORD \
        -v rutaexpress-resultados:/app/resultados "$service:ev3"
    else
      docker run -d --name "$service" --network host --env-file "$tmpdir/$service" \
        -e ORACLE_USER -e ORACLE_PASSWORD -e RABBIT_USER -e RABBIT_PASSWORD "$service:ev3"
    fi
  done
)
```

Verificar en los logs el arranque de ambos servicios y la conexión a Oracle, RabbitMQ y Kafka. Ante un fallo, para cada servicio que llegó a renombrarse: detener y renombrar el nuevo contenedor si existe, devolver el nombre original al contenedor con sufijo -previo-kafka y arrancarlo. Los datos de Oracle y los resultados de notify usan la persistencia de EV2.

## 3. Demostrar el caso y los indicadores

1. Entrar a Angular como Cliente, crear un envío y anotar su REX. En el dashboard autorizado cambiar su estado por una transición válida. Verificar ambos hechos desde el productor y ambos consumidores con el mismo código:
   ```bash
   read -rp 'REX del envío: ' rex
   docker logs ms-shipments 2>&1 | grep -F "$rex"
   docker logs ms-notify 2>&1 | grep -F "$rex"
   docker exec ms-notify find /app/resultados/kafka -name '*.json'
   ```
2. En Kafka UI inspeccionar envios-eventos y envios-auditoria: clave REX, JSON, partición, offset, líder, réplicas e ISR. El DTO EventoEnvio incluye eventId, tipo, fecha UTC, ID del envío, REX y estados. El par negocio/auditoría comparte identificador para correlación. Se usa JSON UTF-8 mediante serializers String y mapeo tipado en Java.
3. EventPublisher, dentro de shipments, es el único productor. KafkaListeners recibe ambos tópicos y delega en ProcesamientoEnvio. Reportería clasifica movimientos por estado; auditoría conserva el registro de la transición. Cada resultado se escribe por eventId mediante reemplazo atómico para que una reentrega del mismo registro no duplique el archivo. El productor espera acks=all en la confirmación asíncrona y registra errores definitivos. Oracle y los dos envíos Kafka son operaciones independientes; existe una ventana de pérdida si el proceso cae tras guardar o una publicación agota sus reintentos.
4. Ejecutar bash kafka-cluster/grupos.sh. Comparar CURRENT-OFFSET con LOG-END-OFFSET. LAG muestra la diferencia; si aumenta entre consultas, el grupo está acumulando trabajo. Identificar los dos miembros del grupo ruta-reporteria y cómo se reparten las tres particiones. Explicar la asignación del grupo ruta-auditoria por separado.
5. Probar el manejo de un JSON inválido en el tópico de negocio usando la misma clave REX del envío. Después realizar otra transición válida de ese envío; comprobar el descarte con partición/offset y el avance posterior de esa misma partición.
   ```bash
   printf '%s|{invalido}\n' "$rex" | docker compose -f kafka-cluster/compose.yaml exec -T kafka-1 \
     kafka-console-producer --bootstrap-server kafka-1:9092 --topic envios-eventos \
     --property parse.key=true --property 'key.separator=|'
   docker logs --since 3m ms-notify 2>&1 | grep -F "$rex"
   ```
   Los errores de validación se descartan con registro contextual. Los fallos recuperables de procesamiento tienen dos reintentos separados por un segundo.
6. Para EV2, verificar los tres ACK del REX y los archivos de correo, picking y PDF. Ejecutar pruebas/verificar-rabbit.sh con sus variables de entorno como indica el README de EV2. Conservar evidencia de login/JWT, ruta pública del proyecto, despliegue y administración RabbitMQ.
7. Para presentar en diez minutos: arquitectura y clúster (2 min), tópicos y réplica (1 min), envío real y procesamiento (3 min), administración y grupos (2 min), error y continuidad EV2 (2 min). Entregar los enlaces de frontend y backend de la rama efectivamente probada.
