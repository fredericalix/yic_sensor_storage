# Service of sensors storage

Wait on 'sensors' RabbitMQ topic for every routing key, and store it into a PostgreSQL DB

It creates its SQL table if it does not already exist.

## Compile & run

    go build
    ./sensor-storage

## Config by Environment variables

``` sh
RABBITMQ_URI="amqp://guest:guest@localhost:5672"
POSTGRESQL_URI="postgresql://postgres:passwords@localhost:5432/yic_sensors?sslmode=disable"
```
