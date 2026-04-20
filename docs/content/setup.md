# Setup

This page contains information on how to set up the necessary ICU-Heart Mindray Waveform Adapter components. We recommend using quadlets or a network of containers, one for each component; however, this is not strictly necessary. 

For more information on running the components (parameters etc.), see the rest of these docs.

## RabbitMQ

RabbitMQ can be set-up using various configuration files, which we have provided examples of in `utils/rabbitmq`.

What you need:

| Item | Information | 
| --------- | ----------- |
| RabbitMQ username | Generate this yourself |
| RabbitMQ password | Generate this yourself |
| RabbitMQ password hash | This can be generated using the RabbitMQ CLI (`rabbitmqctl hash_password`) |
| RabbitMQ port | If containerised setup, you only need to expose this within the container network or equivalent. |
| definitions.json | Specifies schema (see [RabbitMQ's docs](https://www.rabbitmq.com/docs/definitions)). The queues, exchanges etc. have been set up in here as is needed for the Adapter, but you will need to replace "\_\_RABBITMQ_USER\_\_" and "\_\_RABBITMQ_PASSWORD_HASH\_\_" with your username and password hash. |
| enabled_plugins | Specifies plugins, we have included the Prometheus plugin which you will need if you want to scrape RabbitMQ metrics using Prometheus |
| rabbitmq.conf | [Config file](https://www.rabbitmq.com/docs/configure) which specifies where your definitions.json can be found. To connect to Prometheus, you need to specify the Prometheus port and IP here as well. |

## Receiver

The HL7 Receiver is run using the script `src/python-hl7_receiver.py`

| Item | Information | 
| --------- | ----------- |
| Port for Mindray eGateway | Specify using `--port` argument when running |
| Prometheus port (if using) | Specify using "OTEL_ENDPOINT" environment variable e.g. "OTEL_ENDPOINT=http://prometheus:<prometheus_port\>/api/v1/otlp/v1/metrics |
| RabbitMQ username and password | Specify using "RABBITMQ_USER" and "RABBITMQ_PASSWORD" environment variables |
| Path to write backup .hl7 files to | This is set as "/app/hl7" in the script, you can replace this as needed. |

## Parser

The HL7 Receiver is run using the script `src/python-hl7_parser_realtime.py`. To run the error queue parser, use `src/python-hl7_parser_errors.py`

| Item | Information | 
| --------- | ----------- |
| Prometheus port (if using) | Specify using "OTEL_ENDPOINT" environment variable e.g. "OTEL_ENDPOINT=http://prometheus:<prometheus_port\>/api/v1/otlp/v1/metrics |
| RabbitMQ username and password | Specify using "RABBITMQ_USER" and "RABBITMQ_PASSWORD" environment variables |
| mapping_registry.json | This provides a mapping between wards/bedspaces as they appear in the .hl7 messages and how you would like them to appear in the CMF messages produced by the parser. It also provides a list of which signals to include/not-include in CMF. An example file is in `src/core/`, which you will need to edit to fit your setup. |

## Prometheus

Set this up like normal but you will also need:

| Item | Information | 
| --------- | ----------- |
| prometheus.yml | Specifies where to scrape metrics from. There is an example in `utils/prometheus` set up to scrape from RabbitMQ and Prometheus. Metrics from the Parser/Receiver aren't scraped but are sent directly |

## Grafana

Set this up like normal and connect to Prometheus. 