# RabbitMQ

[RabbitMQ](https://www.rabbitmq.com/) is a message broker, which means is handles the sending and receiving of messages between different code modules. We need to use RabbitMQ for the waveform data pipeline to prevent the Mindray eGateway from being overwhelmed if there are problems with the pipeline server-side; as long as the Receiver module is still operating, the incoming messages will be queued up for processing at a later stage. This setup also allows us to better separate the different modules so that they can be brought offline and online again without loss of data. It also provides a convenient way for handling any errors during parsing without losing data.

Full documentation on RabbitMQ can be found on the [official website](https://www.rabbitmq.com/), however the essence with regards to the waveform data pipeline is:

RabbitMQ provides a series of message queues that other containers on the network can send messages to and receive messages from. The Receiver module sends HL7 messages it picks up from the eGateway to the `hl7` queue. The Parser module then receives messages from this queue, parses them into common message format (CMF), and publishes them to the `cmf_exchange` exchange. From here they can be picked up and ingested into an AtriumDB database.

For errors during parsing, the Parser publishes to a number of additional queues: one for each fail point.

* `hl7_unknown_signal`
* `hl7_unknown_bedspace`
* `hl7_format_error`
* `hl7_missing_field_error`
* `hl7_invalid_timestamp_error`
* `hl7_signal_data_error`
* `hl7_unhandled_error`

## Monitoring RabbitMQ

RabbitMQ metrics are scraped by Prometheus and can be visualised downstream using Grafana. We have also provided a monitoring script (`utils/rabbitmq/monitor.sh`) which can be used to monitor RabbitMQ locally. It provides output like:

```
RabbitMQ Queues - Mon Dec  1 09:31:51 UTC 2025
------------------------------------------------------------
QUEUE NAME                     MESSAGES   CONSUMERS  STATE
------------------------------------------------------------
cmf_checker                    0          1          running
hl7_invalid_timestamp_error    0          0          running
hl7_unknown_bedspace           0          0          running
hl7_format_error               0          0          running
hl7_unhandled_error            0          0          running
hl7_signal_data_error          0          0          running
hl7_missing_field_error        0          0          running
hl7_unknown_signal             0          0          running
cmf_atriumdb                   10         1          running
hl7                            0          1          running
```

i.e. there are 10 messages in the `cmf_atriumdb` queue

## Configuration

RabbitMQ is configured using the file at `utils/rabbitmq/definitions.template.json`. This defines all the available queues, users and passwords. You will need to set up a RabbitMQ user/password in here.

## Tools for interfacing with RabbitMQ

A wrapper module for interfacing with RabbitMQ can be found in `src/core/rabbitmq.py`. This module contains two classes for connecting to RabbitMQ: `SyncRMQConnection` and `AsyncRMQConnection`. These classes are used by the Parser and Receiver modules. 
