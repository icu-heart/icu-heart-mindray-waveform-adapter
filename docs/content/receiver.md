# Receiver module

The Receiver module listens to the Mindray eGateway and 

* Pushes incoming HL7 messages to the `hl7` [RabbitMQ](rabbitmq.md) queue
* Saves a copy to local disk as temporary backup (recommended 1 year storage)

The Receiver module can be launched by running the `src/python-hl7_receiver.py` script. This

1. starts an [asynchronous connection to RabbitMQ](rabbitmq.md#tools-for-interfacing-with-rabbitmq)
2. listens to the message stream incoming from `EGATEWAY_PORT` sent via [MLLP](https://en.wikipedia.org/wiki/Health_Level_7#MLLP).
3. splits up messages from the stream based on standard MLLP framing bytes: start block (SB); end block (EB) = file separator (FS) + carriage return (CR)
4. processes each message
    * parses required HL7 fields to get a the control ID (unique identifier for the message) and message timestamp. If this fails, the message is declared invalid and the metric `hl7_receiver_messages_rejected_total` is incremented.
    * publishes the message to RabbitMQ `hl7` queue with a message ID formed from the control ID and timestamp
    * writes the message to file with a filepath formed from the control ID and timestamp
    * sends an acknowledgement to the eGateway that the message is received

The Receiver sends various [metrics over OTel](metrics.md) under the service name `hl7.receiver` which can be processed using Prometheus and Grafana. These include counts of rejected, published and archived messages as well as the message processing duration and read buffer size, which are useful for performance monitoring.