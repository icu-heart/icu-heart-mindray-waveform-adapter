# Metrics

## What are metrics?

Metrics are numerical measurements of some part of the pipeline which are tracked over time. Each of the processes in the waveform pipeline produce metrics, and these can be gathered together by the metrics monitoring software [Prometheus](https://prometheus.io/). The metrics visualisation tool [Grafana](https://grafana.com/) can then be used to build and display dashboards from the gathered metrics. 

## Waveform pipeline metrics

Metrics are collected from different elements of the pipeline in different ways. The table is a summary of the metrics and their [collection methods](#collection-methods).

| Pipeline component | Collection method | Description | Metrics |
| ------------------ | ----------------- | ----------- | ------- |
| [RabbitMQ](rabbitmq.md) | Scraped by Prometheus (built-in metrics) | How many messages are in which queues, and how much memory is taken up by RabbitMQ | <details><summary>Click to expand</summary>Metrics defined under:<br><a href="https://www.rabbitmq.com/docs/prometheus#queue-coarse-metrics">queue_coarse_metrics</a><br><a href="https://www.rabbitmq.com/docs/prometheus#per-queue-consumer-count">queue_consumer_count</a><br><a href="https://www.rabbitmq.com/docs/prometheus#exchange-metrics">queue_exchange_metrics</a><br><a href="https://www.rabbitmq.com/docs/prometheus#detailed-queue-metrics">queue_metrics</a></details> |
| [Receiver](receiver.md) | Sent over OTel using [MetricsManager](#the-metricsmanager-class) | How many messages are received, written to file and published to RabbitMQ | <details><summary>Click to expand</summary>hl7_receiver_messages_received_total<br>hl7_receiver_messages_rejected_total<br>hl7_receiver_messages_published_total<br>hl7_receiver_rmq_publish_duration_ms<br>hl7_receiver_file_write_duration_ms<br>hl7_receiver_message_size<br>hl7_receiver_messages_written_total<br>hl7_receiver_message_processing_duration_ms<br>hl7_receiver_connections_total<br>hl7_receiver_socket_buffer_bytes<br>hl7_receiver_connections_failed_total</details> |
| [Parser](parser.md) | Sent over OTel using [MetricsManager](#the-metricsmanager-class) | How many messages are processed and published to RabbitMQ, and how long the parser is taking. Counts for unknown signals and beds. | <details><summary>Click to expand</summary>hl7_messages_processed_total<br>hl7_messages_published_total<br>hl7_message_errors_total<br>hl7_parse_batch_duration_ms<br>hl7_batch_size<br>hl7_signals_processed_total<br>hl7_unknown_bed_messages_total<br>hl7_unknown_signal_messages_total</details> |

### Collection methods

Metrics are collected in different ways depending on the component being monitored; as outlined in the table above.

#### Scraping using Prometheus

Some services can be scraped automatically by Prometheus because there are in-built metrics being recorded already. In the waveform pipeline, these are:

* Prometheus itself
* RabbitMQ

To configure RabbitMQ:

=== "conf.d/20-management_agent.disable_metrics_collector.conf"

    ```ini
    management_agent.disable_metrics_collector = true
    ```

Then metric scraping can be [configured in a prometheus.yml file](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) by adding jobs under `scrape_configs` which specify which metrics to scrape and from which targets. 

#### The MetricsManager class

This is an internal class for managing metrics in Python code. It is used in the pieces of the adapter which have been written internally. The class can be found in `src/core/utils/metrics.py`.

To use the metrics manager, first initialise an instance of the class by specifying a service name (a string for tagging the metrics) and an OTel endpoint for exporting the metrics via an [OTLP Metrics Exporter](https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/otlp/). For example:

```python
mm = MetricsManager(
    service_name="hl7.receiver"
    endpoint="http://prometheus:9090/api/v1/otlp/v1/metrics"
)
```

Note that the endpoint can also be set using the "OTEL_ENDPOINT" environment variable.

There are a few different [types of metrics](https://prometheus.io/docs/tutorials/understanding_metric_types/) supported:

* **Counters:** a number that can only increase or be reset. When a counter is recorded, the value recorded is added to the current value.
* **Gauges:** a number that can go up or down. When a gauge is recorded, the value recorded overwrites the current value.
* **Histograms:** counts within bins

To record a metric, you can use the `.record` method, e.g.

```python
mm.record({
    "counters": {
        # increment by 120
        "hl7_messages_processed_total": 120,
        # increment by 118 and add metadata with route: parsed
        "hl7_messages_published_total": (118, {"route": "parsed"}),
        # increment by 2 for ward A, bed 01
        # and by 1 for ward B, bed 07
        "hl7_unknown_bed_messages_total": [
            (2, {"ward": "A", "bed": "01"}),
            (1, {"ward": "B", "bed": "07"})
        ]
    },
    "gauges": {
        # replace with 5432
        "hl7_queue_depth": 5432
    },
    "histograms": {
        # Add three values which can later be viewed as counts within bins
        "hl7_parse_batch_duration_ms": [5.2, 6.1, 4.9]
    }
})
```

These recorded metrics are exported to the chosen endpoint by the [OTLP Metrics Exporter](https://opentelemetry.io/docs/specs/otel/metrics/sdk_exporters/otlp/). In our [waveform pipeline](index.md) this is set as an environment variable in the podman container e.g.

```sh
OTEL_ENDPOINT=http://prometheus:9090/api/v1/otlp/v1/metrics
```

which is pointing at the OTLP metrics endpoint on port 9090 on the prometheus container, which is also on the [wavenet](index.md#container-setup) network. This means the metrics will be monitored by the Prometheus installation.
