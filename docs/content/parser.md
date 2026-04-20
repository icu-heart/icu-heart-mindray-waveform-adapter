# Parser module

The Parser module parses HL7 messages and pushes them to the `cmf_atriumdb` exchange in common message format (CMF). 

The Parser can pull messages from three different sources for three different use cases. Each of these is run with a separate script.

| Use case | HL7 source | Script | Notes |
| -------- | ---------- | ------ | ----- |
| Realtime | `hl7` [RabbitMQ](rabbitmq.md) queue | hl7_parser_realtime.py 
| Retrying previously failed messages | RabbitMQ error queues | hl7_parser_errors.py 

You can have multiple instances of these running at the same time (i.e. if you want to ingest from multiple error queues). The realtime parser will be prioritised.

## Structure of an HL7

Waveform data are received in the form of HL7 messages, one per bed per second. HL7 (Health Level 7 version 2) defines standards for electronic message formats for sending health information. A dummy waveform HL7 message is as follows:

```
MSH|^~\&|Mindray_eGateway|WARDNAME|ReceivingApp|ReceivingFacility|20251006120000||ORU^R01|MSGID1234|P|2.3.1
PID|1||123456^^^MindrayMRN||Lastname^Firstname||19800101|M|||123 Main St^^City^ST^12345||(555)555-5555|||M||123456789
PV1|1|I|WARDNAME^BEDNAME||||1234^Doctor^Test||||||||||1234567|||||||||||||||||||||||||20251006115900
OBR|1||ORDER1234^Mindray_eGateway|WaveformReport^Waveform Observation|||20251006115900||||||||1234^Doctor^Test|||||||||
OBX|1|NA|131330^MDC_PULS_OXIM_PLETH^MDC|1.7.6.131330|Pleth signal
OBX|2|NM|0^MDC_ATTR_SAMP_RATE^MDC|1.7.6.131330.1|125|264640^MDC_DIM_HZ^MDC|||||R
OBX|3|NM|67917^MDC_ATTR_NU_MSMT_RES^MDC|1.7.6.131330.2|0.003906|266418^MDC_DIM_PERCENT^MDC|||||R
OBX|4|NM|196660^MDC_EVT_INOP^MDC|1.7.6.131330.3|1||||||R
OBX|5|NA|131310^MDC_ECG_ELEC_POTL_I^MDC|1.7.6.131310|Lead I
OBX|6|NM|0^MDC_ATTR_SAMP_RATE^MDC|1.7.6.131310.1|256|264640^MDC_DIM_HZ^MDC|||||R
OBX|7|NM|67917^MDC_ATTR_NU_MSMT_RES^MDC|1.7.6.131310.2|0.02|266418^MDC_DIM_MILLI_VOLT^MDC|||||R
OBX|8|NM|196660^MDC_EVT_INOP^MDC|1.7.6.131310.3|0||||||R
OBX|9|NA|131311^MDC_ECG_ELEC_POTL_II^MDC|1.7.6.131311|Lead II
OBX|10|NM|0^MDC_ATTR_SAMP_RATE^MDC|1.7.6.131311.1|256|264640^MDC_DIM_HZ^MDC|||||R
OBX|11|NM|67917^MDC_ATTR_NU_MSMT_RES^MDC|1.7.6.131311.2|0.02|266418^MDC_DIM_MILLI_VOLT^MDC|||||R
OBX|12|NM|196660^MDC_EVT_INOP^MDC|1.7.6.131311.3|0||||||R
OBX|13|NA|131312^MDC_ECG_ELEC_POTL_III^MDC|1.7.6.131312|Lead III
OBX|14|NM|0^MDC_ATTR_SAMP_RATE^MDC|1.7.6.131312.1|256|264640^MDC_DIM_HZ^MDC|||||R
OBX|15|NM|67917^MDC_ATTR_NU_MSMT_RES^MDC|1.7.6.131312.2|0.02|266418^MDC_DIM_MILLI_VOLT^MDC|||||R
OBX|16|NM|196660^MDC_EVT_INOP^MDC|1.7.6.131312.3|0||||||R
OBX|17|NA|131313^MDC_ECG_ELEC_POTL_AVR^MDC|1.7.6.131313|Lead aVR
OBX|18|NM|0^MDC_ATTR_SAMP_RATE^MDC|1.7.6.131313.1|256|264640^MDC_DIM_HZ^MDC|||||R
OBX|19|NM|67917^MDC_ATTR_NU_MSMT_RES^MDC|1.7.6.131313.2|0.02|266418^MDC_DIM_MILLI_VOLT^MDC|||||R
OBX|20|NM|196660^MDC_EVT_INOP^MDC|1.7.6.131313.3|0||||||R
OBX|21|NA|131314^MDC_ECG_ELEC_POTL_AVL^MDC|1.7.6.131314|Lead aVL
OBX|22|NM|0^MDC_ATTR_SAMP_RATE^MDC|1.7.6.131314.1|256|264640^MDC_DIM_HZ^MDC|||||R
OBX|23|NM|67917^MDC_ATTR_NU_MSMT_RES^MDC|1.7.6.131314.2|0.02|266418^MDC_DIM_MILLI_VOLT^MDC|||||R
OBX|24|NM|196660^MDC_EVT_INOP^MDC|1.7.6.131314.3|0||||||R
OBX|25|NA|131315^MDC_ECG_ELEC_POTL_AVF^MDC|1.7.6.131315|Lead aVF
OBX|26|NM|0^MDC_ATTR_SAMP_RATE^MDC|1.7.6.131315.1|256|264640^MDC_DIM_HZ^MDC|||||R
OBX|27|NM|67917^MDC_ATTR_NU_MSMT_RES^MDC|1.7.6.131315.2|0.02|266418^MDC_DIM_MILLI_VOLT^MDC|||||R
OBX|28|NM|196660^MDC_EVT_INOP^MDC|1.7.6.131315.3|0||||||R
OBX|29|NA|131331^MDC_ECG_ELEC_POTL_V1^MDC|1.7.6.131331|Lead V1
OBX|30|NM|0^MDC_ATTR_SAMP_RATE^MDC|1.7.6.131331.1|256|264640^MDC_DIM_HZ^MDC|||||R
OBX|31|NM|67917^MDC_ATTR_NU_MSMT_RES^MDC|1.7.6.131331.2|0.02|266418^MDC_DIM_MILLI_VOLT^MDC|||||R
OBX|32|NM|196660^MDC_EVT_INOP^MDC|1.7.6.131331.3|0||||||R
```

The message is broken down into sections:

* Message header (MSH)
* Patient identification (PID) 
* Patient visit segment 1 (PV1) - information about the patient's encounter
* Observation request (OBR) - information on origin of request for data
* Observation result (OBX) - values from observation

Each waveform signal generates four consecutive OBX lines:

* The first has the signal name and values. Individual values are ^ separated e.g. in the example above instead of "Pleth signal" you would actually see something like "1^2^3^4^..." (Narrative/NA type)
* The second has the sample rate (Numeric/NM type)
* The third has the measurement resolution and unit (Numeric/NM type)
* The fourth is for returning device inoperability/error cocdes (Numeric/NM type)

## Extracting the relevant information

Conversion from HL7 to CMF is done by the `SignalDataExtractor` class in "containers/building/python/code/core/mindray_hl7_parser.py". Not all information is kept in this step.

| Segment | Included Information                             | Notes                                                              |
|---------|-------------------------------------------------|--------------------------------------------------------------------|
| MSH     | Nothing                                         |                                                                    |
| PID     | Nothing                                         | Patient info currently unreliable (manually input on monitor)     |
| PV1     | Ward and bed                                    |                                                                    |
| OBR     | Message timestamp                               |                                                                    |
| OBX     | Name, frequency, resolution, unit, values      | For each signal                                          |


A CMF message is then created with the following contents:

* Device ID (composed of ward and bed)
* Timestamp
* For each signal:
    * Name
    * Unit
    * Frequency
    * Values
    * Resolution


!!! note
    The Parser as provided is not currently configured to parse demographic information from the PID section of the .hl7. If you want to parse this information you will need to implement this yourself, and we welcome pull requests with this or other features.

## Bedspace and signal checks

During parsing, some consistency checks are carried out, namely that the recorded bedspace (bed, ward combination) and signal names belong to a known set, stored in `mapping_registry.json` in "containers/building/python/code/core". If an unknown signal or bedspace is recorded, an error is thrown. This is to prevent the flow of unexpected data into the waveform database. If a new bedspace or signal is to be monitored, the mapping registry must be updated accordingly. 

The mapping registry also provides a mapping for bedspace and signal names into a standardised format.

## Code

Parsing scripts can all be found in "containers/building/python/code". All of the parsing scripts are based on the `ParsingOrchestrator` class in the "core/parser_pipeline.py" module. This class implements the common parsing workflow which is common to all use cases.

Each individual script (for realtime, backlog and errors) contains an orchestrator class which inherits from the `ParsingOrchestrator`, adding specific implementation for the `_input_task` method, which defines how to pull HL7s for parsing. 

### Common parsing workflow

All parsers implement he common parsing workflow:

1. Initialise one or more parser workers; parsing can be parallelised by initialising more than one worker. A worker is initialised by:
    * Generating a unique ID for the specific worker process instance
    * Creating a worker as an instance of the `HL7DataExtractor` class
    * Checking connection to RabbitMQ if required
    * Initialising a [metrics manager](metrics.md)
2. Pull messages one by one from source
    * Extract the CMF from the message body
    * Publish the CMF to `cmf_exchange` exchange
    * Send any messages failing checks or erroring to the correct error queue
    * Record metrics

### Realtime parser

Parsing in realtime is carried out by `hl7_parser_realtime.py`. The script takes a `--max-parsers` argument for how many parser workers to spin up (default = 1). Messages are pulled from the `hl7` queue to ingest into the pipeline.

### Error parser

Any messages which lead to errors, including failing checks for known bedspaces and signals, are re-routed as HL7s to the corresponding RabbitMQ [error queues](rabbitmq.md#monitoring-rabbitmq).

Once the underlying issues have been resolved, the messages waiting in these queues can be parsed and ingested into AtriumDB. This is done by manually setting the `hl7_parser_errors.py` script to run from a shell attached to the `python-hl7_backlog` container. The script takes the following arguments:

| Argument   | Description |
|------------|-------------|
| `--error-queue` | The error queue to pull messages from. |
| `--max-parsers` | How many parser workers to spin up (default = 4) |
| `--disable-backpressure` | Because the data are all available and not pulled in realtime, backpressure is applied to prevent overwhelming the parser. Adding this flag disables the backpressure. |