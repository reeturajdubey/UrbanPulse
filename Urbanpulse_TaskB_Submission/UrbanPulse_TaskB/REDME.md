\# UrbanPulse Task B — Kafka Ingestion Layer



\## Contents

\- `code/bus\_gps\_producer.py` — GPS producer, keyed by route\_id

\- `code/air\_quality\_producer.py` — AQI producer with retry + 5% null simulation

\- `code/dlq\_validator.py` — DLQ validation + 5-min error report

\- `code/signals\_producer.py` — Traffic signals producer

\- `code/high\_priority\_consumer.py` — HIGH\_PRIORITY consumer group

\- `code/standard\_priority\_consumer.py` — STANDARD\_PRIORITY consumer group (3 instances)

\- `code/enrichment\_job.py` — Stream-table join (bus\_gps + route\_schedule)

\- `code/route\_schedule.csv` — Static reference data (KTable simulation)



\## How to Run

1\. Start Zookeeper: `bin/zookeeper-server-start.sh config/zookeeper.properties`

2\. Start Kafka: `bin/kafka-server-start.sh config/server.properties`

3\. Create topics (see report for exact commands)

4\. Run producers/consumers as needed (see report for sequence)



\## Notes

\- Single-node Kafka broker used for lab demonstration; production design (Task A) specifies 3-broker cluster with RF=3.

\- Enrichment implemented via Python in-memory dictionary join (KTable-simulation pattern) instead of native Kafka Streams Java API, due to lab constraints — logic mirrors Kafka Streams stream-table join semantics.

