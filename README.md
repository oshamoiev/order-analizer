## Stream Analytics Pattern implementation

In this project Stream Analytics Pattern is implemented.

Orders are generated randomly in some time intervals and sent to the Kafka topic in a separate thread.

Generated orders are read from the Kafka topic using Kafka Streams and analyzed. Analized data is persisted into MariaDB.

The goal s is to analyze orders every n seconds to compute product-wise total value (quantity * price).

