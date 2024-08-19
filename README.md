🌟 Advanced Apache Kafka Cluster Management 🌟

Discover the advanced Apache Kafka management system I developed in Java, which enables efficient and reliable management of brokers, topics, and clients. This system is ideal for handling large-scale data streams effectively.

🔑 Key Components:

    KafkaBroker: Manages brokers with functionalities for adding/removing topics, configuration validation, and monitoring partitions and replicas.
    KafkaClient: Base class for sending and receiving messages, with topic integrity validation.
    KafkaConsumer: Consumes messages from partitions and updates offsets.
    KafkaProducer: Produces messages to topics (both keyed and non-keyed), managing partitions and message consumption.
    KafkaPartition: Manages partitions with offsets, replicas, and message queues.
    KafkaReplica: Ensures data durability by managing replicas.
    KafkaTopic: Manages topics including partitions, producers, consumers, and file handling.
    KeyedMessage: Represents messages with a key and value for storage and management.
    LinkedStack: Implements a stack for efficient message storage.
    Message: Abstract class with message comparison based on ingestion time.
    KafkaCLI_2: Command-line tool for loading messages, saving, and exiting.

🔍 Key Features:

    Broker and Topic Management: User-friendly management of brokers and topics with built-in validation checks.
    Partition and Replica Handling: Dynamic management of partitions and replicas.
    Message Storage: Serialization and storage of messages.
    File Reading and Distribution: File management and message distribution to producers.
    Validation: Comprehensive parameter validation to ensure system integrity.
    Dynamic Reporting: Generation of detailed reports for brokers and topics.
