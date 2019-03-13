# Sample Solution for Manual Offset

This is a sample solution for the problem of saving the offset manually in an external file and resetting the consumer to the last stored offset (+1, to not handle the last record twice).

1. Run the Kafka cluster
2. Create a topic `sample-topic-net` with 1 partition.
3. In a first terminal run the producer
4. In a second terminal run the consumer
    a. Run the consumer for a short while (it will start at offset 0)
    b. Stop the consumer with `CTRL-c`
    c. Start consumer again; it will continue at the offset where it left off
    d. repeat