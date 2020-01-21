# Timed Worker Pool
Worker pool pattern implemented with a maximum rate of work. Consumption from the consumer is throttled by an intermediary which batches work and places it on a read channel at a pre-specified interval.
