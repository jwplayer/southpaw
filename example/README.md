# Southpaw Example

This is a working example of an end-to-end pipeline using Southpaw and Debezium.

The Kafka topics are generated and populated using
[Debezium](https://github.com/debezium/debezium), which does data change capture
on databases including MySQL.

The database being used is the classic Northwind database adapted from
https://github.com/dalers/mywind.

## Run

```bash
make run
```

You can view the records by looking at the kafka-topics UI: http://localhost:8000.
The output should look something like the following image:
![Kafka topics UI](output.png)
