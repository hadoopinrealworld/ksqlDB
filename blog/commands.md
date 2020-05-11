## Example Data

Orders - {"id": 1009,"customername":"Peter","date":1588907310,"amount":1144.565,"item_quantity":26}
Customers - {"customername":"Peter", "country": "AUSTRALIA", "customer_type": "PLATINUM"}

## Confluent Commands

confluent local start ksql-server

## Kafka Topics

kafka-topics --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic order_topic
kafka-topics --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic customer_topic
kafka-topics --zookeeper localhost:2181 --delete --topic orders_topic
kafka-topics --zookeeper localhost:2181 --describe

## KSQL Datagen Commands

ksql-datagen schema=./blog/datagen/orders.avro format=json topic=orders key=id maxInterval=5000 iterations=10
ksql-datagen schema=/Users/karthikdivya/Desktop/Personal/Udemy/KSQL/blog/datagen/customers.avro format=json topic=customers_topic_test key=customername maxInterval=10000 iterations=10

## STREAMS

kafka-topics --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic orders
ksql-datagen schema=/Users/karthikdivya/Desktop/Personal/Udemy/KSQL/blog/datagen/orders.avro format=json topic=orders  key=id maxInterval=5000 iterations=10

CREATE STREAM orders_stream(id INT, customername VARCHAR, date BIGINT, amount DOUBLE, item_quantity INT)
WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='orders')

SELECT customername,TIMESTAMPTOSTRING(date, 'dd/MMM') AS purchase_date, 
SUM(amount) AS total_amount, SUM(item_quantity) 
FROM orders_stream
GROUP BY customername, TIMESTAMPTOSTRING(date, 'dd/MMM') emit changes;


## TABLES

kafka-topics --zookeeper localhost:2181 --partitions 1 --replication-factor 1 --create --topic customers
ksql-datagen schema=/Users/karthikdivya/Desktop/Personal/Udemy/KSQL/blog/datagen/customers.avro format=json topic=customers  key=customername maxInterval=5000 iterations=10

CREATE TABLE customer_table(customername VARCHAR, country VARCHAR, customer_type VARCHAR) 
WITH (VALUE_FORMAT='JSON', KAFKA_TOPIC='customers', KEY='customername');

SELECT customername, country, customer_type from customer_table emit changes;

## JOINS

ksql-datagen schema=/Users/karthikdivya/Desktop/Personal/Udemy/KSQL/blog/datagen/customers.avro format=json topic=customers  key=customername maxInterval=5000 iterations=10

ksql-datagen schema=/Users/karthikdivya/Desktop/Personal/Udemy/KSQL/blog/datagen/orders.avro format=json topic=orders  key=id maxInterval=5000 iterations=10

SELECT customer.customername, customer.country, customer.customer_type as customer_status,
TIMESTAMPTOSTRING(order.date, 'dd/MMM') AS purchase_date,
SUM(order.amount) as total_amount, SUM(order.item_quantity) as total_quantity
FROM orders_stream as order 
LEFT JOIN customer_table as customer 
ON order.customername = customer.customername
GROUP BY customer.customername, customer.country, customer.customer_type,
TIMESTAMPTOSTRING(order.date, 'dd/MMM')
emit changes;
