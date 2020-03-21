# KSQL Latest N User Defined Aggregate Function (UDAF)

This is an example of a UDAF which retrieves the latest N elements in a group by KSQL query.


---

Table of Contents

* <a href="#Requirements">Versions and Requirements</a>
* <a href="#Learn">Want to Learn More?</a>
* <a href="#License">License</a>

---

<a name="Requirements"></a>

# Versions and Requirements

Compatible [KSQL](https://github.com/confluentinc/ksql) versions:

Requirements to locally build, test, package the UDF/UDAF examples:

* Java 8+
* Maven 3.0+

To package the UDFs/UDAFs ([details](https://docs.confluent.io/current/ksql/docs/developer-guide/implement-a-udf.html#build-the-udf-package)):

```bash
# Create a standalone jar ("fat jar")
$ mvn clean package

# >>> Creates target/ksql-udaf-latestn-1.0-SNAPSHOT.jar
```

To deploy the packaged UDFs/UDAFs to KSQL servers, refer to the
[KSQL documentation on UDF/UDAF](https://docs.confluent.io/current/ksql/docs/developer-guide/udf.html#deploying).
You can verify that the UDFs/UDAFs are available for use by running `SHOW FUNCTIONS`, and show the details of
any specific function with `DESCRIBE FUNCTION <name>`.

To use the UDFs/UDAFs in KSQL ([details]()):

```sql
CREATE STREAM balances (b1 BIGINT, b2 BIGINT, d1 DOUBLE, d2 DOUBLE, v VARCHAR)
  WITH (VALUE_FORMAT = 'AVRO', KAFKA_TOPIC = 'balances', KEY='b1');

SELECT b1, LATESTN(b1, 2) FROM balances GROUP BY b1 EMIT CHANGES;


```


<a name="Learn"></a>

# Want to Learn More?

* Head over to the [KSQL documentation](https://docs.confluent.io/current/ksql/).

<a name="License"></a>

# License

See [LICENSE](LICENSE) for licensing information.
