# Week 4 Spark Optimization Report

## Pipeline
Bronze → Silver Spark transformation for orders.

## Optimization Techniques Used

### 1. Partition Pruning
Silver data is partitioned by `order_date`.

Evidence:
- Spark physical plan shows `PartitionFilters`.
- Query on `order_date = 2026-05-01` avoids full dataset scan.

### 2. Repartition vs Coalesce
- `repartition(n, column)` triggers shuffle and redistributes data.
- `coalesce(n)` reduces partitions with less shuffle.
- Use repartition before wide writes or skewed distribution.
- Use coalesce only when reducing output files safely.

### 3. Adaptive Query Execution
Enabled:

```python
spark.sql.adaptive.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
```

AQE allows Spark to adjust query plans at runtime.

4. Skew Handling

Synthetic skew was created where customer_id = 999 owns most rows.

Mitigation:

* Salted aggregation distributes the hot key across multiple salted groups.
* Final aggregation recombines salted results.

Senior Engineering Notes

* Partition strategy improves read performance.
* AQE helps but does not remove need for data modeling decisions.
* Salting is useful for extreme skew.
* Avoid small files by tuning partitions before writes.
* Always inspect physical plans with explain("formatted").

Success Criteria

* Bronze → Silver Spark job works.
* Partition pruning confirmed.
* AQE enabled.
* Skew scenario simulated.
* Optimization report completed.