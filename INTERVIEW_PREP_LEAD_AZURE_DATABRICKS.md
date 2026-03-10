# Lead Azure Databricks Engineer — Interview Q&A + Coding Tests (PySpark + SQL)

## 1) Technical Interview Questions and Suggested Answers

### A. Architecture and Platform Design

1. **Q: How would you design a medallion architecture (Bronze/Silver/Gold) on Azure Databricks?**
   - **Answer:**
     - **Bronze:** Land raw data exactly as received (schema-on-read, audit columns, minimal transforms).
     - **Silver:** Apply cleansing, deduplication, standardization, conformance, and CDC merge logic.
     - **Gold:** Build business-level aggregates, dimensions, and fact tables for BI/ML serving.
     - Use **Delta Lake** for all zones, **Auto Loader** for incremental ingest, and **Unity Catalog** for governance.
     - Partition and optimize based on read patterns, not write convenience.

2. **Q: When would you use streaming vs batch in Databricks?**
   - **Answer:**
     - Use **streaming** when freshness SLAs are in minutes/seconds and events are continuous.
     - Use **batch** for daily/hourly jobs when latency tolerance is higher and dependencies are complex.
     - For many enterprise workloads, use a hybrid: streaming bronze/silver + scheduled gold refresh.

3. **Q: Explain how Delta Lake ACID transactions work in simple terms.**
   - **Answer:**
     - Delta stores data files plus a transaction log (`_delta_log`).
     - Every write creates a new atomic version.
     - Readers use snapshot isolation to see a consistent table version.
     - Concurrency is managed through optimistic concurrency control.

4. **Q: How do you enforce data governance and security on Azure Databricks?**
   - **Answer:**
     - Use **Unity Catalog** for centralized permissions (catalog/schema/table/view/function).
     - Apply least privilege with role/group-based grants.
     - Use column masks/row filters where required.
     - Integrate with Azure AD groups, Key Vault-backed secrets, and audit logs.

### B. Performance and Optimization

5. **Q: How do you optimize a slow Delta table query?**
   - **Answer:**
     - Check execution plan for shuffles, skew, and full scans.
     - Optimize file layout: avoid many tiny files, run `OPTIMIZE`, consider `ZORDER` on frequent filter columns.
     - Tune partitioning carefully (avoid high cardinality partition columns).
     - Cache selectively for repeated interactive reads.
     - Ensure predicates are pushdown-friendly and avoid unnecessary UDFs.

6. **Q: What causes small file problems and how do you fix them?**
   - **Answer:**
     - Frequent micro-batch writes or over-partitioned output can produce many small files.
     - Fix with Auto Optimize/optimized writes, periodic `OPTIMIZE`, and better partition strategy.
     - Use Auto Loader options and trigger intervals aligned with volume.

7. **Q: How do AQE and broadcast joins help in Spark?**
   - **Answer:**
     - **AQE (Adaptive Query Execution)** adjusts join strategy and partitioning at runtime.
     - **Broadcast join** ships the small table to executors to avoid expensive shuffles.
     - Together they reduce stage time and skew impact when configured well.

### C. Data Engineering Patterns

8. **Q: How would you implement incremental loads with CDC?**
   - **Answer:**
     - Land source changes with operation flags/timestamps.
     - Deduplicate by business key + latest change timestamp.
     - Use Delta `MERGE INTO` in Silver to apply inserts/updates/deletes.
     - Keep audit metadata: source file, load timestamp, batch_id.

9. **Q: How do you design idempotent pipelines?**
   - **Answer:**
     - Use deterministic keys and merge logic.
     - Track processed checkpoints/watermarks.
     - Write logic that can safely re-run without duplicating outcomes.
     - Separate raw landing from curated outputs.

10. **Q: Explain SCD Type 2 in Databricks.**
    - **Answer:**
      - Track historical dimension changes by creating new rows per change.
      - Maintain `effective_from`, `effective_to`, `is_current` fields.
      - Use merge logic to close old current record and insert new record.

### D. Azure + Operational Excellence

11. **Q: What Azure services commonly integrate with Databricks in enterprise data platforms?**
    - **Answer:**
      - ADLS Gen2 (storage), Azure Data Factory (orchestrator), Event Hubs (streaming), Key Vault (secrets), Synapse/Power BI (consumption), Azure Monitor/Log Analytics (observability).

12. **Q: How would you design CI/CD for Databricks workloads?**
    - **Answer:**
      - Source control in Git, branch strategy, PR checks, unit/integration tests.
      - Deploy notebooks/jobs/workflows via Databricks Asset Bundles or Terraform.
      - Use environment-specific configs and secrets from Key Vault.

13. **Q: How do you monitor and troubleshoot production failures?**
    - **Answer:**
      - Capture job-level and task-level metrics, data quality checks, and SLA alerts.
      - Use retries for transient issues; dead-letter patterns for bad records.
      - Correlate failures with cluster logs, Spark UI, and Delta transaction history.

14. **Q: What are your key leadership practices as a lead engineer?**
    - **Answer:**
      - Define engineering standards (naming, testing, observability, security).
      - Mentor team members through design reviews and pair debugging.
      - Create reusable frameworks for ingestion, validation, and deployment.
      - Communicate trade-offs to architecture and business stakeholders.

---

## 2) PySpark Coding Tests (with expected approach)

## Test 1 — Deduplicate latest customer records

### Problem
Given a DataFrame with duplicates per `customer_id`, keep only the latest record using `updated_at`.

### Sample schema
- `customer_id` (string)
- `name` (string)
- `city` (string)
- `updated_at` (timestamp)

### Expected PySpark solution
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())

latest_df = (
    df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
```

### What interviewer is evaluating
- Correct use of window functions.
- Deterministic dedup logic.
- Understanding of tie-breaking (if same timestamp).

---

## Test 2 — Incremental UPSERT into Delta table

### Problem
Load daily changes from `staging_orders` into `silver_orders` with upsert logic by `order_id`.

### Expected SQL (Delta MERGE)
```sql
MERGE INTO silver_orders AS tgt
USING staging_orders AS src
ON tgt.order_id = src.order_id
WHEN MATCHED AND src.is_deleted = true THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

### What interviewer is evaluating
- CDC handling (insert/update/delete).
- Proper merge key and deterministic behavior.
- Awareness of schema evolution constraints.

---

## Test 3 — Sessionization in PySpark

### Problem
For clickstream events (`user_id`, `event_ts`), start a new session when inactivity is > 30 minutes.

### Expected PySpark approach
```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("user_id").orderBy("event_ts")

events = (
    events
    .withColumn("prev_ts", F.lag("event_ts").over(w))
    .withColumn(
        "is_new_session",
        F.when(
            F.col("prev_ts").isNull() |
            (F.col("event_ts").cast("long") - F.col("prev_ts").cast("long") > 30 * 60),
            1
        ).otherwise(0)
    )
    .withColumn("session_num", F.sum("is_new_session").over(w))
)
```

### What interviewer is evaluating
- Correct ordered windowing.
- Time arithmetic correctness.
- Scalability thinking for very large user populations.

---

## 3) SQL Coding Tests (with expected answers)

## Test 1 — Top 3 salaries by department

### Problem
Return top 3 earners per department from `employees(emp_id, dept_id, salary)`.

### SQL answer
```sql
WITH ranked AS (
  SELECT
      emp_id,
      dept_id,
      salary,
      DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS rnk
  FROM employees
)
SELECT emp_id, dept_id, salary
FROM ranked
WHERE rnk <= 3;
```

---

## Test 2 — Monthly active customers

### Problem
From `orders(customer_id, order_ts)`, compute active customers by month.

### SQL answer
```sql
SELECT
    DATE_TRUNC('month', order_ts) AS order_month,
    COUNT(DISTINCT customer_id) AS active_customers
FROM orders
GROUP BY DATE_TRUNC('month', order_ts)
ORDER BY order_month;
```

---

## Test 3 — Detect duplicate transactions

### Problem
Find duplicates where same `account_id`, `amount`, and `txn_date` occur more than once.

### SQL answer
```sql
SELECT
    account_id,
    amount,
    txn_date,
    COUNT(*) AS cnt
FROM transactions
GROUP BY account_id, amount, txn_date
HAVING COUNT(*) > 1;
```

---

## 4) Hands-on Interview Scenario (Lead-level)

### Scenario prompt
Design an end-to-end pipeline for sales data arriving from multiple regions to Azure Data Lake, with near-real-time dashboards and historical accuracy.

### Strong answer should include
- Auto Loader ingestion pattern with schema evolution controls.
- Bronze/Silver/Gold layering with Delta standards.
- CDC merge strategy and late-arriving data handling.
- Data quality framework (nulls, uniqueness, referential checks).
- Orchestration, retries, alerting, SLA monitoring.
- Unity Catalog security model and PII controls.
- Cost/performance strategy (cluster policy, job clusters, optimization cadence).
- CI/CD and environment promotion approach.

---

## 5) Behavioral + Leadership Questions (with answer framing)

1. **Tell me about a major production incident you handled.**
   - Frame with STAR: impact, diagnosis approach, communication, permanent fix.

2. **How do you handle disagreements on architecture?**
   - Emphasize decision records, benchmark evidence, and time-boxed experiments.

3. **How do you raise team quality bar?**
   - Standards + templates + automated checks + mentoring loops.

4. **How do you balance speed vs reliability?**
   - Risk-tiering, MVP with guardrails, and iterative hardening.

---

## 6) 7-Day Preparation Plan

- **Day 1:** Delta Lake internals, MERGE, OPTIMIZE/ZORDER.
- **Day 2:** PySpark transformations + window functions.
- **Day 3:** Structured Streaming + Auto Loader patterns.
- **Day 4:** SQL analytics (CTEs, windows, ranking, dedup).
- **Day 5:** Azure integration + security/governance.
- **Day 6:** Mock architecture + leadership Q&A rehearsal.
- **Day 7:** Timed coding drills (PySpark + SQL) and review.

## 7) Quick Tips for the Interview

- Narrate trade-offs (cost, latency, quality, maintainability).
- State assumptions explicitly before coding.
- Write clean, testable transformations.
- Mention observability and rollback strategy proactively.
- For lead roles, emphasize decision-making and mentorship, not just coding.
