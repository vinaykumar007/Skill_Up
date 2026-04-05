# Architecture Diagrams

## 1. Medallion Architecture Overview

```mermaid
graph TD
    subgraph Source["📥 Source Systems"]
        CSV["claims.csv<br/>customers.csv"]
    end
    
    subgraph Bronze["🥉 Bronze Layer<br/>Raw Ingestion"]
        B1["CSV Ingester<br/>+ Schema Validation"]
        B2["Metadata Injection<br/>_ingested_at<br/>_source_name<br/>_record_id"]
        B3["Delta Tables<br/>claims_raw<br/>customers_raw"]
    end
    
    subgraph Silver["🥈 Silver Layer<br/>Cleaned & Featured"]
        S1["Cleaning Rules<br/>Standardize:<br/>amounts, dates, status<br/>Mask PII: SSN, address"]
        S2["Feature Engineering<br/>resolution_days<br/>date features<br/>amount_category"]
        S3["Validation<br/>nulls, duplicates<br/>retention %"]
        S4["Deduplication<br/>remove exact<br/>duplicates"]
        S5["Parquet Tables<br/>claims_cleaned<br/>customers_enhanced"]
    end
    
    subgraph Gold["🥇 Gold Layer<br/>Business Analytics"]
        G1["monthly_summary<br/>35 rows"]
        G2["status_breakdown<br/>4 status values"]
        G3["claim_type_analysis<br/>5 claim types"]
        G4["high_value_claims<br/>amount &gt; $20K"]
        G5["resolution_time_analysis<br/>6 time buckets"]
        G6["fraud_risk_features<br/>1000 rows"]
    end
    
    subgraph Tracking["📊 Tracking"]
        MLF["MLflow<br/>23+ metrics<br/>reproducible runs"]
    end
    
    CSV --> B1 --> B2 --> B3
    B3 --> S1 --> S2 --> S3 --> S4 --> S5
    S5 --> G1 & G2 & G3 & G4 & G5 & G6
    B1 -.-> MLF
    S1 -.-> MLF
    G1 -.-> MLF
    
    style Source fill:#e1f5ff
    style Bronze fill:#fff3e0
    style Silver fill:#f3e5f5
    style Gold fill:#e8f5e9
    style Tracking fill:#fce4ec
```

---

## 2. Data Quality Gates

```mermaid
graph LR
    subgraph Input["Raw Data"]
        I["1,000 records<br/>from source"]
    end
    
    subgraph BronzeGate["🔒 Bronze Gate<br/>Schema Validation"]
        B["✓ Type checking<br/>✓ Field validation<br/>✓ Metadata injection<br/>→ 1,000 records"]
    end
    
    subgraph SilverGate["🔒 Silver Gate<br/>Cleaning Rules"]
        S["✓ Standardize amounts<br/>✓ Parse dates<br/>✓ PII masking<br/>✓ Feature engineering<br/>→ 995 records (99.5%)"]
    end
    
    subgraph QualityCheck["🔒 Quality Checks"]
        Q["✓ Null detection<br/>✓ Duplicate removal<br/>✓ Retention %<br/>✓ Value distribution"]
    end
    
    subgraph Output["Analytics Ready"]
        O["Gold metrics<br/>6 tables"]
    end
    
    I --> BronzeGate --> SilverGate --> QualityCheck --> Output
    
    style Input fill:#ffebee
    style BronzeGate fill:#fff3e0
    style SilverGate fill:#f3e5f5
    style QualityCheck fill:#e0f2f1
    style Output fill:#e8f5e9
```

---

## 3. Code Structure & Dependencies

```mermaid
graph TB
    subgraph ETL["ETL Pipeline Orchestration"]
        EP["ETLPipeline<br/>def execute"]
    end
    
    subgraph Bronze["Bronze Layer"]
        BI["CSVIngester<br/>read_csv<br/>apply_schema"]
        SD["SchemaDefinitions<br/>get_claims_schema<br/>get_customers_schema"]
    end
    
    subgraph Silver["Silver Layer"]
        CR["ClaimCleaningRules<br/>standardize_amount<br/>standardize_date<br/>standardize_status<br/>mask_ssn<br/>mask_address"]
        FE["FeatureEngineering<br/>calculate_resolution_days<br/>extract_date_features"]
        DD["Deduplicator<br/>remove_duplicates<br/>detect_duplicates"]
        DV["DataValidator<br/>check_row_count<br/>check_nulls<br/>check_duplicates"]
        CT["ClaimTransformer<br/>transform<br/>orchestrates silver"]
    end
    
    subgraph Gold["Gold Layer"]
        CM["ClaimMetrics<br/>generate_monthly_summary<br/>generate_status_breakdown<br/>generate_claim_type_analysis<br/>generate_high_value_claims<br/>generate_resolution_time_analysis<br/>generate_fraud_risk_features"]
    end
    
    subgraph Utils["Utilities"]
        SM["SparkManager<br/>get_spark_session"]
        MLF["MLflowTracker<br/>setup<br/>start_run<br/>log_phase_metrics<br/>log_metric"]
        LOG["Logger<br/>log_info<br/>log_error"]
        CONF["Config<br/>data_locations<br/>mlflow_uri"]
    end
    
    EP --> BI --> SD
    EP --> CT
    CT --> CR & FE & DD & DV
    EP --> CM
    EP --> MLF & SM & LOG
    CONF -.-> EP
    
    style ETL fill:#e3f2fd
    style Bronze fill:#fff3e0
    style Silver fill:#f3e5f5
    style Gold fill:#e8f5e9
    style Utils fill:#f5f5f5
```

---

## 4. Data Transformation Pipeline

```mermaid
graph TD
    subgraph Bronze["BRONZE - Raw Ingestion"]
        B1["Input CSV<br/>claims.csv<br/>1,000 rows"]
        B2["Schema<br/>Applied"]
        B3["Metadata<br/>Added"]
        B4["Delta Table<br/>claims_raw_v1"]
    end
    
    subgraph Silver["SILVER - Data Cleaning"]
        S1["Clean:<br/>amounts, dates, status"]
        S2["Feature:<br/>resolution_days<br/>date features<br/>categories"]
        S3["Validate:<br/>nulls, duplicates<br/>retention"]
        S4["Dedupe:<br/>exact matches<br/>Log removals"]
        S5["Parquet<br/>claims_cleaned"]
    end
    
    subgraph Gold["GOLD - Analytics"]
        G1["Aggregation 1:<br/>monthly_summary<br/>GROUP BY month"]
        G2["Aggregation 2:<br/>status_breakdown<br/>GROUP BY status"]
        G3["Aggregation 3+:<br/>5 more tables"]
        G4["Parquet Tables<br/>metrics/*"]
    end
    
    B1 --> B2 --> B3 --> B4
    B4 --> S1 --> S2 --> S3 --> S4 --> S5
    S5 --> G1 & G2 & G3 --> G4
    
    style Bronze fill:#fff3e0
    style Silver fill:#f3e5f5
    style Gold fill:#e8f5e9
```

---

## 5. Test Structure

```mermaid
graph TB
    subgraph TestSuite["63 Comprehensive Tests"]
        subgraph Unit["Unit Tests (50)"]
            B["Bronze Tests (11)<br/>├─ Schema validation (3)<br/>├─ CSV ingestion (3)<br/>├─ Metadata (2)<br/>└─ Data validation (3)"]
            S["Silver Tests (18)<br/>├─ Cleaning rules (6)<br/>├─ Validation (4)<br/>├─ Features (3)<br/>├─ Deduplication (2)<br/>└─ Transformer (3)"]
            G["Gold Tests (20)<br/>├─ Basics (3)<br/>├─ Aggregations (3)<br/>├─ Correctness (4)<br/>├─ Table generation (3)<br/>└─ Fraud features (2)"]
        end
        
        subgraph Integration["Integration Tests (14)"]
            I1["Bronze→Silver flow"]
            I2["Silver→Gold aggregation"]
            I3["MLflow tracking"]
            I4["Error handling"]
            I5["Output generation"]
            I6["Data integrity"]
            I7["End-to-end pipeline"]
        end
        
        subgraph Fixtures["Shared Fixtures"]
            F1["sample_claims_data<br/>5 records"]
            F2["sample_large_claims_data<br/>1000 records"]
            F3["config<br/>test configuration"]
            F4["mlflow_tracker<br/>test tracking"]
        end
    end
    
    B --> F1 & F2
    S --> F1 & F2
    G --> F1 & F2
    I1 & I2 & I3 & I4 & I5 & I6 & I7 --> Fixtures
    
    style TestSuite fill:#f3e5f5
    style Unit fill:#e1bee7
    style Integration fill:#ce93d8
    style Fixtures fill:#f8bbd0
```

---

## 6. MLflow Experiment Tracking

```mermaid
graph LR
    subgraph Run["MLflow Run"]
        RID["Run ID<br/>uuid"]
        Tags["Tags<br/>phase: silver<br/>environment: test"]
        Params["Parameters<br/>cleaning_rules_v1.2<br/>schema_v3"]
    end
    
    subgraph Metrics["Metrics Logged"]
        M1["Bronze Phase<br/>├─ rows_ingested: 1000<br/>├─ nulls_detected: 12<br/>└─ duration_sec: 0.5"]
        M2["Silver Phase<br/>├─ records_validated: 1000<br/>├─ duplicates_removed: 5<br/>├─ features_created: 4<br/>└─ duration_sec: 1.2"]
        M3["Gold Phase<br/>├─ monthly_rows: 35<br/>├─ status_rows: 4<br/>└─ duration_sec: 0.8"]
    end
    
    subgraph Artifacts["Artifacts"]
        A1["config.yaml<br/>transformation_log.txt<br/>quality_report.json"]
    end
    
    subgraph Comparison["Historical Comparison"]
        H["Run 2026-03-02: 23 metrics<br/>Run 2026-03-01: 23 metrics<br/>Δ duplicates: +3 (alert!)"]
    end
    
    Run --> Metrics
    Metrics --> Artifacts
    Artifacts --> Comparison
    
    style Run fill:#fce4ec
    style Metrics fill:#f8bbd0
    style Artifacts fill:#f48fb1
    style Comparison fill:#ec407a
```

---

## 7. Error Handling & Resilience

```mermaid
graph TD
    subgraph Normal["Happy Path"]
        N1["Read Bronze"] --> N2["Clean Silver"] --> N3["Aggregate Gold"]
    end
    
    subgraph ErrorHandling["Error Detection"]
        E1["Schema Mismatch<br/>→ Fail at Bronze"] --> E1A["Log error to MLflow<br/>Stop pipeline"]
        E2["Null percentage<br/>&gt; threshold<br/>→ Alert in Silver"] --> E2A["Log warning<br/>Continue (investigate)"]
        E3["Aggregation error<br/>→ Fail at Gold"] --> E3A["Log error<br/>Roll back Gold"]
    end
    
    subgraph Logging["Audit Trail"]
        L["MLflow Run ID<br/>Timestamp<br/>Phase<br/>Error message<br/>Stack trace"]
    end
    
    N1 & N2 & N3 --> L
    E1A & E2A & E3A --> L
    
    style Normal fill:#c8e6c9
    style ErrorHandling fill:#ffccbc
    style Logging fill:#bbdefb
```

---

## 8. Data Lineage (Claim Journey)

```mermaid
graph LR
    subgraph Source["📋 Source"]
        S["Claim submitted<br/>Date: 2024-01-15<br/>Amount: $1,250"]
    end
    
    subgraph Bronze["🥉 Bronze<br/>_ingested_at: 2026-03-02 10:00<br/>_source_name: claims_system<br/>_record_id: REC-789123"]
        B["claim_id: C001<br/>claim_amount: 1250<br/>submitted_date: 2024-01-15<br/>status: pending"]
    end
    
    subgraph Silver["🥈 Silver<br/>transformation_id: TXN-456<br/>_transformed_at: 2026-03-02 10:01"]
        SL["claim_amount: 1250.0 (standardized)<br/>amount_category: LOW<br/>status: SUBMITTED (normalized)<br/>resolution_days: 45<br/>submitted_quarter: Q1"]
    end
    
    subgraph Gold["🥇 Gold<br/>aggregation_id: AGG-123"]
        G["monthly_summary: Jan = 1<br/>status_breakdown: SUBMITTED += 1<br/>resolution_time_analysis: 31-60d += 1"]
    end
    
    subgraph MLflow["📊 MLflow Run<br/>run_id: abc-def-123<br/>14:30 UTC"]
        MF["Run tags<br/>Run metrics (23)<br/>Run artifacts"]
    end
    
    S --> Bronze
    Bronze --> Silver
    Silver --> Gold
    Bronze -.-> MLflow
    Silver -.-> MLflow
    Gold -.-> MLflow
    
    style Source fill:#ffebee
    style Bronze fill:#fff3e0
    style Silver fill:#f3e5f5
    style Gold fill:#e8f5e9
    style MLflow fill:#fce4ec
```

---

## 9. Deployment Architecture (Future)

```mermaid
graph TB
    subgraph Schedule["⏰ Orchestration<br/>Apache Airflow"]
        DAG["Daily DAG<br/>trigger_date=yesterday"]
    end
    
    subgraph Compute["🖥️ Processing<br/>Spark Cluster"]
        Driver["Spark Driver<br/>→ Coordinates"]
        Workers["Spark Workers (3+)<br/>→ Parallelize<br/>→ Cache"]
    end
    
    subgraph Storage["💾 Storage<br/>Delta Lake"]
        Bronze_DB["Bronze<br/>Delta Tables"]
        Silver_DB["Silver<br/>Parquet Files"]
        Gold_DB["Gold<br/>Parquet Files"]
    end
    
    subgraph Catalog["🔍 Discovery<br/>Data Catalog"]
        Register["Table Registration<br/>Schema documentation<br/>Lineage tracking"]
    end
    
    subgraph Analytics["📊 Analytics<br/>BI Tools"]
        Tableau["Tableau<br/>SQL queries on Gold"]
        PowerBI["Power BI<br/>Direct SQL"]
    end
    
    subgraph Monitoring["📈 Monitoring"]
        MLF["MLflow<br/>Metric tracking"]
        Alert["Alerts<br/>null%, retention%"]
    end
    
    DAG --> Driver --> Workers
    Workers --> Bronze_DB --> Silver_DB --> Gold_DB
    Gold_DB --> Catalog
    Catalog --> Analytics
    Bronze_DB & Silver_DB & Gold_DB --> MLF --> Alert
    
    style Schedule fill:#e3f2fd
    style Compute fill:#fff3e0
    style Storage fill:#f3e5f5
    style Catalog fill:#e0f2f1
    style Analytics fill:#e8f5e9
    style Monitoring fill:#fce4ec
```

---

## 10. Performance Characteristics

```mermaid
graph LR
    subgraph DataSize["Data Volume"]
        S1["1K records<br/>2 seconds"]
        S2["10K records<br/>5 seconds"]
        S3["100K records<br/>15 seconds"]
    end
    
    subgraph PhaseBreakdown["Phase Timing (1K records)"]
        P1["Bronze Load<br/>0.5s"]
        P2["Silver Transform<br/>1.2s"]
        P3["Gold Aggregate<br/>0.3s"]
    end
    
    subgraph Bottleneck["Bottleneck Analysis"]
        B["Silver layer<br/>Most time (cleaning)"]
    end
    
    subgraph Optimization["Optimization Opportunities"]
        O1["Caching intermediates"]
        O2["Partitioning by date"]
        O3["Parallel aggregations"]
    end
    
    S1 --> P1 --> B
    S2 --> P2 --> O1 & O2 & O3
    S3 --> P3
    
    style DataSize fill:#e1f5ff
    style PhaseBreakdown fill:#fff3e0
    style Bottleneck fill:#ffccbc
    style Optimization fill:#c8e6c9
```

---

These diagrams illustrate:
1. **Level 1:** High-level medallion architecture
2. **Level 2:** Quality gates ensuring data integrity
3. **Level 3:** Code structure and dependencies
4. **Level 4:** Data transformation flow
5. **Level 5:** Test coverage strategy
6. **Level 6:** MLflow tracking for reproducibility
7. **Level 7:** Error handling and resilience
8. **Level 8:** Complete data lineage journey
9. **Level 9:** Future deployment architecture
10. **Level 10:** Performance characteristics

**Documentation note:** Walk through diagrams 1–5 in order, use 6–8 to discuss observability, and reference 9–10 when asked about scaling/production.
