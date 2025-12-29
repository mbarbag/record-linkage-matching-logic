# Record Linkage & Matching Logic

**Methodology & Assumptions**

## 1. Purpose and Scope

This repository contains a Python code implementing a **deterministic record-linkage pipeline** across three independent data sources:
- TLD Report
- Sherpa Report
- Carrier Report

The objective is to enrich Carrier records with the best-available information from TLD and Sherpa in the absence of a shared universal primary key.

This document explains:

- The tools and environment used
- The step-by-step matching logic (TLD → Sherpa → Carrier)
- Normalization rules
- Conflict-handling strategies
- Known limitations and proposed next steps

## 2. Tools & Environment

- **Language / Environment**: Python

- **Primary Libraries**:
    - `pandas` — data ingestion, cleaning, joins, and transformations
    - `numpy` — conditional assignments (np.where) and masking logic

## 3. Matching Pipeline (High Level)

The overall workflow follows a progressive, signal-weakening strategy:
```
TLD Report
   ↓ clean & deduplicate
Sherpa Report
   ↓ normalize
Carrier Report
   ↓ clean
TLD + Sherpa
   ↓ conditional enrichment
TLD-Sherpa dataset
   ↓
Carrier + TLD-Sherpa
   ↓
Final matched dataset
```

Matching occurs in **two major phases**, progressively relaxing the matching criteria.

## 4. Step-by-Step Matching Logic

### 4.1 TLD → Sherpa (Enrichment Phase)

A working copy of TLD (`tld_sherpa`) is created and enriched with Sherpa values.

Three match conditions (boolean masks) are computed:

1. Application ID + Phone match
    - `ffm_app_id == application_number`
    - `phone == lead_phone`

    **→ Strongest signal**
2. Application ID match only
    - `ffm_app_id == application_number`
3. Phone match only
    - Normalized phone equality

    **→ Weakest signal**

For each Sherpa column, a **nested conditional assignment** is applied:
```
If (Application ID + Phone match) → use Sherpa value
Else if (Application ID match)    → use Sherpa value
Else if (Phone match)             → use Sherpa value
Else                              → keep existing TLD value
```

This guarantees that **only the highest-priority successful match** enriches each field.

### 4.2 TLD-Sherpa → Carrier (Final Join)

Carrier records are left-merged with the enriched TLD-Sherpa dataset:

- **Carrier keys**: `['FullName', 'Phone']`

- **TLD-Sherpa keys**: `['full_name', 'phone']`

This produces a final dataset where:

- All Carrier rows are preserved

- TLD/Sherpa data is appended where a match exists

## 5. Normalization & Transformations
**Phone Numbers**

- Cast using: `astype('Int64').astype('string')`

- Ensures:
    - No punctuation

    - Digit-only format

    - Cross-dataset comparability

**Names**

- All name fields are uppercased prior to matching

**Policy IDs**

- Sherpa policy IDs: cast to string only (no aggressive cleaning)

- Carrier policy IDs: treated as strings

**Completeness Scoring**

An `info_count` column is computed in the Sherpa dataset:

- Counts the number of non-null fields per row

- Used to prefer more complete Sherpa records in ambiguous match scenarios

## 6. Conflict Handling Strategy
**Multiple Sherpa Matches**

- Sherpa records are sorted by info_count

- Rows with more populated fields take precedence

- This provides an implicit, deterministic conflict-resolution mechanism

**No Match Found**

- **TLD rows** remain intact; Sherpa fields are `NULL`

- **Carrier rows** remain intact; TLD-Sherpa fields are `NULL`

This ensures **no data loss** and preserves unmatched records for downstream analysis.

## 7. Known Limitations

**Deterministic equality-based matching**

- Highly auditable and reproducible

- Recall could be improved with bounded fuzzy matching for noisy data

**Limited provenance tracking**

- While match priority is enforced, individual fields are not explicitly tagged with their match source

- Adding provenance flags would improve auditability and debugging

## 8. Closing Notes

No single primary key exists across TLD, Sherpa, and Carrier datasets.

This solution addresses that constraint by applying a **structured, deterministic hierarchy**:

1. Application ID (when present)

2. Exact name match

3. Normalized phone match

The result is a **reproducible, auditable, and extensible matching pipeline** that provides clear levers for future improvement, including:

- Provenance tagging

- Canonical phone handling

- Controlled fuzzy matching layers

