# Druid Indexing Task Template Documentation

This document explains the structure and purpose of the Druid indexing task template, detailing each section of the configuration.

---

## Purpose
The provided JSON template is designed to define and submit a parallel indexing task to a Druid cluster. It processes data from an external source and creates optimized segments in a Druid data source.

---

## Template Structure
The template is divided into the following sections:

### 1. `type`
Specifies the task type. In this case:
```json
"type": "index_parallel"
```
This indicates the task will perform parallel indexing to ingest data.

---

### 2. `resource`
Defines resource constraints and grouping for the task.
- **`availabilityGroup`**: A unique identifier for task grouping.
- **`requiredCapacity`**: Number of worker slots required.

Example:
```json
"resource": {
  "availabilityGroup": "wikipedia-index-task",
  "requiredCapacity": 1
}
```

---

### 3. `spec`
Contains the main configuration for the task, including data schema, input source, and tuning parameters.

#### a. `dataSchema`
Defines the structure of the data and how it is processed.
- **`dataSource`**: Name of the Druid data source to populate.
- **`timestampSpec`**: Specifies the timestamp column and format.
- **`dimensionsSpec`**: Lists the dimensions to be indexed and their properties.
  - `dimensions`: Defines columns and their types (e.g., `string`, `long`).
  - `dimensionExclusions`: Specifies columns to exclude.
  - `includeAllDimensions`: If `true`, includes all dimensions not explicitly defined.

#### Example:
```json
"dataSchema": {
  "dataSource": "wikipedia",
  "timestampSpec": {
    "column": "timestamp",
    "format": "iso"
  },
  "dimensionsSpec": {
    "dimensions": [
      {"type": "string", "name": "isRobot"},
      {"type": "long", "name": "added"}
    ],
    "dimensionExclusions": ["__time", "timestamp"],
    "includeAllDimensions": false
  }
}
```

#### b. `granularitySpec`
Specifies data granularity and segmentation settings.
- **`segmentGranularity`**: Granularity of segments (e.g., `DAY`, `HOUR`).
- **`queryGranularity`**: Defines how data is grouped during queries.
- **`rollup`**: If `true`, aggregates data during ingestion.

#### c. `transformSpec`
Defines transformations and filters to apply during ingestion.

---

#### d. `ioConfig`
Configures the input source and format.
- **`inputSource`**: Specifies where the data is coming from (e.g., HTTP, local files).
  - Example:
    ```json
    "inputSource": {
      "type": "http",
      "uris": ["https://druid.apache.org/data/wikipedia.json.gz"]
    }
    ```
- **`inputFormat`**: Defines the data format (e.g., JSON, CSV).

---

#### e. `tuningConfig`
Fine-tunes task performance and memory usage.
- **`maxRowsPerSegment`**: Maximum number of rows per segment.
- **`maxRowsInMemory`**: Maximum rows stored in memory during ingestion.
- **`indexSpec`**: Compression settings for segments.

Example:
```json
"tuningConfig": {
  "type": "index_parallel",
  "maxRowsPerSegment": 5000000,
  "maxRowsInMemory": 1000000
}
```

---

### 4. `context`
Defines task-specific context settings.
- **`forceTimeChunkLock`**: Ensures the task locks data intervals.
- **`useLineageBasedSegmentAllocation`**: Enables lineage-based segment allocation.

Example:
```json
"context": {
  "forceTimeChunkLock": true,
  "useLineageBasedSegmentAllocation": true
}
```

---

### 5. `dataSource`
Specifies the name of the target Druid data source.

Example:
```json
"dataSource": "wikipedia"
```

---

## Full Example
Below is a complete example of a Druid indexing task template:

```json
{
  "type": "index_parallel",
  "resource": {
    "availabilityGroup": "wikipedia-index-task",
    "requiredCapacity": 1
  },
  "spec": {
    "dataSchema": {
      "dataSource": "wikipedia",
      "timestampSpec": {
        "column": "timestamp",
        "format": "iso"
      },
      "dimensionsSpec": {
        "dimensions": [
          {"type": "string", "name": "isRobot"},
          {"type": "string", "name": "channel"},
          {"type": "string", "name": "page"},
          {"type": "string", "name": "user"},
          {"type": "long", "name": "added"},
          {"type": "long", "name": "deleted"}
        ],
        "dimensionExclusions": ["__time", "timestamp"],
        "includeAllDimensions": false
      },
      "granularitySpec": {
        "type": "uniform",
        "segmentGranularity": "DAY",
        "queryGranularity": {
          "type": "none"
        },
        "rollup": false
      },
      "transformSpec": {
        "filter": null,
        "transforms": []
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "http",
        "uris": ["https://druid.apache.org/data/wikipedia.json.gz"]
      },
      "inputFormat": {
        "type": "json",
        "keepNullColumns": false,
        "assumeNewlineDelimited": false,
        "useJsonNodeReader": false
      },
      "appendToExisting": false
    },
    "tuningConfig": {
      "type": "index_parallel",
      "maxRowsPerSegment": 5000000,
      "maxRowsInMemory": 1000000,
      "indexSpec": {
        "bitmap": {
          "type": "roaring"
        },
        "dimensionCompression": "lz4",
        "metricCompression": "lz4",
        "longEncoding": "longs"
      }
    }
  },
  "context": {
    "forceTimeChunkLock": true,
    "useLineageBasedSegmentAllocation": true
  },
  "dataSource": "wikipedia"
}
```

---

## Example Use Case
This template is configured to ingest Wikipedia edits data from an external JSON file. It indexes various dimensions, such as `isRobot`, `channel`, `page`, and `user`, while applying specific compression and granularity settings.

---

## Steps to Use
1. Copy the template JSON.
2. Modify necessary fields, such as `dataSource`, `inputSource`, and `dimensions`.
3. Submit the task to the Druid Operator:
   ```code
    ingest_data = DruidOperator(
        task_id='druid_ingest',
        json_index_file='wikipedia-index.json', //index file
        druid_ingest_conn_id='druid_default',
    )
   ```
4. Monitor the task status in the Druid console.

---

## Troubleshooting
- **Error:** Missing `availabilityGroup`.
  - **Solution:** Add an `availabilityGroup` field in the `resource` section.

- **Error:** Task fails with input format issues.
  - **Solution:** Verify the `inputFormat` matches the data structure.

- **Error:** Task hangs during ingestion.
  - **Solution:** Adjust `tuningConfig` parameters, such as `maxRowsInMemory` or `maxPendingPersists`.
---

## 🔙 [Back to Homepage](README.md)

