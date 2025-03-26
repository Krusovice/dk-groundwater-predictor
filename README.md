This repository contains an ETL pipeline for ingesting and processing climate data relevant to groundwater level predictions.

Data is extracted from DMI’s REST API, cleaned, and stored in Azure Data Lake in Delta format using a medallion architecture.

Tech stack: Databricks, Python, PySpark, Delta Lake, Azure Data Factory (for automated ingestion).