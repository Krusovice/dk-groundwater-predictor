This repository contains ETL pipelines of climate, groundwater and soil profile data in preparation for a groundwater prediction model, based on climate data and surrounding soil layers.
The application is written in Azure Databricks and the formatting is using a medallion architecture.

Tech stack: Databricks, Python, PySpark, Delta Lake, Azure Data Factory (for automated ingestion).

The model is trained on historic climate data and groundwater levels, and the soil profiles at each groundwater measurement location.
Initially, the groundwater level model only takes in the climate data as a variable for predicting the groundwater level in each location.

Later, the soil profile is also implemented, to allow for groundwater level predictions based on soil profiles in the surroundings. However, there may be other significant factors relevant to the groundwater level, when it comes to evaluation on location. Therefore the expectation of the model prediction based on location is not high.

DMI Climate Data
Data is ingested weekly using DMI’s REST API.

Groundwater Data
Data ingested manually. Data from grundvandsstanden.dk

Soil Profile ingested manually. Data from geus.dk
