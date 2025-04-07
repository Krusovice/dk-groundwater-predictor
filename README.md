<h1>DK Groundwater Predictor</h1>

This repository contains material for building a machine learning model that predicts groundwater levels based on climate data.

It includes data extraction, cleaning, feature engineering and model training. Implemented in Databricks using a medallion architecture.

It is currently being developed on Azure and therefore requires additional setup for running on a local environment.

<h3>Contents</h3>
<ul>
  <li><strong>Climate data ETL:</strong> Automatically ingested daily via DMI's REST API.</li>
  <li><strong>Groundwater data ETL:</strong> Manually extracted from grundvandsstanden.dk (GEUS).</li>
  <li><strong>Groundwater prediction model:</strong> In development — Applying Linear Regression to predict groundwater levels from climate features.</li>
</ul>

<h3>Tech Stack</h3>
<ul>
  <li>Python</li>
  <li>PySpark</li>
  <li>Azure Databricks</li>
  <li>Delta Lake</li>
  <li>PySpark MLlib</li>
</ul>

<h3>Future Ideas</h3>
<ul>
  <li>Make the project easily runnable by adding dependency checks and automated setup for local environments.</li>
  <li>Incorporate soil profile data to improve model accuracy.</li>
  <li>Explore more advanced ML models.</li>
</ul>
