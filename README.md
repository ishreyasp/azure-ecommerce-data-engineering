# Azure Ecommerce Data Engineering

## Overview
This project showcases a robust data engineering pipeline using Azure services to process and transform data from the Brazilian E-commerce dataset. 
It demonstrates the use of Azure Data Factory, Databricks, Azure Data Lake Storage Gen 2, MongoDB, GitHub, MySQL, and Azure Synapse Analytics within a Medallion architecture.

## System Architecture
![alt text](system_architecture-1.png)

## Tools and Technologies
 - Azure Data Factory: Automates data ingestion from GitHub and MySQL.
 - Azure Data Lake Storage Gen 2: Serves as the central repository for raw (Bronze layer) and transformed (Silver layer) data.
 - Databricks: Handles data transformations to clean and process raw data.
 - MongoDB: Provides additional data for enrichment.
 - Azure Synapse Analytics: Used for final transformations and analytics, converting Silver layer data to Gold layer data.

## Pipeline Description
 - Data Ingestion: Data is fetched from GitHub via HTTPS and MySQL, then stored in the Bronze layer of Azure Data Lake Storage Gen 2 as raw data.
 - Data Transformation: Using Databricks, the raw data is cleaned and transformed, then stored back in the Data Lake as Silver layer data.
 - Data Enrichment: MongoDB data is used to enrich the data stored in the Bronze layer.
 - Data Analytics: Azure Synapse Analytics further transforms the data into the Gold layer and performs advanced analytics.

## Acknowledgments
 - Data sourced from the Brazilian E-commerce public dataset by olist.