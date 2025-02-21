# Lakehouse for TMDB movie analystics with Azure and Delta Lake 🎬
personal project developing a three stage data lakehouse

📊 Project Status: In Development

## Project Overview

### Motivation

Modern analytics needs have often grown beyond only relying on structured data in old-fashioned data warehouses. This trend towards hetergeneous data sources, with a mix of structured, semi-structured and unstructured data, has facilitated the rise of data lakes. However, rising data volumes have made the weakness of data lakes with regards to missing structure and weak performance in comparison to data warehouses apparent. Modern Cloud Lakehouses aim to integrate the advantages of both approaches and bring the structure and perfomance of a data warehouse to the flexibility and analytical power of a data lake. 

### Goal of this project
### Business Constraints
 + must handle semi-structured data like JSON
 + cost must stay managable with a azure students subscription

## Core features
- **Medaillion-Architecture**
- **SCD2-Historization**
- **Dimensional Data Modeling**
- **Data Ingestion**´
- **Data Processing**
- **Analytics**´
- **Orchestration/CICD**


## Tools and technology

## Architecture
## Project Structure
## Project Plan
### Goal
 Build a POC
### Scope
4 weeks development time
### Delievarables

| Milestone | Description | Week |
|----------|----------|----------|
| Bronce Layer    | Ingestion of raw JSON from API into Azure Blob Storage  | Week 1  |
| Silver Layer   | Cleaned and flattened JSON data in Parquet data with SCD2  | Week 2 |
| Gold Layer   | Delta Lake Tables with star schema model and Synapse Queries/ Visualization| Week 3 +4|
| Documentation   | Readme | Week 1/4 |

## Setup and Deployment
## License
