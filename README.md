# BikeCorp ETL Pipeline OOP

A modular ETL (Extract, Transform, Load) system for processing bicycle shop data from multiple sources into a unified database structure.

## Project Overview

This project is a continuation of the BikeCorp ETL project, which has been restructured after an OOP principle, to provide a flexible data pipeline that:

Extracts data from multiple sources (MySQL database, CSV files, and API)
Transforms the data through cleaning, validation, and standardization
Loads the transformed data into a target database (BikeCorpDB)

The system is designed with modularity in mind, allowing easy replacement of components without affecting the overall process.

## Project Structure

### Key Components

Extractor: Handles data extraction from various sources

MySQL database (ProductDB)
CSV files (staffs.csv, stores.csv)
API endpoints (customers, orders, order_items)


Transformer: Processes and standardizes the extracted data

Data type conversions
Reference data validation
Data cleaning and standardization


Loader: Loads transformed data into the target database

Handles database connections
Ensures proper data insertion



## Setup and Installation

Clone the repository
Install required dependencies:
pip install pandas mysql-connector-python requests fastapi uvicorn

Ensure your database credentials are stored in a cred_info.json file:
json{
  "host": "your_host",
  "user": "your_username",
  "password": "your_password"
}


## Usage
Running the ETL Pipeline
Execute the main script to run the complete ETL process:
python main.py
This will:

Initialize the Extractor, Transformer, and Loader classes
Process data from all configured sources
Transform the data according to predefined rules
Load the transformed data into the target database

API Data Source
To extract data from the API:

Start the API server (in a separate terminal):
fastapi run main.py

The ETL process will connect to the API endpoints at http://localhost:8000

## Data Sources

ProductDB Database: Contains brands, categories, products, and stocks data
CSV Files: Contains staffs and stores information
API: Provides customers, orders, and order_items data

## Modularity
The system is designed to be modular:

Each component (Extractor, Transformer, Loader) can be replaced independently
Alternative transformers can be created and easily swapped in
Additional data sources can be added with minimal changes

## Data Flow
Data Sources → Extractor → Transformer → Loader → Target Database
Each step communicates using standardized pandas DataFrames, ensuring compatibility across components.

## Requirements

Python 3.6+
pandas
mysql-connector-python
requests
FastAPI (for the API server)
uvicorn (for running the API server)
