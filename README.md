# data_engineering_project

Project Title: End-to-End Data Pipeline & Warehouse for Fintech Loan Portfolio Analytics
Description:
Engineered a scalable ETL pipeline to process and analyze a multi-entity loan application dataset. The project encompassed the full lifecycle of data engineering, from exploratory analysis and feature creation in Python (Pandas) to containerized deployment and warehousing in PostgreSQL.
Key Contributions & Technical Stack:
Data Wrangling (Python/Pandas): Developed modular functions for data standardization, statistical imputation of missing values (MCAR/MAR analysis), and outlier handling. Designed a financial feature engineering module to calculate monthly installments and affordability metrics.
Data Lineage & Governance: Implemented a programmatic lookup table generation system to document all categorical encodings and imputations, ensuring data reproducibility and transparency.
Containerization & Orchestration (Docker/Bash): Refactored notebook code into production Python scripts and containerized the application using python:3.11. Orchestrated a multi-service architecture with Docker Compose to network the Python ETL service with a postgres:13 database instance.
Data Warehousing (PostgreSQL/SQL): Managed volume mounts for persistent database storage and developed complex SQL queries for state-level financial analysis (avg. interest rates, default likelihood).
Big Data Processing (PySpark): Migrated the pipeline to PySpark for distributed data processing of Parquet files. Utilized Spark Window functions for advanced time-series feature creation (calculating previous loan sequences per customer grade). Performed comparative analytics using both PySpark DataFrame API and Spark SQL.
Outcome: Delivered a containerized, fully automated data pipeline that ingests raw financial data, cleanses and enriches it, and loads it into a query-ready analytical database—demonstrating core competencies in Python, SQL, Docker, and Distributed Computing.


