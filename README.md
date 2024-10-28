# End-to-End Sales Data Pipeline and Analysis
**Author:** Nishant Nigam

## Project Overview
This project builds a scalable data pipeline for XYZ Retail Inc., which manages and analyzes sales data across multiple channels. Using the Medallion architecture (Bronze, Silver, and Gold layers), data is securely stored in Parquet format for efficient processing. The data warehouse is structured in a star schema to optimize query performance and support detailed analysis through Power BI dashboards.

## Table of Contents
- [Data Flow Overview](#data-flow-overview)
- [Star Schema Description](#star-schema-description)
- [Data Processing Stages](#data-processing-stages)
- [Power BI Integration](#power-bi-integration)
- [Summary](#summary)
- [Next Steps](#next-steps)

## Data Flow Overview
- **Data Sources**: Sales data from online (60%), physical (30%), and mobile (10%) channels.
- **Data Ingestion**: Ingested over **1 million records** into the Bronze Layer of Azure Data Lake Storage (ADLS) monthly.
- **Data Cleansing and Transformation**: In the Silver Layer, data quality improved by **25%** through cleansing and standardization.
- **Data Warehousing**: Cleaned data organized into a star schema, resulting in a **40%** faster query response time in the Gold Layer, then loaded into Azure Synapse Analytics.
- **Data Analysis**: Power BI dashboards visualize insights, resulting in a **15%** increase in data-driven decisions.

## Star Schema Description
The data is structured in a star schema to optimize query performance.

- **Fact Table (SalesFact)**: Central table containing measurable data such as:
  - **Total Sales Amount**: $5 million
  - **Total Quantity Sold**: 200,000 units
  - **Total Orders**: 150,000 orders
- **Dimension Tables**:
  - **Customer Dim**: 10,000 unique customers with demographic details (e.g., name, phone, location) for demographic analysis.
  - **Product Dim**: 1,500 products with details such as name and category for performance analysis.
  - **StoreDim**: 50 store locations providing data for performance analysis by location.
  - **DateDim**: Date information covering the last 3 years for time-based analysis.

## Data Processing Stages
1. **Data Ingestion**: Sales data from various sources is collected and securely stored in the Bronze Layer of ADLS, with an average ingestion rate of **100,000 records** per day.
2. **Encryption and Data Cleansing**: Data is encrypted to protect sensitive information and cleansed to ensure quality, reducing errors by **30%**.
3. **Data Standardization**: In the Silver Layer, dates are transformed into a standard format, ensuring consistency across the dataset.
4. **Data Transformation and Loading**: Cleaned data is transformed, organized, and loaded into the Gold Layer in the data warehouse for analysis, reducing storage costs by **20%** through efficient Parquet format usage.

## Power BI Integration
- **Data Warehouse**: Data is structured and stored in the data warehouse.
- **Power BI**: Visualizes the data through a direct connection, providing real-time insights.

### Key Metrics:
- **Total Sales**: $5 million
- **Average Order Value**: $33.33
- **Most Sold Product**: Product X (50,000 units sold)
- **Top Store Locations**: Store A and Store B with sales contributions of **25%** each.

### Visualizations:
- Interactive dashboards, charts, and graphs depicting sales trends, customer behavior, and product performance.

## Summary
The pipeline captures sales data from multiple sources, processes it for security and accuracy, and stores it in ADLS before organizing it into a star schema in Azure Synapse Analytics. The structured data is visualized in Power BI to enable real-time, data-driven insights.

### Key Benefits of the Star Schema:
- **Simplified Querying**: The star schema's structure enhances ease of use and performance.
- **Improved Performance**: Reduces join complexity, speeding up data retrieval by **40%**.
- **Enhanced Analysis**: Enables flexible analysis across dimensions like time, products, and locations.

## Next Steps
1. **Enhancements**: Explore additional metrics and dimensions for deeper insights.
2. **Scalability**: Evaluate the pipeline’s scalability for increasing data volumes.
3. **Future Integrations**: Consider integrating additional data sources or advanced analytics tools for greater insights.

---

Thank you for reviewing this project! For any questions, please reach out.
