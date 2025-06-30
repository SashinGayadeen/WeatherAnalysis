# Weather Analysis
📑 Data Pipeline Architecture: REST API to Power BI Report (Microsoft Fabric)
🔷 1️⃣ Overview
This data pipeline outlines how data is ingested from an external REST API, transformed and prepared using Microsoft Fabric Notebooks, stored in a Lakehous or Warehouse and finally visualized in a Power BI report.


🔷 2️⃣ High-Level Architecture
           
  REST API      --->   Fabric Notebook    --->  Lakehouse/Table  --->   Power BI     
 (Source Data)       (Extract & Transform)       (Load)               (Reports & Viz)
  

🔷 3️⃣ Detailed Process
✅ Step 1: Data Ingestion 
Purpose:
Securely access and retrieve raw data from an external REST API source.

Process:

Establish a secure connection to the REST API using API Key

Store the raw JSON response in the Lakehouse’s Raw zone for traceability and further processing.


✅ Step 2: Data Extraction
Purpose:
Load the ingested raw data into a processing layer for transformation.

Process:

Open the stored raw JSON files using Fabric Notebooks.

Identify the relevant parts of the nested JSON to be extracted (e.g., arrays, nested objects).

Prepare the data for flattening.

✅ Step 3: Data Transformation
Purpose:
Convert complex, nested JSON structures into clean, analysis-ready tabular formats.

Process:

Use the Notebook to flatten arrays and nested fields.

Filter, cleanse, and enrich the data as needed.


✅ Step 4: Data Load
Purpose:
Store the curated, flattened dataset in a structured, performant storage for reporting.

Process:

Save the final dataset to a Lakehouse (Delta tables) or Warehouse inside Fabric.

✅ Step 5: Power BI Report
Purpose:
Connect the clean, loaded data to Power BI for business intelligence and decision-making.

Process:

Create a connection from Power BI to the Fabric Lakehouse or Warehouse.



**Output:
This architecture ensures your REST API data is transformed into actionable insights in Power BI, following best practices for modern data engineering in Microsoft Fabric.**


















