# SpotifyTracks-Data-Warehousing-Analysis-and-Popularity-Prediction-Model-
![Spotify Analysis](https://github.com/user-attachments/assets/73bc09c1-3256-4f9b-81a1-87a43e0c92d8)
# Detailed Report: Azure Spotify ML Pipeline and Data Warehousing Project

## **Overview**

This project involves building a robust end-to-end data pipeline to process Spotify track data from 2017-2021, which includes the top 200 tracks across all regions, and store it in a Synapse Data Warehouse for analytics and predictive modeling of track popularity. This pipeline integrates several Azure services, including Blob Storage, Databricks, Data Factory, Synapse Analytics, Synapse Notebooks, and Azure Machine Learning (Azure ML), as well as orchestration tools like Apache Airflow. The purpose is to automate workflows, manage large-scale data processing, and generate actionable insights. Power BI is utilized for business intelligence and reporting, with Databricks notebooks executed using batch processing orchestrated by Data Factory.

---

## **Pipeline Architecture**

### **1. Data Ingestion**

- **Source**: Spotify dataset retrieved from Kaggle using the Kaggle API.**Aiflow** was used to orchestrate the data from kaggle to Blob Storage
- **Objective**: Extract raw track data containing attributes such as track names, artists, features (danceability, energy, tempo, etc.), and popularity.
- **Steps**:
  1. **Kaggle API Integration**:
     - Kaggle API is used to download the dataset.
     - Python libraries `kagglehub` and `requests` are employed for automation.
     ```python
     import kagglehub
     #Download latest version
     path = kagglehub.dataset_download("dhruvildave/spotify-charts")
     print("Path to dataset files:", path)
     ```
     This is block of code is used as one of the task in the python dag file , which was stored in the airflow dag folder. 
      Used wsl
  2. **Upload to Blob Storage via Airflow**:
     - Apache Airflow orchestrates the automation due to its strengths in workflow automation, such as scheduling, monitoring, and managing complex data pipelines. It was specifically chosen for its compatibility with WSL for Windows users, allowing seamless execution in a Windows environment, and its ease of integration with Azure services for handling large-scale data workflows.
     - The raw dataset is uploaded to an Azure Blob Storage container named `rawdata`. This step ensures centralized and scalable storage for subsequent processing.
  Here is the more refined guideline to establish this connection
Here's a step-by-step tutorial to set up and run an Airflow DAG for transferring Spotify data from Kaggle to Azure Storage:

## 1. Initial Setup

**Create and Activate Virtual Environment**
If your using windows operating system, install the wsl command for bash scripting ans ubutnu setup
once done , run the following commands
```bash
python -m venv airflow_env
source airflow_env/bin/activate
go to nano/airflow_env/bin/activate file, 
then 
add your azure storage credentials
export AZURE_STORAGE_ACCOUNT_NAME="your_account_name"
export AZURE_STORAGE_CONTAINER_NAME="your_container"
export AZURE_STORAGE_CONNECTION_STRING="your_storage_connection_string"

```

**Install Required Packages**
```bash
pip install apache-airflow
pip install kagglehub
```

## 2. Initialize Airflow

**Set Up Airflow Home**
```bash
mkdir -p /home/shushilgirish/airflow/dags
cd airflow/dags
```

## 3. Configure Airflow

**Edit Airflow Configuration**
```bash
nano airflow.cfg
```
Key configurations to modify:
- Set the correct dags_folder path
- Configure the database connection
- Set the appropriate timezone
<img width="1280" alt="airlfow cfg_scrrenshot" src="https://github.com/user-attachments/assets/0aca3eff-253e-44d2-9394-83773c30c9d1" />

## 4. Start Airflow Services

**Start Webserver**
```bash
airflow webserver --port 8081
```
<img width="1273" alt="Airflow_webserver" src="https://github.com/user-attachments/assets/444be708-92f0-4de4-a09d-44789d762275" />

**Start Scheduler in a New Terminal**
```bash
source airflow_env/bin/activate
airflow scheduler
```

## 5. Create Admin User
```bash
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
```

## 6. Verify Setup

**Check Running DAGs**
```bash
airflow dags list
```

**Check for Import Errors**
```bash
airflow dags list-import-errors
```

## 7. DAG Management

**View DAG Details**
```bash
airflow dags detail spotify_dagfile.py
```

## 8. Troubleshooting

**Check Port Conflicts**
```bash
lsof -i tcp:8080
```

**Stop Running Processes**
```bash
# Kill specific processes
kill 20003 44800 44801

# Kill scheduler using PID file
kill $(cat ~/airflow/airflow-scheduler.pid)
```

This setup allows you to create a DAG that:
- Fetches Spotify data from Kaggle
- Processes the data as needed
- Uploads it to Azure Storage Account
- Runs on a scheduled basis

Remember to configure the appropriate connections in Airflow for both Kaggle and Azure Storage authentication.
<img width="953" alt="AirflowDagTaskruns" src="https://github.com/user-attachments/assets/c7109afa-17c5-4545-a5c5-7bda0fd955af" />

### **2. Data Storage in Azure Blob**

- **Azure Blob Storage**:
  - Acts as the central repository for raw and processed data.
  - Containers:
    - `rawdata`: Stores the raw Kaggle dataset.
    - `cleandata`: Stores the cleansed and transformed data.
  - Airflow triggers scripts to process the dataset and write cleansed data back to the `cleandata` container.

---
<img width="1280" alt="Azure Storage container" src="https://github.com/user-attachments/assets/0bd72ada-d1b5-4288-b5b5-078973e2f90f" />


### **3. Data Transformation**

- **Platform**: Azure Databricks
- **Objective**: Cleanse and transform raw data by addressing missing values, normalizing features (e.g., tempo, loudness), and creating new interaction features such as `energy_tempo`. Write the processed data back to Blob Storage in Delta format for efficient querying and analysis.

#### **Steps**:

1. **Data Processing in Databricks**:
   - Raw data is loaded into a Databricks notebook.
   - Transformation steps include:
     - Handling missing values.
     - Normalizing numerical features (e.g., tempo, loudness).
     - Creating interaction features (e.g., energy \* tempo).
     ```python
     df["energy_tempo"] = df["energy"] * df["tempo"]
     ```
2. **Output**:
   - Transformed data is saved in **Delta format** in the `cleandata` container on Azure Blob Storage.
   ```python
   df.write.format("delta").mode("overwrite").save(output_path)
   ```
<img width="1280" alt="DataBricksDataProcessingADF" src="https://github.com/user-attachments/assets/6ae8eb41-9d18-4521-851e-140bd8be33e1" />

---

### **4. Data Warehousing in Synapse Analytics**
![Spotify Dimensional Model](https://github.com/user-attachments/assets/296023dd-c2ba-4016-881d-fc6f583db67b)


- **Platform**: Azure Synapse Analytics
- **Objective**: Store cleansed data in a data warehouse for advanced analytics and visualization.

#### **Steps**:

1. **Synapse Workspace Setup**:
<img width="1280" alt="SynapseCopyTransfer" src="https://github.com/user-attachments/assets/71e2e00f-a4df-47f2-b211-c05d8cbf2afe" />

   - A Synapse SQL pool is configured to serve as the data warehouse.
   - The Delta files from Blob Storage are ingested into Synapse using external tables and pipelines.

2. **Data Model**:

   - Tables:
     - **Staging Table**: Holds the cleansed data.
     - **Fact Table**: Tracks metrics like popularity.

   ```sql
   CREATE TABLE staging_tracks AS
   SELECT * FROM OPENROWSET(
     BULK 'https://shushilgstorage.blob.core.windows.net/cleandata/*.delta',
     FORMAT = 'DELTA'
   )
   ```

3. **Spark Notebooks for Analytics**:

   - Synapse Spark pools are used for exploratory data analysis (EDA) and visualization.
   - Analytical queries identify trends in track popularity based on attributes like region, tempo, and valence.

---

### **5. Business Intelligence with Power BI**
![SpotfiyPowerbi-1](https://github.com/user-attachments/assets/95561a04-18de-4be6-b989-687e89403128)


- **Platform**: Power BI
- **Objective**: Provide business insights from Synapse data.

#### **Steps**:

1. **Data Connectivity**:
   - Connect Power BI to the Synapse SQL pool via a DirectQuery connection.
2. **Dashboards**:
   - **Popularity Trends**: Visualize how popularity varies by region and time.
   - **Feature Correlation**: Highlight the influence of audio features on track popularity.

---

### **6. Machine Learning with Azure ML**
<img width="1280" alt="DatastoreAzureml" src="https://github.com/user-attachments/assets/66ef5253-c5cd-403e-8415-9959dad615fe" />
<img width="1280" alt="DataAssetAzureml" src="https://github.com/user-attachments/assets/4d3e0b70-697f-46c9-8d2d-0b00fecdc3da" />


- **Platform**: Azure ML
- **Objective**: Build and deploy a predictive model for track popularity.

#### **Steps**:

1. **Data Preparation**:

   - The cleansed data is pulled from Synapse staging tables into an Azure ML Notebook.
   - Features include `energy`, `tempo`, `valence`, and engineered features like `energy_tempo`.

2. **Model Building**:

   - Train a `RandomForestRegressor` model:
     ```python
     from sklearn.ensemble import RandomForestRegressor
     model = RandomForestRegressor()
     model.fit(X_train, y_train)
     ```
   - Evaluate the model using R² score and Mean Squared Error (MSE).

3. **Deployment**:

   - Register the model in Azure ML for deployment.

   ```python
   from azureml.core.model import Model
   model = Model.register(workspace=ws, model_path="outputs/model.pkl", model_name="spotify_popularity_model")
   ```

---

## **Final Architecture Summary**

1. **Data Ingestion**:

   - Source: Kaggle API.
   - Orchestrated via Apache Airflow.
   - Stored in Azure Blob Storage (`rawdata` container).

2. **Data Transformation**:

   - Platform: Azure Databricks.
   - Output: Cleansed data in Delta format, stored in Blob Storage (`cleandata` container).

3. **Data Warehousing**:

   - Platform: Synapse Analytics.
   - Data Model: Staging and fact tables in SQL pools.

4. **Analytics**:

   - Synapse Spark notebooks for advanced queries.
   - Power BI dashboards for visualization.

5. **Machine Learning**:

   - Platform: Azure ML.
   - Predictive Model: Predict track popularity based on audio features.

---

## **Key Insights and Applications**

1. **Music Popularity Trends**:

   - Identify attributes that drive song popularity.
   - Region-specific insights for targeted marketing campaigns.

2. **Recommendation System**:

   - Build a recommendation engine using the cleansed data.

3. **Business Optimization**:

   - Use Power BI to optimize promotional efforts based on streaming patterns.

---

## **Conclusion**

This project demonstrates a comprehensive Azure-based pipeline for ingesting, transforming, analyzing, and modeling Spotify track data. It showcases seamless integration across Azure services to achieve efficient data processing and actionable insights.

