# ETL Pipeline in Apache Airflow

### 🧩Overview
This project demonstrates a **daily ETL pipeline** built in **Apache Airflow**.  
The DAG automates the extraction, transformation, and loading of user activity data (feed actions and messages)  
from the ClickHouse simulator database into an analytical layer for further reporting.

---

### DAG Description
The DAG runs **daily at 07:00 (MSK)** and performs the following tasks:

1. **Extract Feed Data** — retrieves user actions (likes, views) from `feed_actions`.  
2. **Extract Message Data** — retrieves messaging activity (sent/received) from `message_actions`.  
3. **Transform Feed + Message Data** — merges both datasets on user-level.  
4. **Apply Data Transformations** to create analytical slices by:
   - operating system (OS);  
   - user gender;  
   - user age group;  
   - contact type.  
5. **Load the final table** into the ClickHouse schema `test.aleksandr_antonov_hnm5755`.

---

**Schedule & Configuration**

| Parameter | Value |
|------------|--------|
| **Owner** | `aleksandr_antonov_hnm5755` |
| **Start Date** | `2025-09-29` |
| **Schedule Interval** | `0 7 * * *` |
| **Retries** | 2 |
| **Retry Delay** | 5 minutes |
| **Database** | ClickHouse (simulator) |

---

### 📊 DAG Visualization
Below is a screenshot of the DAG execution tree from **Airflow Web UI**:

![ETL DAG](./ETL%20pipeline.png)

Each green square represents a successful run of the DAG over the daily schedule period.

---

### ⚙️ Tools & Technologies
![Python](https://img.shields.io/badge/Python-20A5A8?style=flat&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCB00?style=flat&logo=clickhouse&logoColor=black)
![Pandas](https://img.shields.io/badge/Pandas-20A5A8?style=flat&logo=pandas&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-20A5A8?style=flat&logo=numpy&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-20A5A8?style=flat&logo=jupyter&logoColor=white)

---

### 🚀 Usage
All materials are provided for reference and demonstration purposes.  
Database connections and Airflow runtime are not required to explore the project.

To review:
1. Open the file **`dag_aleksandr_antonov_hnm5755_etl.py`** — full DAG logic and dependencies.  
2. View the DAG execution tree in **`ETL pipeline.png`**.  
3. No code execution is necessary — the project illustrates structure, dependencies, and ETL design.

---

### 📂 Repository structure

- dag_aleksandr_antonov_hnm5755_etl.py     # airflow DAG code.  
- ETL pipeline.png                         # airflow Tree View.  
- README.md                                # project description

---

### Author
**Aleksandr Antonov**  
📊 Product & Data Analyst  
🎓 [Karpov.Courses — Data Analyst Simulator](https://karpov.courses)

---
