# Data Cleaning Project - Layoffs Dataset

## 📌 Project Overview
This project focuses on **cleaning and preprocessing** a dataset containing information on **company layoffs**. The dataset includes details such as company names, locations, industries, total layoffs, funding raised, and more. The goal of this project is to **enhance data quality** by removing inconsistencies, handling missing values, and ensuring standardized formatting using **MySQL**.

## 📊 Dataset Information
- **File Name:** `layoffs.csv`
- **Total Entries:** 2,361 rows
- **Columns:** 9
  - `company`: Name of the company
  - `location`: Company headquarters
  - `industry`: Industry category
  - `total_laid_off`: Number of employees laid off
  - `percentage_laid_off`: Percentage of workforce laid off
  - `date`: Layoff announcement date
  - `stage`: Funding stage of the company
  - `country`: Country of the company
  - `funds_raised_millions`: Total funds raised in millions

## 🔧 Data Cleaning Steps
### 1️⃣ Removing Duplicates
- Used a **Common Table Expression (CTE)** to detect and eliminate duplicate records.

### 2️⃣ Standardizing Data
- Ensured consistency in text formatting (e.g., removing extra spaces, capitalization issues).

### 3️⃣ Handling Missing Values
- Identified `NULL` or blank values and addressed them appropriately.
- Applied imputation techniques where necessary.

### 4️⃣ Dropping Irrelevant Columns
- Removed columns that were unnecessary for analysis.

### 5️⃣ Creating Staging Tables
- Created two **staging tables** (`layoffs_staging` and `layoffs_staging2`) to preserve original data and perform transformations efficiently.

## 💻 Technologies Used
- **MySQL** – Used for executing SQL queries and performing data cleaning operations.
- **Jupyter Notebook / Python (Optional)** – Could be used for further exploratory data analysis (EDA).

## 📜 SQL Queries Used
The cleaning process was executed using **SQL queries**, including:
- `CREATE TABLE` – To create staging tables for transformation.
- `INSERT INTO` – To populate the staging tables.
- `ROW_NUMBER() OVER(PARTITION BY...)` – To detect duplicate records.
- `DELETE` – To remove unwanted rows.

## 📌 Why This Project?
Data cleaning is a crucial step in **data analysis and machine learning**. Unclean data can lead to inaccurate insights and faulty decision-making. By applying **systematic data cleaning techniques**, we ensure that our dataset is **reliable, consistent, and ready for further analysis**.

## 🚀 How to Use This Repository
1. Clone this repository to your local system.
   ```bash
   git clone https://github.com/your-username/data-cleaning-project.git
