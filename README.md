# PySpark Tutorial Project <img src="https://go-skill-icons.vercel.app/api/icons?i=spark" height="30"/>

This repository contains my practice and implementation of the concepts learned from the PySpark beginner tutorial:

📺 Tutorial link: https://youtu.be/EB8lfdxpirM  

The goal of this project is to understand the fundamentals of Apache Spark using PySpark, including RDDs, DataFrames, and Spark SQL, and to build a solid base for Data Engineering workflows.

---

## 📌 Topics Covered

- Introduction to Apache Spark & PySpark  
- Creating `SparkContext` and `SparkSession`  
- RDDs:
  - Transformations
  - Actions
  - Basic operations  
- DataFrames:
  - Creation from different sources (CSV, JSON singleLine & multiLine, Parquet)
  - Filtering, selecting, aggregations
  - Column operations  
- Spark SQL:
  - Temporary views
  - SQL queries on DataFrames  

---

## 🚀 Why DataFrames?

Although the tutorial introduces RDDs, the main focus is on DataFrames since they are:
- Faster (Catalyst Optimizer + Tungsten Engine)
- Easier to write and maintain
- Schema-aware
- Standard in modern Data Engineering pipelines

RDDs are mainly used for learning Spark internals or very low-level transformations.

---

## 🛠️ Requirements

To test the code, make sure you have the following installed:

- Python 3.10
- Java jdk 17 or 21
- hadoop version 3.3.6 (WinUtils if you have Windows https://github.com/cdarlint/winutils/tree/master)
- PySpark==3.5.1
- Set Environment Variables: Windows Search > View advanced system settings > Environment Variables... > System variables:
  - New:
    * Variable name: HADOOP_HOME
      Variable value: C:\hadoop
    * Variable name: JAVA_HOME
      Variable value: C:\Program Files\Java\jdk-17
  - Path > Edit... > New:
    * C:\hadoop\bin
    * %JAVA_HOME%\bin
  - Do some checks in the cmd:
    * echo %HADOOP_HOME%
    * echo %JAVA_HOME%
- VSCode / Jupyter Notebook / JupyterLab
