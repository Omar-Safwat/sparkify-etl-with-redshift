### Introduction

In this project, I setup an ETL pipeline for the music streaming startup, Sparkify. Sparkify want their database on the cloud. They want to move their data from an **AWS S3** storage, to tables in **AWS Redshift**, a data warehouse where their analytics team can have access to the data.

### Source Data
Source data is stored used in two public S3 storage buckets in file **json** format. The first bucket contains information on the songs and the artists. The second bucket logs user activity on the app, i.e., which song they were listening to, and when...etc. 

#### Staging Table 
The **json** data is ingested in bulk into the **staging area** as tables, using the SQL `COPY` command, where it will be transformed to the desired schema.<br>
**The two tables are:**
* **staging_events** - logs user activity on the app (who listened to what, when, and from which device...etc.)
+ **staging_songs** - information on the songs and their artists

### Datawarehouse
**AWS Redshift** service will then inserted into a **Redshift** cluster where it can be later used for analytics and business insights. **Redshift** Supports parallel processing and sits on top of a cluster of nodes, where data is idealy equally distributed, to leverage the parallel processing speed.

### Database Schema
The database schema used is the **star schema**. Star schema was chosen for its
* Readability
* Fast queries (Less joins needed)

**The star schema mainly consists of two types of tables:**
* Fact table
* Dimension tables

#### Fact Table 
The schema is centered around queries for **song playing events**. The fact table records these events. The fact table is usually the largest table in the schema, and is therefore, partitioned by a key and distributed over the nodes.
* **songplays**: Is the fact table that records event data associated with song plays.

#### Dimension Tables
Dimension tables contain more specific information, about the user, artists, songs...etc. They are usually small enough to be broadcasted to all cluster nodes, which make future joins with the fact table much faster.
* **users**: Information on users using the app.
* **songs**: Information on songs the app offers.
* **artists**: Information on artists the app hosts.
* **time**: Time data available in **songplays** broken down into units (year, month, ..., hours, ...etc.)

### AWS resources Setup
* Create an `IAM role` for programatic access with Admin rights to be able to create the services in an `IAC`.
    * Use the **access key** and **secret key** to create clients for `EC2`, `S3`, `IAM`, and `Redshift` (using `boto3`).
* Create an `IAM role` for **Redshift**
  * Specify an `AssumeRolePolicyDocument` to attach it later to **Redshift** cluster.
  * Attach an `AmazonS3ReadOnlyAccess` role policy.
* Create a `RedShift Cluster`
    * Take note of its endpoint and the IAM role arn (`HOST` and `ARN` in `dwh.cfg`).

### ETL Pipeline
+ Created tables to store the data from `S3 buckets`.
+ Loading the data from `S3 buckets` to staging tables in the `Redshift Cluster`.
+ Inserted data into fact and dimension tables from the staging tables.

### Project Structure

* `create_tables.py` - Drop old tables if they exist, and create new ones.
* `etl.py`: Run queries to load `JSON` data from `S3` into `Redshift`.
* `sql_queries.py`: Define SQL statements to `CREATE`, `DROP`, `COPY` and `INSERT` tables.
* `dhw.cfg`: Configuration file for authentication and establishing connection to redshift.

### How to Run

1. Create tables with `create_tables.py`.
2. Run ETL pipeline with `etl.py`.