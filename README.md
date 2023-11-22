# Oppai Challenge

# Overview

This repository implements the solution of the Oppai challenge.
The infrastructure runs on Databricks AWS.
Checkout [Alexander Wada's GitHub](https://github.com/alexanderhowada/oppai_challenge.git) for the source code.

This is a <b>fully deployed and ready-to-go</b> solution.

# Features (data analyst/engineering/science):
- Deploy on Databricks AWS
    - Lakehouse (data lake + warehouse).
    - Integration with AWS S3 storage.
    - Fully scalable.
- Full pipeline orchestration (see workflows)
    - Table tiers (raw/bronze/silver/gold)
    - CD (continuous deployment) with Git integration
    - Spark!
- Dashboards
    - On notebooks for money saving (see dashboards folder)
    - Can be deployed as custom dashboards (costs a mininum of 2USD per day!)
- Machine learning (see machine_leaning folder)
    - Integration with OpenAI
    - Simple regression example (can be upgraded to a forecaster)
- ACL
    - While I am not an expert in ACL, Databricks offfers its own ACL solution for its resources.
- Git Integration
    - Code is stored in GitHub.

# Infrastructure

Databricks is a platform that comprises funcionalities for data engineers, analysts and scientists.
With Databricks data people can focus on results instead of infrastructure.

Databricks is deployed on top of a cloud service provider (AWS/GCP/Azure) and
it uses their infrastructure to run.
Therefore, the data is stored withing the data provider and not on Databricks!

For pricing, we have to pay for the cloud provider resources + Databricks.
The total pricing for this deployement is:

Despite its high price, it is important to remember that Datbricks is a all-in-one solution.
The same solution without Databricks and with the similar funcionalities should use:
1. Orchestrator (such as Airflow/Prefect)
    - Airflow must be deployed with Kubernetes
    - Prefect must be deployed with additional infrastructure for each job.
2. Dashboard solution (such as Power BI or Tableu)
    - Monthly subscription
3. SQL solution (Postgres/AWS Athena)
    - Postgres has daily costs for running it.
    - Athena can get very expensive for unoptimized datasets.

Also, notice that Databricks is also capable of deploying Machine Learning solutions,
and that companies such as <b>iFood</b> uses it.

# S3 integration

Databricks allow mounting S3 in its file system.
Here, the dbfs/mount_bucket_dbfs notebook shows how to mount.
Notice, that it is also important to follow this instructions (https://docs.databricks.com/en/dbfs/mounts.html)
in order to give the appropriated permission in AWS.

# Data Pipelines

## Overview

The orchestrated data pipeline can be seen in the workflows tab (the cron is not configured, but it can run on a schedule).

The data pipelines notebooks are stored in the folder etl.
Here we follow Databricks Medallion guideline (https://www.databricks.com/glossary/medallion-architecture)
with addition of the raw layer:

1. raw: appended data (to keep history).
2. bronze: latest data snapshot with fewer tranformations.
3. silver: transformed data for reports
4. gold: (not featured) business-ready tables.

## Medalion implementation explanation

The ideia of keeping an data history is to keep track of changes over the months.
The changes can include user constribution, number of patrons, etc.
These are not implemented in the dashboards due to the absence of data.

Later, at the bronze layer, we keep track of the latest state.
This layer runs on incremental steps: at each step we selectt the latest
updated dates (updated_at_date) and merge (upsert) the data by primary key.
This ensures that we keep the data updated and encapsulates the full and incremental ETLs
into a single process.
The steps can be configured at the etl/etl_constants script by changing the
<code>DT_START</code> and <code>DT_END</code> variables.

The silver tables are all implemented as views and are joins between the tables.
Since they are views, we ensure that that latest data is always there.

# Dashboards

## Overview

While Databricks offers a SQL Warehouse with Dashboards,
their "Serverless" solution always runs two EC2 VMs that costs about 1USD per hour,
which if very expensive for a single person to use.
Hence the dashboards will be deployed in notebooks.

The notebooks in the folder dashboards contains several plots.
Some of these notebooks features widgets at the top whose values can be changed to customize the view.
After changing the values, please run the cell againg to update the results.
I suggest looking at the notebooks following the order:
1. users
2. posts
3. votes

These notebook contains afew annotations explaining a few of the results,
and there are a few extra features like a linear regression (that can be upgraded to a forecaster),
and integration with ChatGPT (paid).

# TODO

## Analyst

1. investigate columns
2. 

## Science

### Users
1. Clusterization?
2. Infer gender
3. Overall feeling

### Posts/Comments

1. Model for translation
2. Add text metrics
    - perplexity
    - phrase size
    - keywords most used
    - toxicity?
    - sentiment classification
3. ChatGPT reports
    - Generate weekly/monthly reports
4. Clustering:
    - suggestions?
    - complaints

### Votes
1. Votes with and without weights




