# Project Overview

(Sorry for the typing mistakes, did not have time to fix and review it)

This repository implements the solution of the Oppai challenge.
Answers to the questions of the Oppai Challenge pdf are at the end.

The infrastructure runs on Databricks AWS and this is a <b>fully deployed and ready-to-go</b> solution.
Many companies, including the company that I currently work at (WMcCann 5bi USD worth it agency),
runs their data pipelines on Databricks deployments like this one.
Checkout [Alexander Wada's GitHub](https://github.com/alexanderhowada/oppai_challenge.git) for the source code.

# Features (data analyst/engineering/science):
- Deploy on Databricks AWS
    - Lakehouse (data lake + warehouse). See https://www.databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html for details.
    - Integration with AWS S3 storage and other services.
    - Fully scalable.
- ACL
    - While I am not an expert in ACL, Databricks offfers its own ACL solution for its resources.
    - Secrets (see <code>dbutils.secrets</code>)
- Git Integration
    - Code is stored in GitHub.
- Full pipeline orchestration (see workflows)
    - Table tiers (raw/bronze/silver/gold)
    - CD (continuous deployment) with Git integration
    - Spark (Big Data framework)!
    - unittest (conceptual implementation)
    - data quality (conceptual implementation)
- Dashboards
    - On notebooks for money saving (see dashboards folder)
    - Can be deployed as custom dashboards (costs a mininum of 1USD per hour!)
- Machine learning (see machine_leaning folder)
    - Integration with OpenAI
    - Simple regression example (can be upgraded to a forecaster)

# Infrastructure

Databricks is a platform that comprises funcionalities for data engineers, analysts and scientists.
With Databricks data people can focus on results instead of infrastructure.

Databricks is deployed on top of a cloud service provider (AWS/GCP/Azure) and
it uses their infrastructure to run.
Therefore, the data is stored withing the cloud provider and not on Databricks!

For pricing, we have to pay for the cloud provider resources + Databricks.
The total pricing for this deployement is approximatelly:
2.5 USD per day (1.5 for AWS and 1.0 for Databricks)
plus a few cents for the ChatGPT API.
Here breaf pricing breakdown of this deployment:
1. AWS (total approx 1.5 USD per day)
    - 1.09 USD for nat gateway
    - the remaining is on EC2
2. Databricks (total approx 1 USD per day)
    - 99% of costs due to interactive machines (notebooks as in Jupyter notebooks)
    - 1% of costs due to pipeline runs.
3. ChatGPT
    - A few parts of cents per request.
    - Total cost < 1 USD.

Despite its high price, it is important to remember that Datbricks is a all-in-one solution.
The same solution without Databricks and with the similar funcionalities should use/implement:
1. Orchestrator (such as Airflow/Prefect)
    - Airflow must be deployed with Kubernetes
    - Prefect must be deployed with additional infrastructure for each job.
2. Dashboard solution (such as Power BI or Tableu)
    - Monthly subscription
3. SQL solution (Postgres/AWS Athena)
    - Postgres has daily costs for running it.
    - Athena can get very expensive for unoptimized datasets.
4. CI/CD
    - Databricks implements a simplified CD
    - CI/CD pipelines can be very cumbersome and take time to implement.
5. Workspace
    - Fully colaborative workspace that avoid problems with local development

Also, notice that Databricks is also capable of deploying Machine Learning solutions,
and that companies such as <b>iFood</b> uses it.

# S3 integration and lakehouse.

Databricks allow mounting S3 in its file system.
Here, the dbfs/mount_bucket_dbfs notebook shows how to mount.
Notice, that it is also important to follow this instructions (https://docs.databricks.com/en/dbfs/mounts.html)
in order to give the appropriated permission in AWS.
Since S3 is directly accessed by Databricks, this means that we can have all
structured and unstructed data at our disposal, therefore the lakehouse.

For structured data, this deployment uses Delta tables as the format for the tables.
The Delta format is open source and uses the parquet as its core,
but allows logs and ACID transactions.
Furthermore, data storage and processing are separated, therefore we only pay
for processing while active using the data.
See [Databricks explanation](https://docs.databricks.com/en/delta/index.html)
for more details.
Delta tables are highly compressed and stored in S3, hence it is much cheaper than
other solutions such as Athena.

# Data Pipelines

## Overview

The orchestrated data pipeline can be seen in the workflows tab (the cron is not configured, but it can run on a schedule).

The data pipelines notebooks are stored in the folder etl.
Here we follow [Databricks Medallion guideline](https://www.databricks.com/glossary/medallion-architecture)
with addition of the raw layer:

1. raw: appended data (to keep history).
2. bronze: latest data snapshot with fewer tranformations.
3. silver: transformed data for reports
4. gold: (not featured) business-ready tables.

Data pipelines greatly benefit from data quality and unittests.
Therefore, we also implement very simplified versions of quality and unittests.
See utils.data_quality and etl/tests for a very simplified view of the data quality and
unittest implementations.

## Medalion implementation explanation

We keep data history (raw tables) to keep track of changes over the months.
The changes can include user constribution, number of patrons, etc.
These are not implemented in the dashboards due to the absence of data.

Later, at the bronze layer, we keep track of the latest state.
This layer runs on incremental steps: at each step we select the latest
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
their "serverless" solution always runs two EC2 VMs that costs about 1USD per hour,
which if very expensive for a single person to use.
Hence the dashboards will be deployed in notebooks.

The notebooks in the folder dashboards contains several plots.
Some of these notebooks features widgets at the top whose values can be changed to customize the view.
After changing the values, please run the cell againg to update the results.
I suggest looking at the notebooks following the order:
1. users
2. posts
3. votes
4. top_countries
5. post_comments_summary (<b>ChatGPT Integration!!</b>)

These notebook contains a few annotations explaining a few of the results.
To observe the results it may be necessary to change the widgets values at the top and run the cells once again.
The notebooks have a few extra features like a linear regression (that can be upgraded to a forecaster),
and integration with ChatGPT (paid).

Here I would like to highlight the integration with the ChatGPT
for <b>summarizing thousands of post comments in a few seconds</b>.
While it is interesting to implement my own solution,
it is impossible to compete with the state-of-art developments of OpenAI.
Therefore, it is imperative to make use of its (paid) API
before building a solution on my own.

Notice that, instead of focusing on a full single use report,
I decided to deploy a reusable notebooks that can be used to aid several decisions.

# Oppai Challenge

## Part 1

A full exploratory data analisys should contain:
1. data summany:
    - min/max
    - distributions
    - null
    - duplication
    - outliers
2. correlation analysis
    - pair plots
3. timeseries
    - analysis

While I did these things to implement the ETLs and dashboards,
I think it is meaningless to explain it and further explore it
without more business context.
Instead, I prefered to provide a full ETL pipeline with quality and unittests,
and dashboards that can provide insights on a monthly basis.

In my opinion, exploratory data analysis requires you to explore
all the properties of every column and their combinations,
which is a process of order n^2.
Therefore, in order to optimize the exploration,
I believe that exploratory data analysis should be performed
with a clear (business) objective.

It is also important to notice that good implementations
of data quality and unittests should cover a great portion of an exploratory data analysis.

## Part 2

### Porque você modelou dessa forma?

Some of the explanations are in the Data Pipelines section.

I modeled the data in this way mainly due to the dataset size and simplicity.
There is not need to complicate things that can be simple.
Here is a breakdown of things that I considered:
- dataset size:  
    > small datasets do not need a lot of maintenance, therefore there is not need to complicate it.
- easy maintenance: 

    > The model that I chose uses the JSON formats directly from the files, therefore it is easier to understand, maintain and track the path of the data.

    > my implementation of the JSON ETL is very systematic, therefore I could build a library for processing this kind of JSON data and deploy it at a larger scale (notice that the classes in the ETL scripts are very similar).

- medalion tier:
    > Adding medalion tier aggregates meaning (governance) to the tables, since only tiers silver and gold are of interest to extract relevant information.

    > Tables have their individual meaning. This ensures that every table can be reused for different purposes.
- separate translations from posts:
    > while it was not necessary to remove the translations from the posts dataset, I believe it will be common to add more translations in the future.
    Therefore, separating the posts from its translations seems a right business decition.

- unnesting columns:
    > A few datasets unnest the arrays. This makes it easier to analyze the data later.

- merge:
    > The merge (upsert) statement (utils.delta.merge) is a very important part of the ETL as it allows to update the data without adding new rows.

- no partitioning:
    > partitioning is very important as it allows to read only part of the data instead of reading the entire dataset when executing a query. However, the dataset is very small and partitioning would only make things more complicated (and possibly more expensive).

- No dimensional modeling:
    > I decided to not use dimensional modeling due to the small size of the dataset, however I would implement it if the dataset was larger (terabytes).

- ETL by updated_at_date:
    > for scalability purposes, it is interesting to process only the data that have changed. Therefore, I process data in according to its updated date.

    > procesing by updated_at_date is a flexible process as the same script can be used to process the entire dataset and or latest data.

## Essa modelagem serviria pra um treinamento de um modelo de machine learning?

Machine learning pipelines can be very complex and may require
very specific data transformations and enrichement.
Therefore, this pipeline will be the basis for machine learning models.

For a better understanding, lets consider the following examples:

1. Say we want to build a ML model that will use the words of the post comments to build predictions.
In this case, we can use the current pipeline and build a new (silver) table where we translate to english, normalize and remove stop words from the comments (most ML models only work for a single language). This table could be used to feed a ML model.
1. We want to build a model that classify post comments as relevent/irrelevant suggestions. In this case we could use the table built at step 1 and create a new ML table with new features for the comments. Among these features, we could include number of words, number of complex words, metrics for [text redability](https://en.wikipedia.org/wiki/Readability), and [lexical diversity](https://en.wikipedia.org/wiki/Lexical_density). With the two new tables, we could build a model to select the relevant suggestions and forward them to the staff.
1. We want to build a simple forecaster to predict the monetary contribution at the end of 2024. By keeping historical data (raw tables), we could build a new (bronze) table with timeseries of monetary contribution. This new table could be used to build a ML model to build a forecaster. The results of the forecaster could be used to compare our yearly growth.

# As estruturas das collections fornecidas podem ser melhoradas para uma melhor análise dos dados? Se sim, sugira melhorias para as estruturas.

Yes, the structures could get a little bit better.
I believe that this document
```
{
    "_id": {"$oid": "1234"},
    "body": "this is my body",
    "created_at": {"$date": "2023-01-01 00:00:00.1234Z"}
}
```
would be better if was in the format
```
{
    "_id": "1234",
    "body": "this is my body",
    "created_at": "2023-01-01 00:00:00.1234Z"
}
```

I worked with MongoDB and know how difficult it is to manipulate data,
however the later format is a lot clearer and simplifies the ETL a lot.

Why the second format is better?
The second format is better since it simplifies column names and eliminates the possiblity of new nested data
```
{
    "_id": {"$oid": "1234", "look_a_new_field": "new field},
    "body": "this is my body",
    "created_at": {"$date": "2023-01-01 00:00:00.1234Z}
}
```
Notice that the current deployment is capable of dealing with new columns (see [schema evolution](https://www.databricks.com/blog/2020/05/19/schema-evolution-in-merge-operations-and-operational-metrics-in-delta-lake.html) for details).
The new column in the current deployment would be named "id_look_a_new_field".

Another change regards how data is delivered.
I would prefer that data is extracted (to S3) with their extraction date "file_name-yyyy-mm-dd_HH:MM:SS.json".
This ensures we have a backup history in S3 and allows for easy processing of both historical (by date range) and incremental (by last date) ETLs.

## What can improve?

While this is a complete functional deployment, there are many things that could improve.
Here is a breakdown of things that I would like to implement from the data engineering, science and analyst point of view.

### Data engineering

- Create reusable code.
    > The classes created for the ETL have a clear pattern, hence I could build a single class and inherit the desired properties to other processes.
- Add README, typing and docstrings
    > Documentation is very important and having a README for each individual folder can save time in the future.

    > typing plays a very important role in Python as it make simples to understand the inputs of functions and methods and is very important when creating reusable code.

    > similarly to typing, docstrings is also very important when writing reusable code.
- Better governance
    > Governance is very important and writing a document the describes the properties or raw, bronze, silver and gold tables and their columns is very important. For example, I could set that no column in the silver and gold tiers have null values. This would improve the data analysis because it avoids the use of the COALESCE statment.
- Better data quality
    > Data quality is important because it tests the data against its statistical propertier. In a way, data quality plays a very similar role of unittests in the world of data.
- Better unit/integration tests
    > Unittests ensure that the code I wrote do what I want it to do, and writing it is an investment for the future. It only takes about 3 to 6 months until it becomes extremaly important.
- CI/CD
    > While Databricks offers a simplified CI/CD, it would be important to implement a full CI/CD pipeline that runs all tests automatically and ensures the correct deployment configuration.
- Implement unity catalog
    > Unity catalog allows ACL for the delta tables. This is very important when working with many colaborators, but it requires bigger machines and therefore an increase in price.

### Data science

There are many things that I would like to implement in machine learning,
however time is short and I had no time to implement anything but a simple linear regression.
In addition, I decided that was more important to integrate ChatGPT than to implement a ML model.

These are the models that I would like to implement and learn:
- Custom NLP models for:
    - sentiment analysis
    - summaryze comments
    - comment classifiers
    - generators with fixed personality
- Custom image generators to optimize drawings and increase creative productivity.
- Simple regressions and forecasters
- Clusterization of CRM and other data

There are also many things in the Databricks ecosystem that I would need to master.
Some of these are:
- Feature store
- MLlib
- ML experiments
- Deployment of ML models

### Data analyst

To be quite honest, I believe that I do not do a good job as a Data Analyst.
To improve I need to start from the very basics and take lessons on history telling
and understand more about the business.
I also could have made a detailed exploraty data analysis, but I decided to do just the necessary.

I believe that I could also have generated a report with all the results.
This would have made a lot easier to understand the results.
