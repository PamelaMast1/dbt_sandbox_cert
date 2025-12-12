Welcome to my sandbox dbt project!!!

A little playground where I experiment with dbt concepts, BigQuery patterns, and anything I’m learning for my 
Analytics Engineering certification. Nothing production-ready, just me thinking, testing, and breaking things on purpose.

This repo is my sandbox for:

Practising incremental strategies (append / merge / insert_overwrite)
Snapshots and SCD concepts
Building macros and testing ideas

generally trying things without fear of ruining anything important

### NOTE!!
This project is intentionally messy, exploratory, and full of experiments.
It’s where I learn by doing, so expect rough edges, inconsistency and not adhering to what I would develop in the real world!!!

If you’re curious about how I explore new tools, or you want to see my thought process as I grow my SchemaNest data engineering toolkit then please also visit my website
https://schemanest.com/

# Environment set up (BigQuery)
1. Create dev and production GCP projects.
2. Create service account in DEV.
   permissions = BigQuery Admin
   (lose lower / tailored permissions in real world projects)

# Bootstrap source test data. 
1. Generate the source tables with small amounts of test data - 
   dbt run-operation bootstrap_sources_raw_workout_data
   If you delete the table you will need to temporarily disable the models {{ config(enabled = false) }} 


### Resources I find helpful:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Quanalabs Analytics Engineer test questions! https://www.qanalabs.com/courses/dbt-developer 
  NOT DUMPS, but an incredibly comprehensive compilation of questions that help you prepare for the real exam.


