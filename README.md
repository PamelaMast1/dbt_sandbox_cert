Welcome to my sandbox dbt project!!!

# BigQuery set up
1. Create dev and production GCP projects.
2. Create service account in DEV.
   name = [name]-dev-serv-acc
   description = dev access to GCP
   permissions = BigQuery Admin

# dbt project set up. 
1. Generate the source tables with small amounts of test data - 
   dbt run-operation bootstrap_sources_raw_workout_data
   If you delete the table you might need to add {{ config(enabled = false) }} to the models


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
