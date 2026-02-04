# data-engineering-zoomcamp-2026
For Jan 2026 Data Engineering Zoomcamp from Data Talks Club

Module 2 Homework:
- I used the backfill functionality on Kestra to run the ELT pipeline that Will made in the lectures to backfill all yellow and green data for Jan 2021 to July 2021.
- I manually added all the KV pairs for the relevant GCP variables, and stored my service account credentials in Secrets using an environment file as recommended by Kestra.

Also, not sure what happened, but copying the code for the gcp-taxi-scheduled.yaml directly from the github repo and for some reason when merging the data from each backfill into the yellow/green_tripdata big table, the merge doesn't complete properly so that it only shows a single row in that table...