# Official implementation references

Checked while preparing this POC. Versions in the Docker image are deliberately pinned for this example, not described as the newest releases. Review support status and security fixes before production use.

- Spark 3.5.6 performance tuning / adaptive query execution: https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html
- Airflow 2.10.5 Docker Compose guide: https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html
- Snowflake unloading to S3: https://docs.snowflake.com/user-guide/data-unload-s3
- Snowflake COPY INTO location: https://docs.snowflake.com/en/sql-reference/sql/copy-into-location
- Snowflake S3 storage integration and external-ID trust: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
- Snowflake key-pair authentication: https://docs.snowflake.com/en/user-guide/key-pair-auth
- MLflow Spark API: https://mlflow.org/docs/latest/api_reference/python_api/mlflow.spark.html
- MLflow model registry aliases: https://mlflow.org/docs/latest/ml/model-registry/workflow/
- EC2 instance metadata options for containers: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-IMDS-new-instances.html
- CloudFormation EC2 metadata options: https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-properties-ec2-instance-metadataoptions.html
