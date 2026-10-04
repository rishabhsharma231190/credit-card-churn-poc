# Deploy the batch model to AWS and connect Snowflake

Follow the local 10,000-customer test in README first. This guide creates billable resources in **your account**. You need AWS permissions for CloudFormation, EC2, S3 and IAM and a Snowflake administrator for initial setup. The runtime uses narrower roles. No account credentials are supplied in this ZIP, and these cloud operations have not been executed by ChatGPT.

The simplest deployment for your deadline is one EC2 host running the same Docker image as the local demo. A real-time endpoint is not needed for scheduled batch scoring. Keep your Snowflake account and S3 bucket in a compatible nearby region where possible to avoid unnecessary transfer latency/cost.

## A. Provision the host and data bucket

1. In AWS, choose your region (for example, Mumbai `ap-south-1` if appropriate for your Snowflake account).
2. Make sure an EC2 key pair exists in that region and you have its `.pem` private file on your computer. Never upload the key here.
3. Open **CloudFormation → Create stack → With new resources → Upload a template**. Upload `aws/cloudformation.json`.
4. Stack name: `credit-card-churn-poc`.
5. Select a VPC and a **public subnet in that same VPC** with an Internet Gateway route. In an account without a default VPC, have your AWS administrator provide these.
6. Choose the EC2 key pair. Set `AdminCidr` to your current public IPv4 address followed by `/32`. Do not enter your private `192.168...` address.
7. Keep `m6i.2xlarge` for an 8-vCPU/32-GiB starting host, or use an allowed smaller instance after evaluating your budget. The template allocates 100 GiB encrypted gp3 EBS. There is no quoted fixed running cost.
8. Review the IAM resource creation acknowledgement and create the stack. Wait for `CREATE_COMPLETE`.
9. In **Outputs**, record `BucketName`, `PublicIP` and `InstanceId`. S3 blocks public access, uses SSE-S3 encryption, and expires `churn-poc/runs/` data after 14 days. MLflow artifacts on EBS do not share that lifecycle.

The host role permits read/write/delete only within this bucket's `churn-poc/` prefix. The EC2 instance uses IMDSv2 with hop limit 2 so containers can obtain temporary role credentials. Do not add static AWS keys to `.env` on EC2.

## B. Copy the project and start Docker

From your Windows PowerShell, in the directory containing the downloaded ZIP, substitute your real key path and the stack PublicIP:

```powershell
scp -i "C:\Keys\churn.pem" .\credit-card-churn-poc.zip ec2-user@YOUR_PUBLIC_IP:/home/ec2-user/
ssh -i "C:\Keys\churn.pem" ec2-user@YOUR_PUBLIC_IP
```

On EC2:

```bash
cloud-init status --wait
unzip credit-card-churn-poc.zip
cd credit-card-churn-poc
docker version
docker compose version
python3 scripts/init_env.py
```

If Docker says permission denied after bootstrap, log out and reconnect so the `docker` group membership applies. If bootstrap failed, inspect `/var/log/cloud-init-output.log`; the host needs outbound HTTPS to package repositories and GitHub for the pinned Compose binary.

Use `nano .env` to set `ROWS=10000` and retain `PIPELINE_MODE=local`. Then run the six service setup commands from README. You build `credit-card-churn:0.1` directly on AWS; this is a Docker image deployment even without ECR. For a later CI/CD setup, tag and push this same image to a private ECR repository.

In another PowerShell window, open the UI tunnel:

```powershell
ssh -i "C:\Keys\churn.pem" -N -L 8080:127.0.0.1:8080 -L 5000:127.0.0.1:5000 ec2-user@YOUR_PUBLIC_IP
```

Keep that window open. Visit <http://localhost:8080> and <http://localhost:5000> on your own computer. Do not open inbound 8080 or 5000 in the security group. Stop the local laptop stack first if it already occupies those ports, or use local tunnel ports 18080 and 15000 instead.

Run a 10,000-customer local test **on EC2** to prove the AWS-hosted image and MLflow service work before adding Snowflake.

## C. Give Snowflake a separate AWS role

Snowflake cannot use the EC2 role automatically; it needs its own storage integration and assume-role trust.

1. AWS IAM → Policies → Create policy → JSON. Use `aws/snowflake-s3-policy.json`, replacing `YOUR_BUCKET` with the stack BucketName. Name the policy `ChurnSnowflakeS3Policy`.
2. IAM → Roles → Create role → AWS account → This account. Name the role `ChurnSnowflakeS3Role` and attach only that policy. This is temporary trust until step D; do not use it for application execution yet.
3. Copy its ARN, for example `arn:aws:iam::123456789012:role/ChurnSnowflakeS3Role`.

## D. Set up Snowflake storage integration

1. In a Snowflake worksheet, open `sql/01_setup.sql`.
2. Replace the bucket name, AWS account ID and role ARN. If using a different S3 prefix, update all policies, the SQL allowed location and `.env` consistently. Default `churn-poc/` is simplest.
3. Run the file as an administrator. It creates a dedicated database, X-Small auto-suspending warehouse, runtime role and storage integration.
4. In the `DESC INTEGRATION CHURN_POC_S3` result, copy **STORAGE_AWS_IAM_USER_ARN** and **STORAGE_AWS_EXTERNAL_ID** exactly.
5. Open `aws/snowflake-trust-policy.json`, replace its two placeholders, and paste that JSON into IAM → `ChurnSnowflakeS3Role` → Trust relationships → Edit trust policy. This replaces the temporary account trust with Snowflake's specific principal and external ID.
6. Allow a short time for IAM propagation, then continue. Avoid replacing the Snowflake integration: recreation can change its generated external ID and break the trust.

The runtime generates synthetic data **inside Snowflake first** with `CREATE TABLE AS SELECT`. This avoids uploading millions of locally generated records simply to demonstrate ingestion. To use your own data later, replace only the `ingest` implementation with a source load and preserve the source schema. A production CSV/SFTP/source-system ingestion job is outside this synthetic POC.

## E. Create a Snowflake service-user key

On EC2, inside the project folder:

```bash
mkdir -p secrets
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM -out secrets/rsa_key.p8
openssl pkey -in secrets/rsa_key.p8 -pubout -out secrets/rsa_key.pub
```

OpenSSL asks for an encryption passphrase. Keep it private. Display only the public key with `cat secrets/rsa_key.pub`. Remove the `BEGIN/END PUBLIC KEY` lines and join the remaining public-key lines when putting the body into `sql/02_service_user.sql`. Run that SQL in Snowflake as an administrator. Do not paste the private key into SQL, chat or source control.

Make the mounted private key readable only by the container user and administrators:

```bash
sudo chown 50000:0 secrets/rsa_key.p8
sudo chmod 640 secrets/rsa_key.p8
chmod 600 .env
```

Update `.env` on EC2:

```dotenv
PIPELINE_MODE=cloud
ROWS=10000
AWS_DEFAULT_REGION=ap-south-1
S3_BUCKET=YOUR_STACK_BUCKET_NAME
SNOWFLAKE_ACCOUNT=YOUR_ORG-YOUR_ACCOUNT
SNOWFLAKE_USER=CHURN_SVC
SNOWFLAKE_ROLE=CHURN_POC_ROLE
SNOWFLAKE_WAREHOUSE=CHURN_POC_WH
SNOWFLAKE_DATABASE=CHURN_POC_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_STORAGE_INTEGRATION=CHURN_POC_S3
SNOWFLAKE_PRIVATE_KEY_FILE=/opt/airflow/secrets/rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE='YOUR_PRIVATE_PASSPHRASE'
```

Use the account identifier shown in your Snowflake account details, without `https://` or a `.snowflakecomputing.com` suffix. Single-quote a passphrase containing `$` in Compose `.env` to prevent interpolation; avoid newlines and a literal single quote for this demo. For production, inject secrets from an approved secret manager rather than keeping the passphrase in environment configuration. This POC's local encrypted-key mount is intentionally simple.

## F. Verify credentials before the full run

On EC2:

```bash
docker compose run --rm runner -c "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"
docker compose run --rm runner -c "from churn.snowflake_io import connection; c=connection(); print(c.cursor().execute('SELECT CURRENT_USER(), CURRENT_ROLE()').fetchone()); c.close()"
```

These should show the EC2 instance role and `CHURN_SVC` / `CHURN_POC_ROLE`, respectively. Do not print private keys or credential environment values.

Recreate services and trigger the cloud smoke run:

```bash
docker compose up -d --force-recreate airflow-webserver airflow-scheduler
docker compose exec airflow-scheduler airflow dags unpause credit_card_churn_poc
docker compose exec airflow-scheduler airflow dags trigger credit_card_churn_poc
```

In Snowflake, look for two `POC_TRAIN_<hash>` / `POC_SCORE_<hash>` tables in `CHURN_POC_DB.PUBLIC`. In S3, look under:

```text
churn-poc/runs/<run-id>/raw/train/
churn-poc/runs/<run-id>/raw/score/
churn-poc/runs/<run-id>/prepared/
churn-poc/runs/<run-id>/features/
churn-poc/runs/<run-id>/model/manifest.json
churn-poc/runs/<run-id>/scores/predictions/
churn-poc/runs/<run-id>/scores/summary.json
churn-poc/runs/<run-id>/timings/
```

Snowflake uses its integration to unload to S3 directly; the Python driver does not fetch 2 million source rows. Spark reads staged Parquet files from EBS on this single-node host; boto3 moves stage outputs back to S3.

## G. Run the complete 2-million-customer cloud demo

After the cloud smoke DAG is green, set `ROWS=2000000`, keep `TRAIN_MODEL=true` for your first full model, recreate the Airflow services, and trigger again. Check:

- Each Snowflake snapshot contains 2,000,000 rows.
- `prepared/quality.json` has 2,000,000 training and 2,000,000 scoring rows.
- MLflow contains test metrics and a registered numeric version.
- `scores/summary.json` reports exactly 2,000,000 output rows.
- Scores are under this run's S3 prefix and carry the manifest's model version.

A green task graph is not enough: inspect those counts and versions. Take screenshots for the demo. The schedule is already in the DAG; the host and services must stay running.

## H. Cost control, persistence and cleanup

The Snowflake warehouse auto-suspends after 60 seconds idle. The stack's S3 lifecycle removes synthetic run data after 14 days. EC2 continues billing while running; EBS and S3 remain billable after stopping EC2. Snowflake transient source tables are **not automatically deleted**. After your demo, remove only the run-specific `POC_TRAIN_*` and `POC_SCORE_*` tables you no longer need. Do not drop unrelated databases or roles.

Pause the DAG before teardown. Use `docker compose down` to stop containers without erasing volumes. Stop EC2 when you are not demonstrating it. Before deleting the stack, export required MLflow metadata and artifacts or take an approved EBS backup: the EC2 root EBS volume is deleted on instance termination. S3 is marked **Retain** and must be deliberately cleaned up separately after confirming the files are disposable. The manually created Snowflake IAM role/policy and Snowflake objects are also outside stack deletion.

Production changes would include supported and patched runtime versions, hardened authentication/TLS, a managed MLflow database/artifact store, backups, secret management, CloudWatch alerts, real data validation and out-of-time testing. None of those should be claimed as completed by this POC.
