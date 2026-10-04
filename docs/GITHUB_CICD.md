# GitHub and CI/CD — POC setup

## What each tool does

Git records code changes. GitHub stores the repository. GitHub Actions runs the automated checks and deployment workflow. Amazon ECR stores Docker images. AWS Systems Manager (SSM) delivers deployment commands to EC2. Airflow continues to schedule data ingestion, preparation, training and scoring after deployment.

**CI/CD deploys code. Airflow runs data jobs. MLflow versions trained models.** A Git push does not upload the dataset or automatically promote a newly trained model. Airflow's existing `TRAIN_MODEL` setting controls retraining.

| Event | Automated result |
|---|---|
| Pull request targeting `main` | Python checks, tests, Docker build, Airflow DAG parsing, container startup and a 5,000-customer local integration test |
| Push / merge into `main` | Same CI checks |
| Successful `main` CI with `ENABLE_DEPLOY=true` | Transfer the exact tested image, push it to ECR, deploy by digest to EC2 using SSM |
| Failed CI | Deployment does not start |
| Active Airflow run during deployment | Leave the previous release in place, restore the prior pause state, and fail CD clearly; rerun CD after the data job finishes |
| Candidate image or DAG fails deployment checks | Attempt to restore the prior image and Compose configuration |

No AWS credentials are supplied to PR checks. AWS deployment uses OIDC temporary credentials. The included trust template uses GitHub's standard environment subject; if your organization customizes OIDC subjects, use its actual configured claim instead.

## 1. Put the project in a private GitHub repository

If ChatGPT has repository access, it can publish once the target repository is identified. Otherwise these commands accomplish the same initial setup from your VS Code terminal.

1. In GitHub, create a **private** repository named `credit-card-churn-poc`. Leave README, .gitignore and license initialization unchecked because the project already has files.
2. Open the extracted project folder containing `compose.yaml` and `.github/` in VS Code.
3. Run:

```powershell
git init
git branch -M main
git add .
git status --short
```

Check the staged list: `.env`, private keys, `secrets/` contents and full datasets must not appear. `.env.example` and `secrets/.gitkeep` are intentional templates/placeholders.

```powershell
git commit -m "Add credit card churn POC with CI/CD"
git remote add origin https://github.com/YOUR_OWNER/credit-card-churn-poc.git
git push -u origin main
```

Use Git Credential Manager's browser sign-in if prompted. Do not paste passwords or access tokens into files or chat. If Git asks for your author identity, configure your own name and verified GitHub email; these are commit metadata, not account credentials. If `origin` already exists, inspect `git remote -v` instead of adding a duplicate.

Open the repository's **Actions** tab. The `Churn CI and CD` workflow should start. Deployment stays disabled until you configure it. A CI run uses GitHub-hosted runner minutes and temporarily stores a large Docker artifact when deployment is enabled; allow build/download time and monitor your account allowance.

## 2. Protect the deployment branch

After the first CI run, set a branch rule/ruleset for `main`: require a pull request and the `verify` status check before merging. Avoid direct pushes to main. Available branch/environment protection features depend on your GitHub plan and repository visibility; verify which controls are available for your private repository. Do not assume workflow YAML itself blocks a user from pushing directly.

A routine change then looks like:

```powershell
git switch -c feature/update-churn-features
# Edit code in VS Code.
git add churn/features.py
git commit -m "Update churn feature calculation"
git push -u origin feature/update-churn-features
```

Open a pull request to main, inspect the CI result and diff, then merge. Local file edits alone do not run GitHub Actions: the changes must be committed and pushed.

## 3. Prepare AWS once

Complete `AWS_AND_SNOWFLAKE.md` and a small test on the EC2 host. The updated CloudFormation template also creates a private ECR repository and grants the EC2 role permission to pull it. Record its new `EcrRepositoryName` output. If you provisioned with the earlier template, update the existing stack using this template and inspect the change set first; do not create a duplicate stack.

The deployed project path must be `/home/ec2-user/credit-card-churn-poc`, matching the original extraction instructions. Its `.env`, encrypted Snowflake private key and Docker volumes stay on the host. Start services there once and confirm the DAG is visible before enabling CD. CD updates an initialized installation; it does not provision resources or fill in account credentials.

Confirm the EC2 instance appears online in AWS Systems Manager managed nodes. Amazon Linux 2023 normally includes SSM Agent and AWS CLI; check `aws --version` and `sudo systemctl status amazon-ssm-agent` on the host and install/enable them through your administrator if absent. The host needs outbound AWS API access.

## 4. Configure GitHub-to-AWS authentication

1. AWS IAM → Identity providers: add an **OpenID Connect** provider `https://token.actions.githubusercontent.com`, audience `sts.amazonaws.com`, if this provider does not already exist in your account.
2. Create an IAM role named `ChurnGitHubDeployRole`. Use `aws/github-oidc-trust.json`, replacing account ID, GitHub owner and repository name exactly. The trusted environment is `poc-aws`.
3. Attach an inline policy using `aws/github-deploy-policy.json`. Replace account ID, region, ECR repository, S3 bucket and the one EC2 instance ID. It can push that image, write deployment bundles, and send commands to that host.
4. Record the role ARN. Do not create IAM access keys for GitHub Actions.
5. In GitHub → Settings → Environments, create **`poc-aws`**. Restrict deployment branches to **`main` only**. This matters because the environment-based OIDC subject does not itself encode the branch. If your plan cannot enforce that restriction, do not enable this environment trust unchanged; have your administrator use a branch-bound OIDC design.

SSM shell access can administer the target host, so limit repository write access and review main-branch changes. Optional required environment reviewers turn deployment into a deliberate approval step; omit that optional gate for fully automatic POC deployment after merge.

## 5. Add GitHub Actions variables

Under repository **Settings → Secrets and variables → Actions → Variables**, add:

| Variable | Value |
|---|---|
| `ENABLE_DEPLOY` | Start with `false`; change to `true` only after the following values and host are ready |
| `AWS_REGION` | For example, `ap-south-1` |
| `AWS_DEPLOY_ROLE_ARN` | ARN of `ChurnGitHubDeployRole` |
| `ECR_REPOSITORY` | `EcrRepositoryName` from stack outputs |
| `DEPLOY_BUCKET` | Stack's `BucketName` |
| `EC2_INSTANCE_ID` | Stack's `InstanceId` |

Use **repository variables** for these values so both jobs see `ENABLE_DEPLOY`. They are identifiers/settings, not passwords. Database/Snowflake passwords and private keys remain in the host configuration and are never included in the workflow artifact.

## 6. Enable and demonstrate automatic deployment

After CI is green and AWS is configured, set `ENABLE_DEPLOY=true`. Changing a variable does not itself start a workflow. Push a small documented change through a PR and merge it. In Actions, watch `verify` followed by `deploy`.

The verify job exports its tested Docker image. The deploy job loads that same image rather than rebuilding it, tags it with commit SHA + workflow run/attempt, pushes to ECR, and deploys its immutable digest. The image records the code commit; future MLflow training runs log that commit as a tag. Deployment checks Airflow/MLflow runtime versions, pauses the DAG briefly, refuses to replace an active run, switches images, verifies DAG imports and service health, and restores the DAG's previous pause state. It does not trigger a 2-million-row data job on each code change.

Never manually trigger a data run while a deployment is updating the host. Concurrent deployments are serialized. A superseded commit is rejected before publishing so an older queued workflow does not overwrite newer main code. GitHub concurrency is not a FIFO deployment queue; the newest pending deployment can replace an older pending one.

Runtime upgrades to Airflow/MLflow are deliberately rejected by the deployment script; they require a planned database migration and backup. The application rollback cannot undo database migrations or external side effects. If rollback fails, inspect the SSM command and restore the prior digest from `.deployed-image` with the host administrator.

## Scope and validation

Application Python code, DAGs, Docker configuration, SQL templates, infrastructure templates, tests and documentation belong in GitHub. Data belongs in Snowflake/S3, models in MLflow and passwords in protected runtime settings. CI/CD automatically tests/deploys application changes. Infrastructure/SQL templates are version-controlled and syntax-checked, but schema/IAM/CloudFormation changes still need a reviewed one-time application; this workflow does not run arbitrary SQL or change infrastructure on every push.

The workflow and deployment scripts were prepared and syntax/configuration-checked locally. No GitHub-hosted workflow, ECR push or SSM deployment has been claimed as executed. The first GitHub CI run must validate the Docker build, which could not be run in the creation environment.

Official references:

- https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-aws
- https://github.com/aws-actions/amazon-ecr-login
- https://docs.aws.amazon.com/systems-manager/latest/userguide/walkthrough-cli.html
