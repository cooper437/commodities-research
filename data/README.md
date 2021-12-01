## Data

#### Overview
Raw and processed data for analysis and automation.

#### Major Dependencies
+ [DVC](https://dvc.org/doc/install) - DVC is a lightweight way of version controlling data files. We use an AWS S3 remote to store these files and then git to track the versioning and changes
+ [AWS CLI](https://aws.amazon.com/cli/) - DVC uses some AWS configuration to authenticate and connect to S3

#### Environment Setup
A few things need to one time to get DVC correctly setup
1. Setup your AWS CLI. You will know its working when you the trading-strategies-data-dvc s3 bucket can be queried.
```
$ aws s3 ls
2021-11-30 09:13:54 trading-strategies-data-dvc
```
2. Setup your dvc remote
3. Pull dvc data for your current branch (see best practice section below)

#### Best Practice
After installing DVC it is important to keep the data in your environement up to date with the particular commit you are on. This can be done quite simply with the following two commands.

```
# Pull the latest commits on your branch from the remote
$ git pull

# Pull the latest data from the DVC remote associated with current commit
$ dvc pull
```