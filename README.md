# ECS Event Fetcher

A simple process that periodically fetches events from ECS Services and writes them to AWS CloudWatch Logs. This is
intended to solve the problem only being able to access the last 100 events through the ECS API. 

### Config
The following environment variables can be set:

- `ACCESS_KEY` (required)
- `SECRET_KEY` (required)
- `REGION`     (required)
- `LOG_LEVEL`
- `CLOUDWATCH_LOG_GROUP`
- `POLL_INTERVAL`
- `API_REQUEST_SPACING`
- `SDB_DOMAIN`

Uses SDB to persist pointers about the last event sent to CloudWatch.

### Running 
Designed to be run in a docker container, see included Dockerfile. Run `docker build .` inside the source directory to 
create container.

### IAM Permissions

Below is an example IAM policy that can be used for this tool.  


```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1468260268000",
            "Effect": "Allow",
            "Action": [
                "ecs:ListClusters",
                "ecs:ListServices",
                "ecs:DescribeServices"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Sid": "Stmt1468259781463",
            "Action": "sdb:*",
            "Effect": "Allow",
            "Resource": "arn:aws:sdb:us-west-2:[account number]:domain/ecs_service_events"
        },
        {
            "Action": [
                "logs:*"
            ],
            "Effect": "Allow",
            "Resource": "*"
        }
    ]
}
```
