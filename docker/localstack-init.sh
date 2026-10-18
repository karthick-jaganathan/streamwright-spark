#!/bin/sh
# Runs inside LocalStack when it is ready: buckets and a queue for the test environment.
set -eu
awslocal s3 mb s3://acme-landing
awslocal s3 mb s3://acme-lake
awslocal sqs create-queue --queue-name file-arrived
