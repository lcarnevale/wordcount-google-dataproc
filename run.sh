#!/bin/bash

# $1 lorenzo-dataproc-template

gsutil cp -r src/* data/* gs://$1
gcloud dataproc workflow-templates create wordcount-workflow \
  --region europe-west1
gcloud dataproc workflow-templates set-managed-cluster wordcount-workflow \
  --cluster-name=wordcount-cluster \
  --region europe-west1 \
  --zone=europe-west1-b \
  --master-machine-type n1-standard-1 \
  --worker-machine-type n1-standard-1 \
  --num-workers 2 \
  --initialization-actions gs://$1/pip-install.sh
gcloud dataproc workflow-templates add-job pyspark gs://$1/wordcount-spark-job.py \
  --step-id wordcount-job \
  --workflow-template wordcount-workflow \
  --region europe-west1 \
  -- nltk numpy
gcloud dataproc workflow-templates instantiate wordcount-workflow \
  --region europe-west1
