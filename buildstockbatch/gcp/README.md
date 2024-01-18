# Buildstock Batch on GCP

![Architecture diagram](/buildstockbatch/gcp/arch.svg)

Buildstock Batch runs on GCP in a few steps:

  * Locally
    - Build a Docker image that includes OpenStudio and BuildStock Batch.
    - Push the Docker image to GCP Artifact Registry.
    - Run sampling, and split the generated buildings + upgrades into batches.
    - Collect all the required input files (including downloading weather files)
      and upload them to a Cloud Storage bucket.
    - Kick off the Batch and Cloud Run jobs (described below), and wait for them to finish.

  * In GCP Batch
    - Run a job on GCP Batch where each task runs one batch of simulations.
      GCP Batch uses the Docker image to run OpenStudio on Compute Engine VMs.
    - Raw output files are written to the bucket in Cloud Storage.

  * In Cloud Run
    - Create and start a Cloud Run job for post-processing steps.
      Also uses the Docker image.
    - Aggregated output files are written to the bucket in Cloud Storage.
