import argparse
from datetime import datetime
import logging
import os
import pathlib

from google.cloud import batch_v1

from buildstockbatch.aws.aws import DockerBatchBase


logger = logging.getLogger(__name__)


class GCPBatch(DockerBatchBase):
    # https://patorjk.com/software/taag/#p=display&f=Santa%20Clara&t=BuildStockBatch%20%20%2F%20GCP
    LOGO = '''
     _ __         _     __,              _ __                      /     ,___ ,___ _ __
    ( /  )    o  //   /(    _/_       / ( /  )     _/_    /       /     /   //   /( /  )
     /--< , ,,  // __/  `.  /  __ _, /<  /--< __,  /  _, /_      /     /  __/      /--'
    /___/(_/_(_(/_(_/_(___)(__(_)(__/ |_/___/(_/(_(__(__/ /_    /     (___/(___/  /
      Executing BuildStock projects with grace since 2018

'''


    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.job_identifier = self.cfg['gcp']['job_identifier']
        # TODO: stop appending timestamp here - it's useful for testing, but we should probably
        # make users choose a unique job ID each time.
        self.unique_job_id = self.job_identifier + datetime.utcnow().strftime('%y-%m-%d-%H%M%S')
        self.project_filename = project_filename
        self.gcp_project = self.cfg['gcp']['gcp_project']
        self.region = self.cfg['gcp']['region']
        self.bucket = self.cfg['gcp']['gcs']['bucket']
        self.prefix = self.cfg['gcp']['gcs']['prefix']

    @staticmethod
    def validate_gcp_args(project_file):
        # TODO: validate GCP section of project definition, like region, machine type, etc
        pass

    @staticmethod
    def validate_project(project_file):
        super(GCPBatch, GCPBatch).validate_project(project_file)
        GCPBatch.validate_gcp_args(project_file)

    def build_image(self):
        """
        Build the docker image to use in the batch simulation
        """
        # TODO: can this method be in DockerBatchBase? It can be shared with AwsBatch.
        root_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.parent
        if not (root_path / 'Dockerfile').exists():
            raise RuntimeError(f'The needs to be run from the root of the repo, found {root_path}')
        logger.debug('Building docker image')
        self.docker_client.images.build(
            path=str(root_path),
            tag=self.docker_image,
            rm=True
        )

    def push_image(self):
        # TODO: push image to artifact registry
        pass

    def cleanup(self):
        # TODO: clean up all resources used for this project
        pass


    def list_jobs(self, verbose):
        """List existing GCP Batch jobs that match the provided project"""
        client = batch_v1.BatchServiceClient()

        request = batch_v1.ListJobsRequest()
        request.parent = f'projects/{self.gcp_project}/locations/{self.region}'
        request.filter = f'name:{request.parent}/jobs/{self.job_identifier}'
        request.order_by = 'create_time desc'
        print(f'Showing existing jobs that match: {request.filter}\n')
        response = client.list_jobs(request)
        for job in response.jobs:
            if verbose:
                print(job)
            else:

                def format_job(job):
                    s = ['']
                    s.append(f'Name: {job.name}')
                    s.append(f'UID: {job.uid}')
                    s.append(f'Status: {str(job.status.state)}')
                    return '\n  '.join(s)

                print(format_job(job))

        # TODO: this only shows jobs still running in GCP Batch - update it to also
        # show any post-processing steps that may be running.

    def run_batch(self):
        # Run sampling and split up buildings into batches.
        # TODO: run this sampling
        # buildstock_csv_filename = self.sampler.run_sampling()

        # Set up a new GCP Batch job
        client = batch_v1.BatchServiceClient()

        runnable = batch_v1.Runnable()
        # Define what the job should actually do - takes a container or a script
        runnable.container = batch_v1.Runnable.Container()
        # TODO: Use the docker image pushed earlier. busybox is a minimal unix image good for testing
        runnable.container.image_uri = 'gcr.io/google-containers/busybox'
        runnable.container.entrypoint = '/bin/sh'

        # Pass environment variables to each task
        environment = batch_v1.Environment()
        # TODO: What other env vars need to exist for run_job() below?
        environment.variables = {'JOB_ID': self.unique_job_id}
        runnable.environment = environment

        # BATCH_TASK_INDEX and BATCH_TASK_COUNT env vars are automatically made available
        # TODO: Update to run batches of openstudio with something like "python3 -m buildstockbatch.gcp.gcp"
        runnable.container.commands = [
        "-c",
            "mkdir /mnt/disks/share/${JOB_ID}; echo Hello world! This is task ${BATCH_TASK_INDEX}. This job has a total of ${BATCH_TASK_COUNT} tasks. > /mnt/disks/share/${JOB_ID}/output_${BATCH_TASK_INDEX}.txt"
    ]

        # Mount GCS Bucket, so we can use it like a normal directory.
        gcs_bucket = batch_v1.GCS(remote_path=self.bucket)
        gcs_volume = batch_v1.Volume(gcs = gcs_bucket)
        # Note: 'mnt/share/' is read-only, but 'mnt/disks/share' works
        gcs_volume.mount_path = '/mnt/disks/share'


        # TODO: Allow specifying resources from the project YAML file
        resources = batch_v1.ComputeResource()
        resources.cpu_milli = 1000
        resources.memory_mib = 1

        task = batch_v1.TaskSpec(
            # Each task executes this list of runnables
            runnables = [runnable],
            volumes = [gcs_volume],
            compute_resource = resources,
            # TODO: check what happens if this fails (e.g. will it leave behind unwanted files?)
            max_retry_count = 2,
            # TODO: How long does this timeout need to be?
            max_run_duration = '60s'
        )

        group = batch_v1.TaskGroup()
        # How many of these tasks to run.
        group.task_count = 3
        group.task_spec = task

        # Specify types of VMs to run on
        policy = batch_v1.AllocationPolicy.InstancePolicy()
        # TODO: look into best default machine type for running OpenStudio
        # https://cloud.google.com/compute/docs/machine-types
        policy.machine_type = 'e2-standard-2'
        instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
        instances.policy = policy
        allocation_policy = batch_v1.AllocationPolicy()
        allocation_policy.instances = [instances]
        # TODO: Add option to set service account?
        # allocation_policy.service_account = batch_v1.ServiceAccount(email = '')

        # Define the batch job
        job = batch_v1.Job()
        job.task_groups = [group]
        job.allocation_policy = allocation_policy
        # TODO: What labels are useful to include here? (These are from sample code)
        job.labels = {'env': 'testing', 'type': 'script'}
        job.logs_policy = batch_v1.LogsPolicy()
        job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
        # Send notifications to pub/sub when the job's state changes
        # TODO: Turn these into emails is an email address is provided
        job.notifications = [batch_v1.JobNotification(
            pubsub_topic = 'projects/buildstockbatch-dev/topics/notifications',
            message = batch_v1.JobNotification.Message(
                type_ = 1,
                new_job_state='STATE_UNSPECIFIED', # notify on any changes(?)
            )
        )]

        create_request = batch_v1.CreateJobRequest()
        create_request.job = job
        # Add timestamp to job ID, since duplicates aren't allowed (even between finished and new jobs)
        # TODO: Or check for existing job and enforce that the ID provided is unique?
        create_request.job_id = self.unique_job_id
        create_request.parent = f'projects/{self.gcp_project}/locations/{self.region}'

        # Start the job!
        created_job = client.create_job(create_request)

        logger.info(created_job)

        logger.info('Newly created job info:')
        logger.info(f'Job name: {created_job.name}')
        logger.info(f'Job UID: {created_job.uid}')


    @classmethod
    def run_job(self, job_id, bucket, prefix, job_name, region):
        """
        Run a few simulations inside a container.

        This method is called from inside docker container in GCP compute engine.
        It will go get the necessary files from GCS, run the simulation, and write the
        results back to GCS.
        """
        # TODO: move this somewhere else?
        pass



def main():
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'defaultfmt': {
                'format': '%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S'
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'defaultfmt',
                'level': 'DEBUG',
                'stream': 'ext://sys.stdout',
            }
        },
        'loggers': {
            '__main__': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            },
            'buildstockbatch': {
                'level': 'DEBUG',
                'propagate': True,
                'handlers': ['console']
            }
        },
    })
    print(GCPBatch.LOGO)

    # If this var exists, we're inside a single batch task
    # TODO: Would it be cleaner to move this to a main() in another file?
    if 'BATCH_TASK_INDEX' in os.environ:
        job_id = int(os.environ['BATCH_TASK_INDEX'])
        # TODO: pass in these env vars to tasks
        # TODO: shouldn't need bucket, if we mount the directory - just read like a normal file
        bucket = ''
        prefix = ''
        job_name = os.environ['JOB_NAMEk']
        region = ''
        GCPBatch.run_job(job_id, bucket, prefix, job_name, region)
    #   ...run one batch
    # otherwise...

    parser = argparse.ArgumentParser()
    parser.add_argument('project_filename')

    parser.add_argument(
        '-c', '--clean',
        action='store_true',
        help='After the simulation is done, run with --clean to clean up GCP environment'
        # TODO: Clarify whether this will also stop a job that's still running.
    )

    parser.add_argument(
        '--validateonly',
        help='Only validate the project YAML file and references. Nothing is executed',
        action='store_true'
    )

    parser.add_argument(
        '--list_jobs',
        help='List existing jobs',
        action='store_true'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )

    args = parser.parse_args()

    logger.info(f'Validating project: {args.project_filename}')
    GCPBatch.validate_project(os.path.abspath(args.project_filename))
    if args.validateonly:
        return

    batch = GCPBatch(args.project_filename)

    if args.clean:
        logger.info('Cleaning up...')
        # TODO: clean up resources
        # Run `terraform destroy` (But make sure outputs aren't deleted!)
        # Note: cleanup also requires the project input file, and only should only clean
        # up resources from that particular project.
        # TODO: should this also stop the job if it's currently running?
        batch.cleanup()
        return

    if args.list_jobs:
        batch.list_jobs(verbose=args.verbose)
        return

    # batch.build_image()
    # batch.push_image()

    # TODO: set up GCP resources with terraform
    # Pass in args like GCS bucket, region, folder name

    batch.run_batch()

if __name__ == '__main__':
    main()

