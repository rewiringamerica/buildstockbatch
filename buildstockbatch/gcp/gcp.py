# -*- coding: utf-8 -*-

"""
buildstockbatch.gcp
~~~~~~~~~~~~~~~
This class contains the object & methods that allow for usage of the library with GCP Batch.

This implementation tries to match the structure of `../aws/aws.py` in the 'aws-batch' branch as
much as possible in order to make it easier to refactor these two (or three, with Eagle) to share
code later. Also, because that branch has not yet been merged, this will also _not_ do any
refactoring right now to share code with that (to reduce merging complexity). Instead, code that's
likely to be refactored out will be commented with 'todo: aws-shared'.

:author: Robert LaThanh
:copyright: (c) 2023 by The Alliance for Sustainable Energy
:license: BSD-3
"""
import argparse
import collections
from datetime import datetime
import docker
import gzip
import hashlib
from joblib import Parallel, delayed
import json
import itertools
import logging
import math
import os
import pathlib
import random
import re
import shutil
import tarfile
import tempfile
import time

from google.cloud import batch_v1, storage
from google.cloud.storage import transfer_manager

from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.utils import ContainerRuntime, log_error_details, read_csv


logger = logging.getLogger(__name__)


# todo: aws-shared (see file comment)
class DockerBatchBase(BuildStockBatchBase):
    CONTAINER_RUNTIME = ContainerRuntime.DOCKER

    def __init__(self, project_filename):
        super().__init__(project_filename)

        return
        self.docker_client = docker.DockerClient.from_env()
        try:
            self.docker_client.ping()
        except:  # noqa: E722 (allow bare except in this case because error can be a weird non-class Windows API error)
            logger.error('The docker server did not respond, make sure Docker Desktop is started then retry.')
            raise RuntimeError('The docker server did not respond, make sure Docker Desktop is started then retry.')

    @staticmethod
    def validate_project(project_file):
        super(DockerBatchBase, DockerBatchBase).validate_project(project_file)

    @property
    def docker_image(self):
        return f"nrel/openstudio:{self.os_version}"


def upload_directory_to_GCS(local_directory, bucket, prefix):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    # blob = bucket.blob(self.prefix)

    local_dir_abs = pathlib.Path(local_directory).absolute()

    def filename_generator():
        for dirpath, dirnames, filenames in os.walk(local_dir_abs):
            for filename in filenames:
                if filename.startswith('.'):
                    continue
                local_filepath = pathlib.Path(dirpath, filename)
                # gcs_path = pathlib.PurePosixPath(
                #   prefix,
                #   local_filepath.relative_to(local_dir_abs)
                # )
                # yield local_filepath, s3_key
                # yield str(gcs_path)
                yield str(local_filepath.relative_to(local_dir_abs))

    string_paths = list(filename_generator())

    # https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    results = transfer_manager.upload_many_from_filenames(
        bucket, string_paths, source_directory=local_dir_abs, blob_name_prefix=prefix
    )
    # TODO: check for errors in results


# todo: aws-shared (see file comment)
def compress_file(in_filename, out_filename):
    with gzip.open(str(out_filename), 'wb') as f_out:
        with open(str(in_filename), 'rb') as f_in:
            shutil.copyfileobj(f_in, f_out)


# todo: aws-shared (see file comment)
def calc_hash_for_file(filename):
    with open(filename, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()


def copy_GCS_file(src_bucket, src_key, dest_bucket, dest_key):
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(src_bucket)
    source_blob = source_bucket.blob(src_key)
    destination_bucket = storage_client.bucket(dest_bucket)
    source_bucket.copy_blob(source_blob, destination_bucket, dest_key)


class GcpBatch(DockerBatchBase):
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

        # TODO: how short does this really need to be? What characters are allowed?
        self.job_identifier = re.sub('[^0-9a-zA-Z-]+', '_', self.cfg['gcp']['job_identifier'])[:10]

        self.project_filename = project_filename
        self.gcp_project = self.cfg['gcp']['project']
        self.region = self.cfg['gcp']['region']
        self.ar_repo = self.cfg['gcp']['artifact_registry']['repository']
        self.gcs_bucket = self.cfg['gcp']['gcs']['bucket']
        self.gcs_prefix = self.cfg['gcp']['gcs']['prefix']
        self.use_spot = self.cfg['gcp']['use_spot']

        # Add timestamp to job ID, since duplicates aren't allowed (even between finished and new jobs)
        # TODO: stop appending timestamp here - it's useful for testing, but we should probably
        # make users choose a unique job ID each time.
        self.unique_job_id = self.job_identifier + datetime.utcnow().strftime('%y-%m-%d-%H%M%S')

    @property
    def weather_dir(self):
        return self._weather_dir

    @staticmethod
    def validate_gcp_args(project_file):
        # TODO: validate GCP section of project definition, like region, machine type, etc
        pass

    @staticmethod
    def validate_project(project_file):
        return  # todo-xxx- to be (re)implemented
        super(GcpBatch, GcpBatch).validate_project(project_file)
        GcpBatch.validate_dask_settings(project_file)
        GcpBatch.validate_gcp_args(project_file)

    @property
    def docker_image(self):
        return 'nrel/buildstockbatch'

    @property
    def registry_url(self):
        """
        The registry that the image(s) will be pushed to.

        :returns: A string of a GCP Artifact Repository URL; for example,
            `https://us-central1-docker.pkg.dev
        """
        return f"https://{self.repository_uri.split('/')[0]}"

    @property
    def repository_uri(self):
        """
        The "repository" (name) for this image for pushing to a GCP Artifact
        Repository.

        :returns: A string for this image given the Artifact Repository (given
            its region, project name, and repo name), followed by
            "buildstockbatch"; for example,
             `us-central1-docker.pkg.dev/buildstockbatch/buildstockbatch-docker/buildstockbatch`
        """
        return f"{self.region}-docker.pkg.dev/{self.gcp_project}/{self.ar_repo}/buildstockbatch"

    # todo: aws-shared (see file comment)
    def build_image(self):
        """
        Build the docker image to use in the batch simulation
        """
        root_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.parent
        if not (root_path / 'Dockerfile').exists():
            raise RuntimeError(f'The needs to be run from the root of the repo, found {root_path}')

        # Make the buildstock/resources/.gcp_docker_image dir to store logs
        local_log_dir = os.path.join(self.buildstock_dir, 'resources', '.gcp_docker_image')
        if not os.path.exists(local_log_dir):
            os.makedirs(local_log_dir)

        # Determine whether or not to build the image with custom gems bundled in
        if self.cfg.get('baseline', dict()).get('custom_gems', False):
            # Ensure the custom Gemfile exists in the buildstock dir
            local_gemfile_path = os.path.join(self.buildstock_dir, 'resources', 'Gemfile')
            if not os.path.exists(local_gemfile_path):
                raise AttributeError(f'baseline:custom_gems = True, but did not find Gemfile at {local_gemfile_path}')

            # Copy the custom Gemfile into the buildstockbatch repo
            bsb_root = os.path.join(os.path.abspath(__file__), os.pardir, os.pardir, os.pardir)
            new_gemfile_path = os.path.join(bsb_root, 'Gemfile')
            shutil.copyfile(local_gemfile_path, new_gemfile_path)
            logger.info(f'Copying custom Gemfile from {local_gemfile_path}')

            # Choose the custom-gems stage in the Dockerfile,
            # which runs bundle install to build custom gems into the image
            stage = 'buildstockbatch-custom-gems'
        else:
            # Choose the base stage in the Dockerfile,
            # which stops before bundling custom gems into the image
            stage = 'buildstockbatch'

        logger.info(f'Building docker image stage: {stage} from OpenStudio {self.os_version}')
        img, build_logs = self.docker_client.images.build(
            path=str(root_path),
            tag=self.docker_image,
            rm=True,
            target=stage,
            platform="linux/amd64",
            buildargs={'OS_VER': self.os_version},
        )
        build_image_log = os.path.join(local_log_dir, 'build_image.log')
        with open(build_image_log, 'w') as f_out:
            f_out.write('Built image')
            for line in build_logs:
                for itm_type, item_msg in line.items():
                    if itm_type in ['stream', 'status']:
                        try:
                            f_out.write(f'{item_msg}')
                        except UnicodeEncodeError:
                            pass
        logger.debug(f'Review docker image build log: {build_image_log}')

        # Report and confirm the openstudio version from the image
        os_ver_cmd = 'openstudio openstudio_version'
        container_output = self.docker_client.containers.run(
            self.docker_image, os_ver_cmd, remove=True, name='list_openstudio_version'
        )
        assert self.os_version in container_output.decode()

        # Report gems included in the docker image.
        # The OpenStudio Docker image installs the default gems
        # to /var/oscli/gems, and the custom docker image
        # overwrites these with the custom gems.
        list_gems_cmd = (
            'openstudio --bundle /var/oscli/Gemfile --bundle_path /var/oscli/gems '
            '--bundle_without native_ext gem_list'
        )
        container_output = self.docker_client.containers.run(
            self.docker_image, list_gems_cmd, remove=True, name='list_gems'
        )
        gem_list_log = os.path.join(local_log_dir, 'openstudio_gem_list_output.log')
        with open(gem_list_log, 'wb') as f_out:
            f_out.write(container_output)
        for line in container_output.decode().split('\n'):
            logger.debug(line)
        logger.debug(f'Review custom gems list at: {gem_list_log}')

    def push_image(self):
        """
        Push the locally built docker image to the GCP Artifact Repository (AR).
        """

        # Log the Docker client into the GCP AR registry
        service_account_key_file = open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], "r")
        service_account_key = service_account_key_file.read()
        docker_client_login_response = self.docker_client.login(
            username="_json_key", password=service_account_key, registry=self.registry_url
        )
        logger.debug(docker_client_login_response)

        # Tag the image with a repo name for pushing to GCP AR
        image = self.docker_client.images.get(self.docker_image)
        repo_uri = self.repository_uri
        image.tag(repo_uri, tag=self.job_identifier)

        # Push to the GCP AR
        last_status = None
        for x in self.docker_client.images.push(repo_uri, tag=self.job_identifier, stream=True):
            try:
                y = json.loads(x)
            except json.JSONDecodeError:
                continue
            else:
                if y.get('status') is not None and y.get('status') != last_status:
                    logger.debug(y['status'])
                    last_status = y['status']

    def clean(self):
        # TODO: clean up all resources used for this project
        pass

    def list_jobs(self):
        """
        List existing GCP Batch jobs that match the provided project.
        """
        # TODO: this only shows jobs that exist in GCP Batch - update it to also
        # show any post-processing steps that may be running.
        client = batch_v1.BatchServiceClient()

        request = batch_v1.ListJobsRequest()
        request.parent = f'projects/{self.gcp_project}/locations/{self.region}'
        request.filter = f'name:{request.parent}/jobs/{self.job_identifier}'
        request.order_by = 'create_time desc'
        logger.info(f'Showing existing jobs that match: {request.filter}\n')
        response = client.list_jobs(request)
        for job in response.jobs:
            logger.debug(job)

            logger.info(f'Name: {job.name}')
            logger.info(f'UID: {job.uid}')
            logger.info(f'Status: {str(job.status.state)}')

    def run_batch(self):
        """
        Start the GCP Batch job to run all the building simulations.

        This will
            - perform the sampling
            - package and upload the assets, including weather
            - kick off a batch simulation on GCP
        """
        # Step 1: Run sampling and split up buildings into batches.
        # TDOO: Generate buildstock.csv
        # buildstock_csv_filename = self.sampler.run_sampling()
        # Hardcoded file for testing before docker works locally...
        buildstock_csv_filename = (
            '/Users/nweires/RewiringAmerica/resstock/project_testing/housing_characteristics/buildstock.csv'
        )

        # Step 2: Upload weather data and any other required files to GCP
        # Compress and upload assets to S3
        # todo: aws-shared (see file comment)
        # Most of this function is identical to the AWS version
        with tempfile.TemporaryDirectory(prefix='bsb_') as tmpdir, tempfile.TemporaryDirectory(
            prefix='bsb_'
        ) as tmp_weather_dir:  # noqa: E501
            self._weather_dir = tmp_weather_dir
            self._get_weather_files()
            tmppath = pathlib.Path(tmpdir)
            logger.debug('Creating assets tarfile')
            with tarfile.open(tmppath / 'assets.tar.gz', 'x:gz') as tar_f:
                project_path = pathlib.Path(self.project_dir)
                buildstock_path = pathlib.Path(self.buildstock_dir)
                tar_f.add(buildstock_path / 'measures', 'measures')
                if os.path.exists(buildstock_path / 'resources/hpxml-measures'):
                    tar_f.add(buildstock_path / 'resources/hpxml-measures', 'resources/hpxml-measures')
                tar_f.add(buildstock_path / 'resources', 'lib/resources')
                tar_f.add(project_path / 'housing_characteristics', 'lib/housing_characteristics')

            # Weather files
            weather_path = tmppath / 'weather'
            os.makedirs(weather_path)

            # Determine the unique weather files
            epw_filenames = list(filter(lambda x: x.endswith('.epw'), os.listdir(self.weather_dir)))
            logger.debug('Calculating hashes for weather files')
            epw_hashes = Parallel(n_jobs=-1, verbose=9)(
                delayed(calc_hash_for_file)(pathlib.Path(self.weather_dir) / epw_filename)
                for epw_filename in epw_filenames
            )
            unique_epws = collections.defaultdict(list)
            for epw_filename, epw_hash in zip(epw_filenames, epw_hashes):
                unique_epws[epw_hash].append(epw_filename)

            # Compress unique weather files
            logger.debug('Compressing weather files')
            Parallel(n_jobs=-1, verbose=9)(
                delayed(compress_file)(pathlib.Path(self.weather_dir) / x[0], str(weather_path / x[0]) + '.gz')
                for x in list(unique_epws.values())[0:3]
            )

            logger.debug('Writing project configuration for upload')
            with open(tmppath / 'config.json', 'wt', encoding='utf-8') as f:
                json.dump(self.cfg, f)

            # Collect simulations to queue
            df = read_csv(buildstock_csv_filename, index_col=0, dtype=str)
            self.validate_buildstock_csv(self.project_filename, df)
            building_ids = df.index.tolist()
            n_datapoints = len(building_ids)
            n_sims = n_datapoints * (len(self.cfg.get('upgrades', [])) + 1)
            logger.debug('Total number of simulations = {}'.format(n_sims))

            # This is the maximum number of jobs that can be in an array
            # TODO: GCP limit is 100,000 tasks - https://cloud.google.com/batch/quotas
            if self.batch_array_size <= 10000:
                max_array_size = self.batch_array_size
            else:
                max_array_size = 10000
            n_sims_per_job = math.ceil(n_sims / max_array_size)
            n_sims_per_job = max(n_sims_per_job, 2)
            logger.debug('Number of simulations per array job = {}'.format(n_sims_per_job))

            # Create list of (building ID, upgrade to apply) pairs for all simulations to run, and shuffle them.
            baseline_sims = zip(building_ids, itertools.repeat(None))
            upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get('upgrades', []))))
            all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
            random.shuffle(all_sims)
            all_sims_iter = iter(all_sims)

            os.makedirs(tmppath / 'jobs')

            # Write each batch of jobs to a file.
            logger.info('Queueing jobs')
            for i in itertools.count(0):
                batch = list(itertools.islice(all_sims_iter, n_sims_per_job))
                if not batch:
                    break
                job_json_filename = tmppath / 'jobs' / 'job{:05d}.json'.format(i)
                with open(job_json_filename, 'w') as f:
                    json.dump(
                        {
                            'job_num': i,
                            'n_datapoints': n_datapoints,
                            'batch': batch,
                        },
                        f,
                        indent=4,
                    )
            # array_size here is number of batches we've ended up with
            array_size = i
            logger.debug('Array size = {}'.format(array_size))

            # Compress job jsons
            jobs_dir = tmppath / 'jobs'
            logger.debug('Compressing job jsons using gz')
            tick = time.time()
            with tarfile.open(tmppath / 'jobs.tar.gz', 'w:gz') as tf:
                tf.add(jobs_dir, arcname='jobs')
            tick = time.time() - tick
            logger.debug('Done compressing job jsons using gz {:.1f} seconds'.format(tick))
            shutil.rmtree(jobs_dir)

            os.makedirs(tmppath / 'results' / 'simulation_output')

            logger.debug(f'Uploading files to GCS bucket: {self.bucket}')
            # TODO: check for '/' at end of prefix
            upload_directory_to_GCS(tmppath, self.bucket, self.prefix + '/')

        # Copy the non-unique weather files on GCS
        epws_to_copy = []
        for epws in unique_epws.values():
            # The first in the list is already up there, copy the rest
            for filename in epws[1:]:
                epws_to_copy.append((f"{self.prefix}/weather/{epws[0]}.gz", f"{self.prefix}/weather/{filename}.gz"))

        logger.debug('Copying weather files on GCS')
        bucket = self.bucket
        breakpoint()
        Parallel(n_jobs=-1, verbose=9)(
            delayed(copy_GCS_file)(bucket, src, bucket, dest) for src, dest in epws_to_copy[0:3]
        )

        # Create the output directories
        # Note: not technically required by GCP...
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket)
        for upgrade_id in range(len(self.cfg.get('upgrades', [])) + 1):
            dir_name = f'{self.prefix}/results/simulation_output/timeseries/up{upgrade_id:02d}/'
            blob = bucket.blob(dir_name)
            # Note: GCS doesn't have real directories, so these are just empty files with names ending in '/'.
            blob.upload_from_string('')

        breakpoint()

        # Step 3: Define and run the GCP Batch job.
        client = batch_v1.BatchServiceClient()

        runnable = batch_v1.Runnable()
        runnable.container = batch_v1.Runnable.Container()
        # TODO: Use the docker image pushed earlier instead of this hardcoded image.
        runnable.container.image_uri = (
            'us-central1-docker.pkg.dev/buildstockbatch-dev/buildstockbatch-docker/buildstockbatch'
        )
        runnable.container.entrypoint = '/bin/sh'

        # Pass environment variables to each task
        environment = batch_v1.Environment()
        # TODO: What other env vars need to exist for run_job() below?
        # BATCH_TASK_INDEX and BATCH_TASK_COUNT env vars are automatically made available.
        environment.variables = {'JOB_ID': self.unique_job_id}
        runnable.environment = environment

        # TODO: Update to run batches of openstudio with something like "python3 -m buildstockbatch.gcp.gcp"
        runnable.container.commands = [
            "-c",
            # Test script that checks whether openstudio is installed and writes to a file in GCS.
            "mkdir /mnt/disks/share/${JOB_ID}; "
            "openstudio --help > /mnt/disks/share/${JOB_ID}/output_${BATCH_TASK_INDEX}.txt",
        ]

        # Mount GCS Bucket, so we can use it like a normal directory.
        gcs_bucket = batch_v1.GCS(remote_path=self.bucket)
        # Note: 'mnt/share/' is read-only, but 'mnt/disks/share' works
        gcs_volume = batch_v1.Volume(
            gcs=gcs_bucket,
            mount_path='/mnt/disks/share',
        )

        # TODO: Allow specifying resources from the project YAML file, plus pick better defaults.
        resources = batch_v1.ComputeResource(
            cpu_milli=1000,
            memory_mib=1,
        )

        task = batch_v1.TaskSpec(
            runnables=[runnable],
            volumes=[gcs_volume],
            compute_resource=resources,
            # TODO: Confirm what happens if this fails repeatedly, or for only some tasks, and document it.
            max_retry_count=2,
            # TODO: How long does this timeout need to be?
            max_run_duration='60s',
        )

        # How many of these tasks to run.
        # TODO: determine this count above, when splitting up samples.
        task_count = 3
        group = batch_v1.TaskGroup(
            task_count=task_count,
            task_spec=task,
        )

        # Specify types of VMs to run on
        # TODO: look into best default machine type for running OpenStudio, but also allow
        # changing via the project config. https://cloud.google.com/compute/docs/machine-types
        policy = batch_v1.AllocationPolicy.InstancePolicy(
            machine_type='e2-standard-2',
        )
        instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate(policy=policy)
        allocation_policy = batch_v1.AllocationPolicy(instances=[instances])
        # TODO: Add option to set service account that runs the job?
        # Otherwise uses the project's default compute engine service account.
        # allocation_policy.service_account = batch_v1.ServiceAccount(email = '')

        # Define the batch job
        job = batch_v1.Job()
        job.task_groups = [group]
        job.allocation_policy = allocation_policy
        # TODO: What (if any) labels are useful to include here? (These are from sample code)
        job.labels = {'env': 'testing'}
        job.logs_policy = batch_v1.LogsPolicy()
        job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

        # Send notifications to pub/sub when the job's state changes
        # TODO: Turn these into emails if an email address is provided?
        job.notifications = [
            batch_v1.JobNotification(
                # TODO: Get topic from config or create a new one as needed (via terraform)
                pubsub_topic='projects/buildstockbatch-dev/topics/notifications',
                message=batch_v1.JobNotification.Message(
                    type_=1,
                    new_job_state='STATE_UNSPECIFIED',  # notify on any changes(?)
                ),
            )
        ]

        create_request = batch_v1.CreateJobRequest()
        create_request.job = job
        create_request.job_id = self.unique_job_id
        create_request.parent = f'projects/{self.gcp_project}/locations/{self.region}'

        # Start the job!
        return
        created_job = client.create_job(create_request)

        logger.debug(created_job)
        logger.info('Newly created GCP Batch job')
        logger.info(f'Job name: {created_job.name}')
        logger.info(f'Job UID: {created_job.uid}')

    @classmethod
    def run_job(self, job_id, bucket, prefix, job_name, region):
        """
        Run a few simulations inside a container.

        This method is called from inside docker container in GCP compute engine.
        It will read the necessary files from GCS, run the simulation, and write the
        results back to GCS.
        """
        pass


@log_error_details()
def main():
    logging.config.dictConfig(
        {
            'version': 1,
            'disable_existing_loggers': True,
            'formatters': {
                'defaultfmt': {
                    'format': '%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    'datefmt': '%Y-%m-%d %H:%M:%S',
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
                '__main__': {'level': 'DEBUG', 'propagate': True, 'handlers': ['console']},
                'buildstockbatch': {'level': 'DEBUG', 'propagate': True, 'handlers': ['console']},
            },
        }
    )
    print(GcpBatch.LOGO)
    if 'BATCH_TASK_INDEX' in os.environ:
        # If this var exists, we're inside a single batch task.
        job_id = int(os.environ['BATCH_TASK_INDEX'])
        # TODO: pass in these env vars to tasks as needed. (Do we really need bucket here?)
        gcs_bucket = os.environ['GCS_BUCKET']
        gcs_prefix = os.environ['GCS_PREFIX']
        job_name = os.environ['JOB_NAME']
        region = os.environ['REGION']
        GcpBatch.run_job(job_id, gcs_bucket, gcs_prefix, job_name, region)
    else:
        parser = argparse.ArgumentParser()
        parser.add_argument('project_filename')
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            '-c',
            '--clean',
            action='store_true',
            help='After the simulation is done, run with --clean to clean up GCP environment'
            # TODO: Clarify whether this will also stop a job that's still running.
        )
        group.add_argument(
            '--validateonly',
            help='Only validate the project YAML file and references. Nothing is executed',
            action='store_true',
        )
        parser.add_argument('--list_jobs', help='List existing jobs', action='store_true')
        parser.add_argument(
            '-v',
            '--verbose',
            action='store_true',
            help='Verbose output - includes DEBUG logs if set',
        )
        parser.add_argument('--list_jobs', help='List existing jobs', action='store_true')
        parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
        group.add_argument(
            '--postprocessonly',
            help='Only do postprocessing, useful for when the simulations are already done',
            action='store_true',
        )
        group.add_argument(
            '--crawl',
            help='Only do the crawling in Athena. When simulations and postprocessing are done.',
            action='store_true',
        )
        args = parser.parse_args()

        if args.verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        # validate the project, and if --validateonly flag is set, return True if validation passes
        GcpBatch.validate_project(os.path.abspath(args.project_filename))
        if args.validateonly:
            return True

        batch = GcpBatch(args.project_filename)
        if args.clean:
            # TODO: clean up resources
            # Run `terraform destroy` (But make sure outputs aren't deleted!)
            # Note: cleanup also requires the project input file, and only should only clean
            # up resources from that particular project.
            # TODO: should this also stop the job if it's currently running?
            batch.clean()
            return
        if args.list_jobs:
            batch.list_jobs()
            return
        elif args.postprocessonly:
            batch.build_image()
            batch.push_image()
            # todo-xxx- to be (re)implemented
            raise NotImplementedError("Not yet (re)implemented past this point")
            batch.process_results()
        elif args.crawl:
            # todo-xxx- to be (re)implemented
            raise NotImplementedError("Not yet (re)implemented past this point")
            batch.process_results(skip_combine=True, use_dask_cluster=False)
        else:
            # batch.build_image()
            # batch.push_image()
            batch.run_batch()
            # todo-xxx- to be (re)implemented
            raise NotImplementedError("Not yet (re)implemented past this point")
            batch.process_results()
            batch.clean()


if __name__ == '__main__':
    main()
