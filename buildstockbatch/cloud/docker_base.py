# -*- coding: utf-8 -*-

"""
buildstockbatch.docker_base
~~~~~~~~~~~~~~~
This is the base class mixed into classes that deploy using a docker container.

:author: Natalie Weires
:license: BSD-3
"""
import collections
import docker
import itertools
from joblib import Parallel, delayed
import json
import logging
import math
import os
import pathlib
import random
import shutil
import tarfile
import tempfile
import time

from buildstockbatch.base import BuildStockBatchBase
from buildstockbatch.utils import ContainerRuntime, calc_hash_for_file, compress_file, read_csv

logger = logging.getLogger(__name__)


class DockerBatchBase(BuildStockBatchBase):
    """Base class for implementations that run in Docker containers."""

    CONTAINER_RUNTIME = ContainerRuntime.DOCKER
    MAX_JOB_COUNT = 10000

    def __init__(self, project_filename):
        super().__init__(project_filename)

        self.docker_client = docker.DockerClient.from_env()
        try:
            self.docker_client.ping()
        except:  # noqa: E722 (allow bare except in this case because error can be a weird non-class Windows API error)
            logger.error("The docker server did not respond, make sure Docker Desktop is started then retry.")
            raise RuntimeError("The docker server did not respond, make sure Docker Desktop is started then retry.")

    @staticmethod
    def validate_project(project_file):
        super(DockerBatchBase, DockerBatchBase).validate_project(project_file)

    @property
    def docker_image(self):
        return "nrel/openstudio:{}".format(self.os_version)

    @property
    def weather_dir(self):
        return self._weather_dir

    def upload_batch_files_to_cloud(self, tmppath):
        """ Upload all files in ``tmppath`` to the cloud (where they will be used by the batch
        jobs).
        """
        raise NotImplementedError

    def copy_files_at_cloud(self, files_to_copy):
        """Copy files from-cloud-to-cloud storage. This is used to avoid using bandwidth to upload
        duplicate files.

        :param files_to_copy: a dict where the key is the relative path to a "source" filename to
            copy, and the value is a list of relative paths to copy the source file to. All paths
            are relative to the ``tmppath`` used in ``prep_batches()`` (so the implementation should
            prepend the bucket name and prefix where they were uploaded to by
            ``upload_batch_files_to_cloud``).
        """
        raise NotImplementedError

    def prep_weather_files_for_batch(self, tmppath):
        """Downloads, if necessary, and extracts weather files to ``self._weather_dir``.

        Because there may be duplicate weather files, this also identifies duplicates to avoid
        redundant compression work and uploading to the cloud.

        It will put unique files in the ``tmppath`` (in the 'weather' subdir), and return a dict of
        duplicates. This will allow the duplicates to be recreated on the cloud via copying
        from-cloud-to-cloud rather than reuploading duplicates.

        :param tmppath: Unique weather files (compressed) will be copied into a 'weather' subdir
            of this path.

        :returns: a dict of the filenames of unique weather files to the names of duplicate weather
            files. After the files in the ``tmppath``, which includes the unique weather files, have
            been uploaded, then a quick cloud-to-cloud copy can be used to recreate the duplicates
            on the cloud (without having to upload them).
        """
        with tempfile.TemporaryDirectory(prefix="bsb_" ) as tmp_weather_in_dir:
            self._weather_dir = tmp_weather_in_dir

            # Downloads, if necessary, and extracts weather files to ``self._weather_dir``
            self._get_weather_files()

            # Determine the unique weather files
            epw_filenames = list(filter(lambda x: x.endswith(".epw"), os.listdir(self.weather_dir)))
            logger.info("Calculating hashes for weather files")
            epw_hashes = Parallel(n_jobs=-1, verbose=9)(
                delayed(calc_hash_for_file)(pathlib.Path(self.weather_dir) / epw_filename) for epw_filename in epw_filenames
            )
            # keep track of unique EPWs that may have dupes, and to compress and upload to cloud
            unique_epws = collections.defaultdict(list)
            # keep track of duplicates of the unique EPWs to copy (from cloud-to-cloud)
            epws_to_copy = []
            for epw_filename, epw_hash in zip(epw_filenames, epw_hashes):
                if bool(unique_epws[epw_hash]):
                    # not the first file with this hash (it's a duplicate). add to ``epws_to_copy``
                    if epw_filename == unique_epws[epw_hash][0]:
                        raise NotImplementedError #todo-xxxxx
                    epws_to_copy.append((unique_epws[epw_hash][0], epw_filename))
                unique_epws[epw_hash].append(epw_filename)

            # Compress unique weather files and save to ``tmp_weather_out_path``, which will get
            # uploaded to cloud storage
            logger.info("Compressing unique weather files")
            tmp_weather_out_path = tmppath / "weather"
            os.makedirs(tmp_weather_out_path)
            Parallel(n_jobs=-1, verbose=9)(
                delayed(compress_file)(
                    pathlib.Path(self.weather_dir) / x[0],
                    str(tmp_weather_out_path / x[0]) + ".gz",
                    )
                for x in unique_epws.values()
            )

            # Calculate and print savings of duplicate files
            total_count = 0
            dupe_count = 0
            dupe_bytes = 0
            for epws in unique_epws.values():
                count = len(epws)
                total_count += count
                if (count > 1):
                    dupe_count += count - 1
                    bytes = os.path.getsize(str(tmp_weather_out_path / epws[0]) + ".gz") * dupe_count
                    dupe_bytes = bytes * (count - 1)
            logger.info(f"Identified {dupe_count:,} duplicate weather files "
                        f"({len(unique_epws):,} unique, {total_count:,} total); "
                        f"saved from uploading {(dupe_bytes / 1024 / 1024):,.1f} MiB")
            return epws_to_copy

    def prep_assets_for_batch(self, tmppath):
        logger.debug("Creating assets tarfile")
        with tarfile.open(tmppath / "assets.tar.gz", "x:gz") as tar_f:
            project_path = pathlib.Path(self.project_dir)
            buildstock_path = pathlib.Path(self.buildstock_dir)
            tar_f.add(buildstock_path / "measures", "measures")
            if os.path.exists(buildstock_path / "resources/hpxml-measures"):
                tar_f.add(
                    buildstock_path / "resources/hpxml-measures",
                    "resources/hpxml-measures",
                    )
            tar_f.add(buildstock_path / "resources", "lib/resources")
            tar_f.add(
                project_path / "housing_characteristics",
                "lib/housing_characteristics",
                )

    def prep_jobs_for_batch(self, tmppath):
        # Generate buildstock.csv
        buildstock_csv_filename = self.sampler.run_sampling()
        df = read_csv(buildstock_csv_filename, index_col=0, dtype=str)
        self.validate_buildstock_csv(self.project_filename, df)
        building_ids = df.index.tolist()
        n_datapoints = len(building_ids)
        n_sims = n_datapoints * (len(self.cfg.get("upgrades", [])) + 1)
        logger.debug("Total number of simulations = {}".format(n_sims))

        # This is the maximum number of jobs that can be in an array
        if self.batch_array_size <= self.MAX_JOB_COUNT:
            max_array_size = self.batch_array_size
        else:
            max_array_size = self.MAX_JOB_COUNT
        n_sims_per_job = math.ceil(n_sims / max_array_size)
        n_sims_per_job = max(n_sims_per_job, 2)
        logger.debug("Number of simulations per array job = {}".format(n_sims_per_job))

        # Create list of (building ID, upgrade to apply) pairs for all simulations to run.
        baseline_sims = zip(building_ids, itertools.repeat(None))
        upgrade_sims = itertools.product(building_ids, range(len(self.cfg.get("upgrades", []))))
        all_sims = list(itertools.chain(baseline_sims, upgrade_sims))
        random.shuffle(all_sims)
        all_sims_iter = iter(all_sims)

        os.makedirs(tmppath / "jobs")

        # Write each batch of simulations to a file.
        logger.info("Queueing jobs")
        for i in itertools.count(0):
            batch = list(itertools.islice(all_sims_iter, n_sims_per_job))
            if not batch:
                break
            job_json_filename = tmppath / "jobs" / "job{:05d}.json".format(i)
            with open(job_json_filename, "w") as f:
                json.dump(
                    {
                        "job_num": i,
                        "n_datapoints": n_datapoints,
                        "batch": batch,
                    },
                    f,
                    indent=4,
                )
        job_count = i
        logger.debug("Job count = {}".format(job_count))

        # Compress job jsons
        jobs_dir = tmppath / "jobs"
        logger.debug("Compressing job jsons using gz")
        tick = time.time()
        with tarfile.open(tmppath / "jobs.tar.gz", "w:gz") as tf:
            tf.add(jobs_dir, arcname="jobs")
        tick = time.time() - tick
        logger.debug("Done compressing job jsons using gz {:.1f} seconds".format(tick))
        shutil.rmtree(jobs_dir)

        return n_sims, job_count


    def prep_batches(self):
        """
        Prepare batches of samples to be uploaded and run in the cloud.

        This will prepare all the files needed by the Batch jobs, including:
            - Weather files
            - Assets
            - Sampling, and splitting the samples into (at most) ``self.batch_array_size`` batches

        It will then upload all of those files, excluding duplicate weather files. It will use
        ``copy_files_at_cloud`` to copy the duplicate files (instead of uploading them).

        ``self.weather_dir`` must exist before calling this method. This is where weather files are
        stored temporarily.

        :returns: (n_sims, job_count), where
            ``n_sims``: The total number of simulations that will be run.
            ``job_count``: The number of jobs the samples were split into.
        """
        with tempfile.TemporaryDirectory(prefix="bsb_") as tmpdir:
            tmppath = pathlib.Path(tmpdir)
            epws_to_copy, n_sims, job_count = self._prep_batch_files(tmppath)

            # Copy all the files to cloud storage
            logger.info("Uploading files for batch...")
            self.upload_batch_files_to_cloud(tmppath)

            logger.info("Copying duplicate weather files...")
            self.copy_files_at_cloud(epws_to_copy)

            return (n_sims, job_count)


    def _prep_batch_files(self, tmppath):
        """
        Split out for testability (so test can manage and inspect the contents of the tmppath).
        """

        # Weather files
        logger.info("Prepping weather files...")
        epws_to_copy = self.prep_weather_files_for_batch(tmppath)

        # Assets
        self.prep_assets_for_batch(tmppath)

        # Project configuration
        logger.info("Writing project configuration for upload")
        with open(tmppath / "config.json", "wt", encoding="utf-8") as f:
            json.dump(self.cfg, f)

        # Collect simulations to queue
        n_sims, job_count = self.prep_jobs_for_batch(tmppath)

        return (epws_to_copy, n_sims, job_count)
