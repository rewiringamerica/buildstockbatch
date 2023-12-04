"""Tests for the DockerBatchBase class."""
from fsspec.implementations.local import LocalFileSystem
import json
import os
import pathlib
import shutil
import tarfile
import tempfile
from unittest.mock import MagicMock, PropertyMock

from buildstockbatch.cloud.docker_base import DockerBatchBase
from buildstockbatch.test.shared_testing_stuff import docker_available
from buildstockbatch.utils import get_project_configuration

here = os.path.dirname(os.path.abspath(__file__))
resources_dir = os.path.join(here, "test_inputs", "test_openstudio_buildstock", "resources")


@docker_available
def test_prep_batches(basic_residential_project_file, mocker):
    """Test that samples are created and bundled into batches correctly."""
    project_filename, results_dir = basic_residential_project_file()

    mocker.patch.object(DockerBatchBase, "results_dir", results_dir)
    sampler_property_mock = mocker.patch.object(DockerBatchBase, "sampler", new_callable=PropertyMock)
    sampler_mock = mocker.MagicMock()
    sampler_property_mock.return_value = sampler_mock
    # Hard-coded sampling output includes 5 buildings.
    sampler_mock.run_sampling = MagicMock(return_value=os.path.join(resources_dir, "buildstock_good.csv"))

    dbb = DockerBatchBase(project_filename)
    dbb.batch_array_size = 3
    DockerBatchBase.validate_project = MagicMock(return_value=True)

    with tempfile.TemporaryDirectory(prefix="bsb_") as tmpdir, tempfile.TemporaryDirectory(
        prefix="bsb_"
    ) as tmp_weather_dir:
        dbb._weather_dir = tmp_weather_dir
        tmppath = pathlib.Path(tmpdir)

        job_count, unique_epws = dbb.prep_batches(tmppath)
        sampler_mock.run_sampling.assert_called_once()

        # Of the three test weather files, two are identical
        assert sorted([sorted(i) for i in unique_epws.values()]) == [
            ["G2500210.epw"],
            ["G2601210.epw", "G2601390.epw"],
        ]

        # Three job files should be created, with 10 total simulations, split
        # into batches of 4, 4, and 2 simulations.
        assert job_count == 3
        jobs_file_path = tmppath / "jobs.tar.gz"
        with tarfile.open(jobs_file_path, "r") as tar_f:
            all_job_files = ["jobs", "jobs/job00000.json", "jobs/job00001.json", "jobs/job00002.json"]
            assert tar_f.getnames() == all_job_files
            simulations = []
            for filename in all_job_files[1:]:
                job = json.load(tar_f.extractfile(filename))
                assert filename == f"jobs/job{job['job_num']:05d}.json"
                assert job["n_datapoints"] == 5  # Total number of buildings
                assert len(job["batch"]) in (2, 4)  # Number of simulations in this batch
                simulations.extend(job["batch"])

            # Check that all 10 expected simulations are present
            assert len(simulations) == 10
            for building in range(1, 6):
                # Building baseline
                assert [building, None] in simulations
                # Building with upgrade 0
                assert [building, 0] in simulations


def test_get_epws_to_download():
    resources_dir_path = pathlib.Path(resources_dir)
    options_file = resources_dir_path / "options_lookup.tsv"
    buildstock_file = resources_dir_path / "buildstock_good.csv"

    with tempfile.TemporaryDirectory(prefix="bsb_") as sim_dir_str:
        sim_dir = pathlib.Path(sim_dir_str)
        os.makedirs(sim_dir / "lib" / "resources")
        os.makedirs(sim_dir / "lib" / "housing_characteristics")
        shutil.copy(options_file, sim_dir / "lib" / "resources")
        shutil.copy(buildstock_file, sim_dir / "lib" / "housing_characteristics" / "buildstock.csv")

        jobs_d = {
            "job_num": 0,
            "n_datapoints": 10,
            "batch": [
                [1, None],
                [5, None],
            ],
        }

        epws = DockerBatchBase.get_epws_to_download(sim_dir, jobs_d)
        assert epws == set(["weather/G0100970.epw", "weather/G0100830.epw"])


def test_run_simulations(basic_residential_project_file):
    jobs_d = {
        "job_num": 0,
        "n_datapoints": 10,
        "batch": [
            [1, None],
            [5, None],
        ],
    }
    fs = LocalFileSystem()
    project_filename, results_dir = basic_residential_project_file()
    cfg = get_project_configuration(project_filename)

    with tempfile.TemporaryDirectory(prefix="bsb_") as temp_dir_str:
        temp_path = pathlib.Path(temp_dir_str)
        sim_dir = temp_path / "simdata" / "openstudio"
        os.makedirs(sim_dir)
        # sim_dir is also the working directory (defined in the nrel/openstudio
        # Dockerfile), which some file operations depend on.
        old_cwd = os.getcwd()
        os.chdir(sim_dir)
        bucket = temp_path / "bucket"
        os.makedirs(bucket / "test_prefix" / "results" / "simulation_output")

        DockerBatchBase.run_simulations(cfg, 0, jobs_d, sim_dir, fs, bucket, "test_prefix")

        output_dir = bucket / "test_prefix" / "results" / "simulation_output"
        assert sorted(os.listdir(output_dir)) == ["results_job0.json.gz", "simulations_job0.tar.gz"]
        # Check that files were cleaned up correctly
        assert not os.listdir(sim_dir)
        os.chdir(old_cwd)
