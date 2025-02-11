import json
import pandas as pd
import pathlib
import pytest
import shutil
import subprocess
import re
import tempfile

from buildstockbatch.local import LocalBatch
from buildstockbatch.utils import get_project_configuration
from buildstockbatch.test.shared_testing_stuff import (
    resstock_directory,
    resstock_required,
)


@pytest.mark.parametrize(
    "project_filename",
    [
        resstock_directory / "project_national" / "national_baseline.yml",
        resstock_directory / "project_testing" / "testing_baseline.yml",
        resstock_directory / "project_national" / "sdr_upgrades_tmy3.yml",
    ],
    ids=lambda x: x.stem,
)
@resstock_required
def test_resstock_local_batch(project_filename):
    LocalBatch.validate_project(str(project_filename))
    batch = LocalBatch(str(project_filename))

    # Get the number of upgrades
    n_upgrades = len(batch.cfg.get("upgrades", []))
    # Limit the number of upgrades to 6 to reduce simulation time
    if n_upgrades > 6:
        batch.cfg["upgrades"] = batch.cfg["upgrades"][0:6]
        n_upgrades = 6

    # Modify the number of datapoints so we're not here all day.
    if n_upgrades == 0:
        n_datapoints = 4
    else:
        n_datapoints = 2
    batch.cfg["sampler"]["args"]["n_datapoints"] = n_datapoints

    local_weather_file = resstock_directory.parent / "weather" / batch.cfg["weather_files_url"].split("/")[-1]
    if local_weather_file.exists():
        del batch.cfg["weather_files_url"]
        batch.cfg["weather_files_path"] = str(local_weather_file)

    batch.run_batch()

    # Make sure all the files are there
    out_path = pathlib.Path(batch.output_dir)
    simout_path = out_path / "simulation_output"
    assert (simout_path / "results_job0.json.gz").exists()
    assert (simout_path / "simulations_job0.tar.gz").exists()

    # Build upgrades2expected_bldgs map by scanning existing files
    upgrades2expected_bldgs = {}  # key: upgrade_id, value: list of building_ids
    for upgrade_dir in (simout_path / "timeseries").glob("up*"):
        upgrade_id = int(upgrade_dir.name[2:])  # Extract ID from 'up##'
        bldg_files = list(upgrade_dir.glob("bldg*.parquet"))
        upgrades2expected_bldgs[upgrade_id] = [int(f.stem[4:]) for f in bldg_files]  # Extract ID from 'bldg#######'

    batch.process_results()

    if batch.cfg.get("postprocessing", {}).get("keep_individual_timeseries", False):
        assert (simout_path / "timeseries").exists()
    else:
        assert not (simout_path / "timeseries").exists()

    assert (simout_path / "simulations_job0.tar.gz").exists()
    base_pq = out_path / "parquet" / "baseline" / "results_up00.parquet"
    assert base_pq.exists()
    base = pd.read_parquet(base_pq, columns=["completed_status", "started_at", "completed_at"])
    assert (base["completed_status"] == "Success").all()
    assert base.dtypes["started_at"] == "timestamp[s][pyarrow]"
    assert base.dtypes["completed_at"] == "timestamp[s][pyarrow]"
    assert base.shape[0] == n_datapoints
    ts_pq_path = out_path / "parquet" / "timeseries"
    ts_time_cols = ["time", "timeutc", "timedst"]
    for upgrade_id in range(0, n_upgrades + 1):
        partition_path = ts_pq_path / f"upgrade={upgrade_id}"
        for partition_col in batch.cfg.get("postprocessing", {}).get("partition_columns", []):
            partition_dirs = list(partition_path.glob(f"{partition_col.lower()}=*"))
            if len(partition_dirs) < 1:
                print(f"No directories found for partition column: {partition_col} for upgrade {upgrade_id}")
                continue
            partition_path = partition_dirs[0]
        if expected_bldgs := upgrades2expected_bldgs.get(upgrade_id, []):
            ts_pq_filename = next(partition_path.glob("group*.parquet"))
            assert ts_pq_filename.exists()
            tsdf = pd.read_parquet(ts_pq_filename, columns=ts_time_cols)
            for col in tsdf.columns:
                assert tsdf[col].dtype == "timestamp[s][pyarrow]"

        assert (out_path / "results_csvs" / f"results_up{upgrade_id:02d}.csv.gz").exists()
        if upgrade_id >= 1:
            upg_pq = out_path / "parquet" / "upgrades" / f"upgrade={upgrade_id}" / f"results_up{upgrade_id:02d}.parquet"
            assert upg_pq.exists()
            upg = pd.read_parquet(upg_pq, columns=["completed_status", "building_id"])
            assert upg.shape[0] == n_datapoints
            for _, row in upg.iterrows():
                if row["building_id"] in expected_bldgs:
                    assert row["completed_status"] == "Success"
                else:
                    assert row["completed_status"] == "Invalid"
    assert (ts_pq_path / "_common_metadata").exists()
    shutil.rmtree(out_path)


@resstock_required
def test_local_simulation_timeout(mocker):
    def mocked_subprocess_run(run_cmd, **kwargs):
        assert "timeout" in kwargs.keys()
        raise subprocess.TimeoutExpired(run_cmd, kwargs["timeout"])

    mocker.patch("buildstockbatch.local.subprocess.run", mocked_subprocess_run)
    sleep_mock = mocker.patch("buildstockbatch.local.time.sleep")

    cfg = get_project_configuration(resstock_directory / "project_national" / "national_baseline.yml")
    cfg["max_minutes_per_sim"] = 5

    with tempfile.TemporaryDirectory() as tmpdir:
        LocalBatch.run_building(
            str(resstock_directory),
            str(resstock_directory / "weather"),
            tmpdir,
            measures_only=False,
            n_datapoints=cfg["sampler"]["args"]["n_datapoints"],
            cfg=cfg,
            i=1,
        )
        sim_path = pathlib.Path(tmpdir, "simulation_output", "up00", "bldg0000001")
        assert sim_path.is_dir()

        msg_re = re.compile(r"Terminated \w+ after reaching max time")

        with open(sim_path / "openstudio_output.log", "r") as f:
            os_output = f.read()
            assert msg_re.search(os_output)

        with open(sim_path / "out.osw", "r") as f:
            out_osw = json.load(f)
            assert "started_at" in out_osw
            assert "completed_at" in out_osw
            assert out_osw["completed_status"] == "Fail"
            assert msg_re.search(out_osw["timeout"])

        err_log_re = re.compile(r"\[\d\d:\d\d:\d\d ERROR\] Terminated \w+ after reaching max time")
        with open(sim_path / "run" / "run.log", "r") as run_log:
            err_log_re.search(run_log.read())
        with open(sim_path / "run" / "failed.job", "r") as failed_job:
            err_log_re.search(failed_job.read())

        sleep_mock.assert_called_once_with(20)
