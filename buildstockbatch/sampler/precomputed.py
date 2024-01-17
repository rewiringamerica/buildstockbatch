# -*- coding: utf-8 -*-

"""
buildstockbatch.sampler.precomputed
~~~~~~~~~~~~~~~
This object contains the code required for ingesting an already existing buildstock.csv file

:author: Ry Horsey
:copyright: (c) 2018 by The Alliance for Sustainable Energy
:license: BSD-3
"""

import csv
import logging
import os
import shutil

from .base import BuildStockSampler
from buildstockbatch.exc import ValidationError
from buildstockbatch.utils import path_rel_to_file

logger = logging.getLogger(__name__)


class PrecomputedSampler(BuildStockSampler):
    def __init__(self, parent, sample_file, building_ids=None):
        """Precomputed Sampler

        :param parent: BuildStockBatchBase object
        :type parent: BuildStockBatchBase (or subclass)
        :param sample_file: relative or absolute path to buildstock.csv to use
        :type sample_file: str
        """
        super().__init__(parent)
        project_filename = self.parent().project_filename
        self.validate_args(project_filename, sample_file=sample_file)
        self.buildstock_csv = path_rel_to_file(project_filename, sample_file)
        self.building_ids = set([str(id) for id in building_ids]) if building_ids else None

    @classmethod
    def validate_args(cls, project_filename, **kw):
        for k, v in kw.items():
            if k == "sample_file":
                if not isinstance(v, str):
                    raise ValidationError("sample_file should be a path string")
                if not os.path.exists(path_rel_to_file(project_filename, v)):
                    raise ValidationError(f"sample_file doesn't exist: {v}")
            elif k == "building_ids":
                if not isinstance(v, list):
                    raise ValidationError("building_ids should be a list of ints")
                if not all([isinstance(b, int) for b in v]):
                    raise ValidationError("building_ids should be a list of ints")
            else:
                raise ValidationError(f"Unknown argument for sampler: {k}")
        return True

    def run_sampling(self):
        """
        Check that the sampling has been precomputed and if necessary move to the required path.
        """
        if self.building_ids:
            if self.csv_path == self.buildstock_csv:
                raise ValidationError(
                    f"Buildstock CSV file ({self.buildstock_csv}) will be overwritten when filtering by building IDs. "
                    "Please move it."
                )

            # Make a copy of buildstock_csv with only the selected building IDs.
            with open(self.buildstock_csv, newline="") as in_f:
                with open(self.csv_path, "w", newline="") as out_f:
                    reader = csv.reader(in_f)
                    writer = csv.writer(out_f)
                    writer.writerow(next(reader))
                    for line in reader:
                        if line[0] in self.building_ids:
                            writer.writerow(line)

            return self.csv_path

        if self.csv_path != self.buildstock_csv:
            shutil.copy(self.buildstock_csv, self.csv_path)
        return self.csv_path
