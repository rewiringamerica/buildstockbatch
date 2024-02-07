"""Create a sample of buildings (buildstock.csv) for a single county+PUMA.

Methodology:

This modifies the conditional probability distributions from the standard ResStock national project
to filter the sample to a single county+PUMA.

To do this, we modify two files:
 - ASHRAE IECC Climate Zone 2004.tsv
   - Make 100% of the samples fall into the climate zone of the selected location.
 - County and PUMA.tsv
   - Make 100% of samples (within the chosen climate zone) fall into the selected county + PUMA

All other variables are downstream or these (or aren't dependent on them).


Assumptions:
    This logic is only guaranteed to work for the current ResStock national project. Other changes
    to the dependencies between the variables can break it!

    In particular, this code assumes:
        - ASHRAE climate zone has no dependencies
        - County and Puma depends only on the ASHRAE climate zone
        - Each County+Puma fall entirely in one climate zone
"""
import argparse
import csv
import os
import shutil
import tempfile

from buildstockbatch.utils import ContainerRuntime
from sampler import residential_quota


class SampleOnly:
    CONTAINER_RUNTIME = ContainerRuntime.DOCKER

    def __init__(self, output_dir):
        # Sampler uses this to find the sampling scripts
        self.buildstock_dir = "/usr/local/google/home/nweires/RewiringAmerica/resstock"
        # ResStock national project
        # Could use a different project, but `County and PUMA.tsv` and `ASHRAE IECC Climate Zone 2004.tsv`
        # must exist in the expected format.
        self.resstock_dir = "/usr/local/google/home/nweires/RewiringAmerica/resstock/project_national"
        # Directory containing the conditional probability distributions we plan to modify
        self.housing_characteristics_dir = os.path.join(self.resstock_dir, "housing_characteristics")
        self.output_dir = output_dir

    @property
    def docker_image(self):
        return "nrel/openstudio:{}".format(self.os_version)

    @property
    def os_version(self):
        return "3.6.1"

    @property
    def project_filename(self):
        """Sampler expects this to exist, but doesn't need it."""
        return None

    def get_climate_zone(self, county, PUMA):
        """Given a county and PUMA, find the climate zone that contains them."""
        with open(os.path.join(self.housing_characteristics_dir, "County and PUMA.tsv")) as f:
            reader = csv.reader(f, delimiter="\t")
            headers = next(reader)
            location_col = headers.index(f"Option={county}, {PUMA}")

            zone = None
            for row in reader:
                if row[0].strip()[0] == "#":
                    continue
                # Find the zone with a non-zero chance of producing this county + PUMA
                if row[location_col] != "0":
                    if zone:
                        raise ValueError(f"Found multiple climate zones for {county}, {PUMA}")
                    zone = row[0]

            if not zone:
                raise ValueError(f"No climate zone found for {county}, {PUMA}")
            return zone

    def run_sampler(self, county, PUMA, n_samples):
        """
        Args:
            county: geoid of county
            PUMA: geoid of PUMA
            n_samples: Number of building samples to produce.
        """
        # TODO: validate format of these

        climate_zone = self.get_climate_zone(county, PUMA)
        print(f"found climate zone: {climate_zone}")
        # Create a new copy of the probability distribution TSV files, so we can
        # make some changes to them.
        with tempfile.TemporaryDirectory(prefix="sampling_", dir=self.buildstock_dir) as tmpdir:
            temp_housing_characteristics_dir = os.path.join(tmpdir, "housing_characteristics")
            shutil.copytree(self.housing_characteristics_dir, temp_housing_characteristics_dir)

            # Update climate zone TSV
            zone_filename = "ASHRAE IECC Climate Zone 2004.tsv"
            zone_tsv = os.path.join(self.housing_characteristics_dir, zone_filename)
            new_zone_tsv = os.path.join(temp_housing_characteristics_dir, zone_filename)
            with open(zone_tsv) as old_f:
                reader = csv.reader(old_f, delimiter="\t")
                with open(new_zone_tsv, "w") as new_f:
                    writer = csv.writer(new_f, delimiter="\t")
                    headers = next(reader)
                    writer.writerow(headers)

                    # This file has a single row of probabilities, which we replace with 0s and a single 1.
                    zone_header = f"Option={climate_zone}"
                    writer.writerow(["1" if header == zone_header else "0" for header in headers])

            # Update county + PUMA TSV
            county_filename = "County and PUMA.tsv"
            county_tsv = os.path.join(self.housing_characteristics_dir, county_filename)
            new_county_tsv = os.path.join(temp_housing_characteristics_dir, county_filename)
            with open(county_tsv) as old_f:
                reader = csv.reader(old_f, delimiter="\t")
                with open(new_county_tsv, "w") as new_f:
                    writer = csv.writer(new_f, delimiter="\t")
                    headers = next(reader)
                    writer.writerow(headers)
                    # First value in headers lists the climate zone dependency -
                    # just keep the others, which list the County+PUMA options.
                    assert headers[0] == "Dependency=ASHRAE IECC Climate Zone 2004"
                    headers = headers[1:]
                    for row in reader:
                        # Skip comments
                        if row[0].strip()[0] == "#":
                            continue

                        elif row[0] == climate_zone:
                            county_header = f"Option={county}, {PUMA}"
                            writer.writerow(
                                [row[0]] + ["1" if headers[i] == county_header else "0" for i, v in enumerate(row[1:])]
                            )

                        else:
                            writer.writerow(row)
                            # writer.writerow([row[0]] + (len(headers) - 1) * ["0"])

            self.cfg = {"project_directory": os.path.basename(tmpdir)}
            self.project_dir = tmpdir
            # Must create sampler after all instances vars exist, because it makes a copy of this object.
            sampler = residential_quota.ResidentialQuotaSampler(self, n_samples)
            sampler.run_sampling()

            # Copy output to non-temp dir
            shutil.copy(
                os.path.join(temp_housing_characteristics_dir, "buildstock.csv"),
                os.path.join(self.output_dir, f"buildstock_{county}_{PUMA}.csv"),
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("county")
    parser.add_argument("PUMA")
    parser.add_argument("n_samples", type=int)
    # TODO: create dir if needed
    parser.add_argument("output_dir", default=".", nargs="?")
    args = parser.parse_args()

    # output_dir = "."

    # county = "G0400010"  # AL
    # PUMA = "G04000300"

    # county = "G1900030"  # IA
    # PUMA = "G19001800"
    s = SampleOnly(args.output_dir)
    s.run_sampler(args.county, args.PUMA, args.n_samples)


if __name__ == "__main__":
    main()
