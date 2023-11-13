import re
import csv
from enum import Enum


def uk_address_to_region(address: str) -> str:
    # NationalGrid accepts Outcode from Postcode to identify regions

    # Using regex filter for UK Postcode definition as per wikipedia for now:
    # https://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom#Validation

    ukpostcode_regex = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})"

    # Sorting to pull back the shortest match which should be the outcode/first section

    postcode = sorted(re.findall(ukpostcode_regex, address)[0], key=len)[-1].replace(
        " ", ""
    )[:-3]

    return postcode


def trim_timestamp(datetimestamp: str) -> str:
    # Given json datetime format can have different granularity (-> minutes/seconds/microseconds)
    # between sources this truncates to a consistent timestamp for our needs going as far as minutes

    # Loads of ways to handle dates and times but just need to match intervals so keeping it simple for now

    return "".join(datetimestamp.split(":", 2)[:2]).replace("Z", "").replace("z", "")


def percent(smallnumber, bignumber, rounding: int = 2):
    return round((smallnumber / bignumber) * 100, 2)


def output_datastream_to_file(
    meter: str,
    output_file: str,
    meter_interval_data: dict,
    generation_mix_data: dict,
    consumption_source_report: dict,
    carbon_emissions_report: dict,
):
    # export of meter_interval_data
    with open(f"{output_file}_{meter}_meter_interval.csv", "w", newline="") as csv_file:
        writer = csv.writer(csv_file)

        outputheader = ["interval", "consumption", "consumption_units"]
        writer.writerow(outputheader)

        for interval in meter_interval_data:
            writer.writerow(
                [
                    interval,
                    meter_interval_data[interval]["consumption"],
                    meter_interval_data[interval]["consumption_units"],
                ]
            )

    # export of generation_mix_data
    with open(f"{output_file}_{meter}_generation_mix.csv", "w", newline="") as csv_file:
        writer = csv.writer(csv_file)

        # TODO Note the headers are static so in future would need to leverage a sorted dictionary for header generation
        outputheader = [
            "interval",
            "biomass",
            "coal",
            "imports",
            "gas",
            "nuclear",
            "other",
            "hydro",
            "solar",
            "wind",
        ]
        writer.writerow(outputheader)

        for interval in generation_mix_data:
            writer.writerow(
                [
                    interval,
                    generation_mix_data[interval]["biomass"],
                    generation_mix_data[interval]["coal"],
                    generation_mix_data[interval]["imports"],
                    generation_mix_data[interval]["gas"],
                    generation_mix_data[interval]["nuclear"],
                    generation_mix_data[interval]["other"],
                    generation_mix_data[interval]["hydro"],
                    generation_mix_data[interval]["solar"],
                    generation_mix_data[interval]["wind"],
                ]
            )

    # export of consumption_source_report
    with open(
        f"{output_file}_{meter}_consumption_source.csv", "w", newline=""
    ) as csv_file:
        writer = csv.writer(csv_file)

        # TODO Note the headers are static so in future would need to leverage a sorted dictionary for header generation
        outputheader = [
            "interval",
            "total",
            "biomass",
            "coal",
            "imports",
            "gas",
            "nuclear",
            "other",
            "hydro",
            "solar",
            "wind",
        ]
        writer.writerow(outputheader)

        for interval in consumption_source_report:
            writer.writerow(
                [
                    interval,
                    consumption_source_report[interval]["total"],
                    consumption_source_report[interval]["biomass"],
                    consumption_source_report[interval]["coal"],
                    consumption_source_report[interval]["imports"],
                    consumption_source_report[interval]["gas"],
                    consumption_source_report[interval]["nuclear"],
                    consumption_source_report[interval]["other"],
                    consumption_source_report[interval]["hydro"],
                    consumption_source_report[interval]["solar"],
                    consumption_source_report[interval]["wind"],
                ]
            )

    # export of carbon_emissions_report
    with open(
        f"{output_file}_{meter}_carbon_emissions.csv", "w", newline=""
    ) as csv_file:
        writer = csv.writer(csv_file)

        # Note the headers are static so in future would need
        # to leverage a sorted dictionary for automation
        outputheader = [
            "interval",
            "total",
            "biomass",
            "coal",
            "imports",
            "gas",
            "nuclear",
            "other",
            "hydro",
            "solar",
            "wind",
        ]
        writer.writerow(outputheader)

        for interval in carbon_emissions_report:
            writer.writerow(
                [
                    interval,
                    carbon_emissions_report[interval]["total"],
                    carbon_emissions_report[interval]["biomass"],
                    carbon_emissions_report[interval]["coal"],
                    carbon_emissions_report[interval]["imports"],
                    carbon_emissions_report[interval]["gas"],
                    carbon_emissions_report[interval]["nuclear"],
                    carbon_emissions_report[interval]["other"],
                    carbon_emissions_report[interval]["hydro"],
                    carbon_emissions_report[interval]["solar"],
                    carbon_emissions_report[interval]["wind"],
                ]
            )
