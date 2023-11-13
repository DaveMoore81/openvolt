from datetime import datetime
import logging
import argparse
import helper
import dataset
import asyncio

logging.basicConfig(
    encoding="utf-8",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def get_consumption_source_report(
    meter_interval_data: dict, generation_mix_data: dict
) -> dict:
    # Create a report for kWh consumption using Meter Interval
    # and NG's Generation Mix datasets

    consumption_source = {}

    # Loop through each of the interval datasets for the meter
    for interval in meter_interval_data:
        consumption_source[interval] = {}
        consumption_source[interval]["total"] = 0

        if meter_interval_data[interval]["consumption_units"].upper() != "KWH":
            logging.error(f"Found non-standard consumption unit in interval {interval}")
            return None

        # Increase the total Kwh's with the intervals total usage
        consumption_source[interval]["total"] += int(
            meter_interval_data[interval]["consumption"]
        )

        # Break the total consumption into individual fuel sources
        # as per the NationalGrid's generation for the interval
        for fuel_type in generation_mix_data[interval]:
            if fuel_type not in consumption_source[interval]:
                consumption_source[interval][fuel_type] = 0

            # Add that percentage of kWh to the total for that specific fuel type
            consumption_source[interval][fuel_type] += (
                float(meter_interval_data[interval]["consumption"]) / 100
            ) * generation_mix_data[interval][fuel_type]

    # return report with total Kwh's per fuel type
    return consumption_source


def get_carbon_emissions_report(
    meter_interval_data: dict, consumption_source: dict, carbon_emission_factors: dict
) -> dict:
    # Create a report for CO2(g) emission using Consumption Source report
    # and the carbon emission factors from NG

    carbon_emissions = {}

    # Loop through each of the interval datasets for the meter
    for interval in meter_interval_data:
        carbon_emissions[interval] = {}

        if meter_interval_data[interval]["consumption_units"].upper() != "KWH":
            logging.error(f"Found non-standard consumption unit in interval {interval}")
            return None

        # Loop through each of the fuel types and calculate emissions for the interval
        carbon_emissions[interval]["total"] = 0

        for fuel_type in consumption_source[interval]:
            if fuel_type != "total":
                if fuel_type not in carbon_emissions[interval]:
                    carbon_emissions[interval][fuel_type] = 0

                # Calculate the carbon emissions and add them
                # to current fuel type total and overall total
                generated_carbon = (
                    consumption_source[interval][fuel_type]
                    * carbon_emission_factors[fuel_type]
                )
                carbon_emissions[interval][fuel_type] += generated_carbon
                carbon_emissions[interval]["total"] += generated_carbon

    # Return raw report of emissions in gCO2/kWh
    return carbon_emissions


async def generate_reports(
    start_date: datetime,
    end_date: datetime,
    customer_id: str,
    meter_id: str,
    output_file: str,
    validate_dataset: bool = True,
):
    # Main function to generate the required reports for the test scenario

    consumption_source_report = {}
    carbon_emissions_report = {}

    consumption_source_report_totals = {}
    carbon_emissions_report_totals = {}

    # Get a list of meters filtering on customer and/or meter id
    logging.info("Retrieving meter interval data...")
    meters = await dataset.get_meters(customer_id=customer_id, meter_id=meter_id)

    # Get the factors for emissions generated per Kwh for fuel types
    logging.info("Retrieving carbon emission factors...")
    carbon_emission_factors = await dataset.get_carbon_emission_factors()

    # Loop through the meters found to cover single customer with multiple meters
    for meter in meters:
        # Pull the postcode from the address to use with regional NationalGrid data
        postcode_region = helper.uk_address_to_region(meters[meter]["address"])

        # Generate both the OpenVolt meter interval data and
        # National Grid Generation / Emission data
        logging.info("Retrieving meter interval data...")
        logging.info("Retrieving generation mix data...")

        meter_interval_data, generation_mix_data = await asyncio.gather(
            dataset.get_meter_interval_data(start_date, end_date, meter),
            dataset.get_generation_mix_data(start_date, end_date, postcode_region),
        )

        # Validate dataset to ensure each meter interval has a corresponding entry

        if validate_dataset:
            if not dataset.validate_openvolt_nationalgrid_datasets(
                meter_interval_data, generation_mix_data
            ):
                logging.error(
                    "Missing intervals from NationalGrid dataset, dataset validation failed"
                )
                raise ValueError(
                    "Missing intervals from NationalGrid dataset, dataset validation failed"
                )

        # Generate raw reports for both Consumption Source(Generation Mix)
        # and Carbon Emissions for the OpenVolt meter data

        logging.info("Generating consumption source report...")
        consumption_source_report[meter] = get_consumption_source_report(
            meter_interval_data, generation_mix_data
        )
        logging.info("Generating carbon emissions report...")
        carbon_emissions_report[meter] = get_carbon_emissions_report(
            meter_interval_data,
            consumption_source_report[meter],
            carbon_emission_factors,
        )

        logging.info("Building final report...")

        # Build the final dataset to deliver the required report

        consumption_source_report_totals[meter] = {}
        carbon_emissions_report_totals[meter] = {}

        # Loop through the intervals of the
        # merged datasets to calculate totals

        for interval in consumption_source_report[meter]:
            # Generate the consumption report, overall total kWh
            # including breakdown by fuel type

            for fuel_type in consumption_source_report[meter][interval]:
                if fuel_type not in consumption_source_report_totals[meter]:
                    consumption_source_report_totals[meter][fuel_type] = 0
                consumption_source_report_totals[meter][fuel_type] = (
                    consumption_source_report_totals[meter][fuel_type]
                    + consumption_source_report[meter][interval][fuel_type]
                )

            # Generate the Carbon Emissions generated
            # (in KG of CO2 per kWh), overall total including breakdown

            for fuel_type in carbon_emissions_report[meter][interval]:
                if fuel_type not in carbon_emissions_report_totals[meter]:
                    carbon_emissions_report_totals[meter][fuel_type] = 0
                carbon_emissions_report_totals[meter][
                    fuel_type
                ] = carbon_emissions_report_totals[meter][fuel_type] + (
                    carbon_emissions_report[meter][interval][fuel_type] / 1000
                )

        # For validation, optional export of data streams to a file
        if output_file is not None:
            helper.output_datastream_to_file(
                meter,
                output_file,
                meter_interval_data,
                generation_mix_data,
                consumption_source_report[meter],
                carbon_emissions_report[meter],
            )

    # return the datasets ready to be consumed
    return [consumption_source_report_totals, carbon_emissions_report_totals]


def display_report(
    consumption_source_report_totals: dict,
    carbon_emissions_report_totals: dict,
    start_date: str,
    end_date: str,
):
    print("\nReport for OpenVolt Test API\n")
    print(f"Start Date: {start_date} -> End Data: {end_date}")
    print("-------------------------------")

    for meter in consumption_source_report_totals:
        print(f"\nMeter ID {meter}\n")

        print(
            f"Total Consumption: {round(consumption_source_report_totals[meter]['total'],2)} kWh"
        )

        for fuel_type in consumption_source_report_totals[meter]:
            if fuel_type != "total":
                print(
                    f"  {fuel_type} {round(consumption_source_report_totals[meter][fuel_type],2)} kWh ({helper.percent(consumption_source_report_totals[meter][fuel_type],consumption_source_report_totals[meter]['total'])} %)"
                )

        print(
            f"\n\nTotal Emissions: {round(carbon_emissions_report_totals[meter]['total'],2)} CO2 kg's "
        )
        for fuel_type in carbon_emissions_report_totals[meter]:
            if fuel_type != "total":
                print(
                    f"  {fuel_type} {round(carbon_emissions_report_totals[meter][fuel_type],2)} CO2 kg's ({helper.percent(carbon_emissions_report_totals[meter][fuel_type],carbon_emissions_report_totals[meter]['total'])} %)"
                )

    print("\n")


def process_cmdline_parser():
    # Set up the parser to accept command line arguments

    parser = argparse.ArgumentParser(description="OpenVolt API Test")
    parser.add_argument("--customerid", "-c", help="Generate data using customer_id")
    parser.add_argument("--meterid", "-m", help="Generate data using meter_id")
    parser.add_argument("--startdate", "-s", help="start date in YYYY-MM-DD format")
    parser.add_argument("--enddate", "-e", help="end date in YYYY-MM-DD format")
    parser.add_argument(
        "--output", "-o", help="specify file output prefix for debug data streams"
    )
    args = vars(parser.parse_args())

    logging.debug(f"Parser arguments: {args}")

    return args


async def main():
    print("\nOpenVolt API Test\n")

    app_start_time = datetime.now()

    # Get list of command line arguements
    args = process_cmdline_parser()

    # Set default target customer/meter and timeframe's to match the test scope

    if not args["customerid"] and not args["meterid"]:
        args["customerid"] = "6514153c23e3d1424bf82738"
        logging.info(
            f"Missing customerid and meterid parameters, customerid set to test customer id: {args['customerid']}"
        )
    if not args["startdate"]:
        args["startdate"] = "2023-01-01"
        logging.info(f"Missing startdate, set to default {args['startdate']}")
    if not args["enddate"]:
        args["enddate"] = "2023-02-01"
        logging.info(f"Missing enddate, set to default {args['enddate']}")

    customer_id = args["customerid"]
    meter_id = args["meterid"]
    start_date = datetime.strptime(args["startdate"], "%Y-%m-%d")
    end_date = datetime.strptime(args["enddate"], "%Y-%m-%d")
    output_file = args["output"]

    # Uncomment as a quick way to test start and end dates
    # start_date = datetime.strptime("2023-01-01 00:00", "%Y-%m-%d %H:%M")
    # end_date = datetime.strptime("2023-01-01 02:00", "%Y-%m-%d %H:%M")

    # Generate the reports as per requirements
    logging.info("Starting Report Generation...")
    (
        consumption_source_report_totals,
        carbon_emissions_report_totals,
    ) = await generate_reports(start_date, end_date, customer_id, meter_id, output_file)

    # Dump the raw data to debug
    logging.debug("Consumption Report Data")
    logging.debug(consumption_source_report_totals)
    logging.debug("Carbon Emissions Report Data")
    logging.debug(carbon_emissions_report_totals)

    # Display final report
    display_report(
        consumption_source_report_totals,
        carbon_emissions_report_totals,
        start_date,
        end_date,
    )

    app_end_time = datetime.now()

    logging.info(f"{(app_end_time-app_start_time).total_seconds()}  seconds runtime")


if __name__ == "__main__":
    asyncio.run(main())
