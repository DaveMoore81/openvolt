from datetime import datetime, timedelta
import helper
import logging
import json
import requests as requests

OPENVOLT_API_KEY = "test-Z9EB05N-07FMA5B-PYFEE46-X4ECYAR"


async def get_rest_req(
    api_url: str, headers: dict = {}, params: dict = {}, validation: str = None
) -> dict:
    # Function to standardise handling Rest API get requests

    headers["Accept"] = "application/json"

    try:
        # Invoke the requests call to make the API request
        response = requests.get(api_url, headers=headers, params=params)

        # If all ok send back the response otherwise log it and send back None
        if response.status_code == 200:
            if validation and validation in response.json():
                return response.json()
            else:
                raise AssertionError(
                    f" API Response validation error, '{validation}' missing"
                )
        else:
            raise ValueError(
                f"Error making REST request: {response.text}, url: {api_url}, headers: {headers}, status code: {response.status_code}"
            )

    except requests.exceptions.ConnectionError as e:
        logging.error(f"Error creating REST API Connection {e}")
        raise (e)
    except Exception as e:
        logging.error(f"Error while completing REST API request {e}")
        raise (e)


async def get_meters(
    customer_id: str = None, meter_id: int = None, status: str = "active"
) -> str:
    # Get a list of meters based on customer_id and/or meter_id

    params = {}
    meters = {}

    if not customer_id and not meter_id:
        logging.error("Tried to get list of meters without a customer_id or meter_id")
        return None

    if customer_id:
        params["customer_id"] = customer_id
    if meter_id:
        params["meter_id"] = meter_id
    if status:
        params["status"] = status

    api_url = f"https://api.openvolt.com/v1/meters"
    headers = {"x-api-key": OPENVOLT_API_KEY}

    meters_json = await get_rest_req(
        api_url, headers=headers, params=params, validation="data"
    )

    for meter in meters_json["data"]:
        meters[meter["_id"]] = meter

    return meters


async def get_meter_interval_data(
    start_date: datetime, end_date: datetime, meter_id: str
) -> dict:
    # Get interval data for specific meter

    meter_interval_data = {}

    api_url = f"https://api.openvolt.com/v1/interval-data?granularity=hh&meter_id={meter_id}&start_date={start_date.isoformat()}&end_date={end_date.isoformat()}"
    headers = {"x-api-key": OPENVOLT_API_KEY}

    meter_interval_data_json = await get_rest_req(
        api_url=api_url, headers=headers, validation="data"
    )

    # Validate intervals returned are actually within our time window and standardise the timestamp as interval identifier
    for interval in meter_interval_data_json["data"]:
        if (
            datetime.strptime(
                helper.trim_timestamp(interval["start_interval"]), "%Y-%m-%dT%H%M"
            )
            >= start_date
            and datetime.strptime(
                helper.trim_timestamp(interval["start_interval"]), "%Y-%m-%dT%H%M"
            )
            <= end_date
        ):
            meter_interval_data[
                helper.trim_timestamp(interval["start_interval"])
            ] = interval
        else:
            logging.warn("Meter Interval falls outside time scope")
            logging.warn(interval)

    return meter_interval_data


async def get_generation_mix_data(
    start_date: datetime,
    end_date: datetime,
    postcode_region: str = None,
) -> dict:
    # Generate the interval data for fuel used to produce power from the National Grid

    generation_mix_data = {}

    # Increase our end time by 30 mins including additional interval to sync with OpenVolt intervals
    end_date += timedelta(minutes=30)

    # Use the postcode from the meter address to get regional data

    postcode_region = None

    # !!!NOTE the regional data by postcode seems to require authorisation (Which I don't have)
    # Spec says it should work unauthorised...but to be fair it's in beta so it is what it is
    # Catered for a retry using national dataset, this is ok for testing but should really have
    # some sort of flag so moving to national dataset isn't implicit

    if postcode_region:
        api_url = f"https://api.carbonintensity.org.uk/regional/{start_date.isoformat()}/{end_date.isoformat()}/postcode/{postcode_region}"
    else:
        api_url = f"https://api.carbonintensity.org.uk/generation/{start_date.isoformat()}/{end_date.isoformat()}"

    try:
        generation_mix_data_json = await get_rest_req(
            api_url=api_url, validation="data"
        )
    except ValueError as e:
        # If regional by postcode dataset fails retry using the national dataset

        if postcode_region:
            logging.error(
                "Couldn't retrieve National Grid data for local region, retrying using national stats"
            )
            api_url = f"https://api.carbonintensity.org.uk/generation/{start_date.isoformat()}/{end_date.isoformat()}"
            generation_mix_data_json = await get_rest_req(
                api_url=api_url, validation="data"
            )
        else:
            raise (e)
    except Exception as e:
        raise (e)

    # Validate intervals returned are actually within our time window and standardise the timestamp as interval identifier

    for interval in generation_mix_data_json["data"]:
        if (
            datetime.strptime(helper.trim_timestamp(interval["from"]), "%Y-%m-%dT%H%M")
            >= start_date
            and datetime.strptime(
                helper.trim_timestamp(interval["from"]), "%Y-%m-%dT%H%M"
            )
            <= end_date
        ):
            generation_mix_interval_entry = {}
            for entry in interval["generationmix"]:
                generation_mix_interval_entry[entry["fuel"]] = entry["perc"]
            generation_mix_data[
                helper.trim_timestamp(interval["from"])
            ] = generation_mix_interval_entry

    return generation_mix_data


async def get_carbon_emission_factors() -> dict:
    # Get carbon emissions data for fuel types from National Grid

    carbon_emission_factors = {}

    api_url = f"https://api.carbonintensity.org.uk/intensity/factors"

    carbon_emission_factors_json = await get_rest_req(
        api_url=api_url, validation="data"
    )

    # Create initial dict to store emission factors, convert keys to lowercase for consistency
    for key, value in carbon_emission_factors_json["data"][0].items():
        carbon_emission_factors[key.lower()] = value

    # !!!NOTE NationalGrid give emissions factors with Gas and Imports broken down to more granular sources
    # Obviously this would be a discussion interally to agree how to address (or to chase NG for clarity/API update)
    # For now for Imports and Gases I'm just averaging them into their generic buckets even though it's not pretty

    # Create combined "import" emissions figure
    overall_import_emissions = 0
    overall_import_count = 0

    for fuel_type in carbon_emission_factors:
        if "imports" in fuel_type:
            overall_import_count += 1
            overall_import_emissions += carbon_emission_factors[fuel_type]

    if overall_import_count > 1:
        logging.debug(
            f"Found {overall_import_count} Import fuel types, Combining and averaging into Imports fuel type"
        )
        carbon_emission_factors["imports"] = (
            overall_import_emissions / overall_import_count
        )

    # Create combined "gas" emissions figure
    overall_gas_emissions = 0
    overall_gas_count = 0
    for fuel_type in carbon_emission_factors:
        if "gas" in fuel_type.lower():
            overall_gas_count += 1
            overall_gas_emissions += carbon_emission_factors[fuel_type]

    if overall_gas_count > 1:
        logging.debug(
            f"Found {overall_gas_count} Gas fuel types, Combining and averaging into Gas fuel type"
        )
        carbon_emission_factors["gas"] = overall_gas_emissions / overall_gas_count

    return carbon_emission_factors


def validate_openvolt_nationalgrid_datasets(
    meter_interval_data: json, generation_mix_data: json
) -> bool:
    # Validates both the OpenVolt and NationalGrid
    # Intervals align and flags if not

    meter_interval_timestamps = []
    generation_mix_timestamps = []

    # Pull the timestamps from both OpenVolt and NG into two defined lists
    for interval in meter_interval_data:
        meter_interval_timestamps.append(interval)

    for interval in generation_mix_data:
        generation_mix_timestamps.append(interval)

    # Compare both lists against each other to spot gaps with interval slots
    meter_interval_diff = set(meter_interval_timestamps) - set(
        generation_mix_timestamps
    )
    generation_mix_interval_diff = set(generation_mix_timestamps) - set(
        meter_interval_timestamps
    )

    logging.info(
        f"Dataset validation Openvolt:NationalGrid, Openvolt Unmatched:{len(meter_interval_diff)} NationalGrid Unmatched:{len(generation_mix_interval_diff)}"
    )

    # Pass the check only giving a warning if NationalGrid have given us additional intervals
    if generation_mix_interval_diff:
        logging.warning(
            f"Dataset validation Openvolt:NationalGrid, found {len(meter_interval_diff)} intervals missing from National Grid"
        )
        logging.debug(
            f"Could not find following timestamps in OpenVolt dataset: {generation_mix_interval_diff}"
        )

    # Fail the check if NationalGrid has missing intervals in their dataset
    if meter_interval_diff:
        logging.error(
            f"Dataset validation Openvolt:NationalGrid, found {len(meter_interval_diff)} intervals missing from Openvolt"
        )
        logging.debug(
            f"Could not find following timestamps in NationalGrid dataset: {meter_interval_diff}"
        )
        return False

    return True
