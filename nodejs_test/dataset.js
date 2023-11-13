const http = require('https');
const helper = require('./helper.js');

const OPENVOLT_API_KEY = "test-Z9EB05N-07FMA5B-PYFEE46-X4ECYAR"

function get_rest_req(url, headers, parameters, validation) {

    // Function to standardise handling Rest API get requests

    return new Promise((resolve, reject) => {

        var options = {};

        // TODO replace this to set the http/https port
        url = url.replace("https://", "");

        // Set up options dict to be used by http.get
        [options['host'], ...options['path']] = url.split("/");
        options.path = "/" + options.path.join("/");
        options['port'] = 443;
        options['headers'] = headers;

        if (options.headers === undefined) {
            options.headers = {};
        }
        options.headers['Accept'] = "application/json";

        // Create parameters string to append to url
        if (parameters !== undefined) {
            var parameters_string = "?";
            for (const [key, value] of Object.entries(parameters)) {
                parameters_string = parameters_string + "&" + key + "=" + value;
            }
            parameters_string = parameters_string.replace("?&", "?");
            options.path = options.path + parameters_string;
        }

        console.logstamp(`Executing API request for ${options.path}`);

        // Invoke the requests call to make the API request

        var req = http.get(options, (res) => {
            var body = '';
            res.on('data', (chunk) => {
                body = body + chunk;
            }).on('end', () => {
                if (validation === undefined) {
                    resolve(body)
                } else {
                    let response = JSON.parse(body)
                    if (validation in response) {
                        resolve(response);
                    } else {
                        reject(`Validation Error, missing ${validation}`);
                    }
                }

            }).on('error', (err) => {
                reject(err);
            });
        });
    });
}

async function get_meters(customer_id, meter_id, status = "active") {

    // Get a list of meters based on customer_id and / or meter_id

    meters = {}

    rest_url = 'https://api.openvolt.com/v1/meters';
    headers = { "x-api-key": OPENVOLT_API_KEY };
    parameters = {};
    validation = "data";

    if (customer_id !== undefined && meter_id !== undefined) {
        throw ("get_meters needs either a customer_id or meter_id specifed");
    }

    if (customer_id !== undefined) { parameters['customer_id'] = customer_id; }
    if (meter_id !== undefined) { parameters['meter_id'] = meter_id; }
    parameters['status'] = status;

    await get_rest_req(rest_url,
        headers,
        parameters,
        validation
    ).then((values, res) => {
        meters_json = values;
    });

    for (const meter of meters_json.data) {
        meters[meter['_id']] = meter;
    }

    return meters;
}

async function get_meter_interval_data(start_date, end_date, meter_id) {

    // Get interval data for specific meter

    meter_interval_data = {};

    rest_url = 'https://api.openvolt.com/v1/interval-data';
    headers = { "x-api-key": OPENVOLT_API_KEY };
    parameters = {
        "granularity": "hh",
        "meter_id": meter_id,
        "start_date": start_date,
        "end_date": end_date
    };
    validation = "data";

    await get_rest_req(rest_url,
        headers,
        parameters,
        validation
    ).then((values, res) => {
        meter_interval_data_json = values;
    });

    // Validate intervals returned are actually within our time window and standardise the timestamp as interval identifier

    for (const interval of meter_interval_data_json.data) {
        if (Date(interval.start_interval) >= Date(start_date) && Date(interval.start_interval) <= Date(end_date)) {
            meter_interval_data[helper.trim_timestamp(interval.start_interval)] = interval;
        } else {
            console.logstamp("Meter interval date falls outside scope:");
            console.logstamp(interval);
        }
    }

    return meter_interval_data;
}

async function get_generation_mix_data(start_date, end_date, postcode_region) {

    // Generate the interval data for fuel used to produce power from the National Grid

    generation_mix_data = {};

    // Increase our end time by 30 mins including additional interval to sync with OpenVolt intervals
    extended_datetime = new Date(end_date);
    extended_datetime = new Date(extended_datetime.getTime() + 30 * 60000);
    end_date = helper.trim_timestamp(extended_datetime.toISOString());

    //!!!NOTE the regional data by postcode seems to require authorisation(Which I don't have)
    // Spec says it should work unauthorised...but to be fair it's in beta so it is what it is
    // Catered for a retry using national dataset, this is ok for testing but should really have
    // some sort of flag so moving to national dataset isn't implicit 

    // Use the postcode from the meter address to get regional data

    if (postcode_region !== undefined) {
        rest_url = "https://api.carbonintensity.org.uk/regional/" + start_date + "/" + end_date + "/postcode/" + postcode_region;
    } else {
        rest_url = "https://api.carbonintensity.org.uk/generation/" + start_date + "/" + end_date;
    }
    headers = undefined;
    parameters = undefined;
    validation = "data";

    try {
        await get_rest_req(rest_url,
            headers,
            parameters,
            validation
        ).then((values, res) => {
            generation_mix_data_json = values;
        });
    } catch (error) {
        console.logstamp(`Error retrieving generation mix data for ${rest_url}`)

        // If regional by postcode dataset fails retry using the national dataset
        if (postcode_region !== undefined) {

            console.logstamp("Couldn't find regional generation mix data, retrying with national dataset...");

            rest_url = "https://api.carbonintensity.org.uk/generation/" + start_date + "/" + end_date;
            await get_rest_req(rest_url,
                headers,
                parameters,
                validation
            ).then((values, res) => {
                generation_mix_data_json = values;
            });
        }

    }

    // Validate intervals returned are actually within our time window and standardise the timestamp as interval identifier
    for (const interval of generation_mix_data_json.data) {
        if (Date(interval.from) >= Date(start_date) && Date(interval.from) <= Date(end_date)) {

            var generation_mix_interval_entry = {};
            for (const [index, entry] of Object.entries(interval['generationmix'])) {
                generation_mix_interval_entry[entry.fuel] = entry.perc;
            }

            generation_mix_data[helper.trim_timestamp(interval.from)] = generation_mix_interval_entry;
        } else {
            console.logstamp("Generation Mix interval date falls outside scope:");
            console.logstamp(interval);
        }
    }

    return generation_mix_data;
}

async function get_carbon_emission_factors() {

    // Get carbon emissions data for fuel types from National Grid
    carbon_emission_factors = {};

    rest_url = 'https://api.carbonintensity.org.uk/intensity/factors';
    headers = undefined;
    parameters = undefined;
    validation = "data";

    await get_rest_req(rest_url,
        headers,
        parameters,
        validation
    ).then((values, res) => {
        carbon_emission_factors_json = values;
    });

    // Create initial dict to store emission factors, convert keys to lowercase for consistency
    for (let [key, value] of Object.entries(carbon_emission_factors_json.data[0])) {
        carbon_emission_factors[key.toLowerCase()] = value;
    }


    //!!!NOTE NationalGrid give emissions factors with Gas and Imports broken down to more granular sources
    // Obviously this would be a discussion interally to agree how to address(or to chase NG for clarity / API update)
    // For now for Imports and Gases I'm just averaging them into their generic buckets even though it's not pretty

    // Create combined "import" emissions figure
    var overall_import_emissions = 0;
    var overall_import_count = 0;

    for (const fuel_type in carbon_emission_factors) {
        if (fuel_type.includes("imports")) {
            overall_import_count++;
            overall_import_emissions = overall_import_emissions + carbon_emission_factors[fuel_type];
        }
    }

    if (overall_import_count > 1) {
        carbon_emission_factors['imports'] = Number((overall_import_emissions / overall_import_count));
    }

    // Create combined "gas" emissions figure
    var overall_gas_emissions = 0;
    var overall_gas_count = 0;

    for (let fuel_type in carbon_emission_factors) {
        if (fuel_type.includes("gas")) {
            overall_gas_count++;
            overall_gas_emissions = overall_gas_emissions + carbon_emission_factors[fuel_type];
        }
    }

    if (overall_gas_count > 1) {
        carbon_emission_factors['gas'] = Number((overall_gas_emissions / overall_gas_count));
    }

    return carbon_emission_factors;
}

function validate_openvolt_nationalgrid_datasets(meter_interval_data, generation_mix_data) {

    // Validates both the OpenVolt and NationalGrid Intervals align and flags if not

    missing_intervals = [];

    // Iterate through generation mix data to highlight any intervals available but not in the meter interval data
    console.logstamp("Checking Generation Mix to ensure all intervals present in Meter Intervals...")
    for (const interval in generation_mix_data) {
        if (!(interval in meter_interval_data)) {
            missing_intervals.push(interval);
        }
    }

    // Only give a warning if NationalGrid has intervals which are missing from OpenVolt
    if (missing_intervals.length === 0) {
        console.logstamp("Generation Mix data validated successfully");
    } else {
        console.logstamp("Generation Mix data missing timestamps " + missing_intervals);
    }

    console.logstamp("Checking Meter Intervals to ensure all intervals present in Generation Mix...")
    missing_intervals = [];

    // Iterate through meter interval data to highlight any intervals available but not in the generation mix data
    for (const interval in meter_interval_data) {
        if (!(interval in generation_mix_data)) {
            missing_intervals.push(interval);
        }
    }

    // Fail the check if NationalGrid has missing intervals in their dataset
    if (missing_intervals.length === 0) {
        console.logstamp("Datasets validated successfully");
        return true;
    } else {
        console.logstamp("Datasets missing timestamps " + missing_intervals);
        return false;
    }
}

module.exports = { get_rest_req, get_carbon_emission_factors, get_meters, get_meter_interval_data, get_generation_mix_data, validate_openvolt_nationalgrid_datasets };