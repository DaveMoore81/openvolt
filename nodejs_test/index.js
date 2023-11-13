const helper = require('./helper.js');
const dataset = require('./dataset.js');
const util = require('util');

function get_consumption_source_report(meter_interval_data, generation_mix_data) {

    // Create a report for kWh consumption using Meter Interval
    // and NG's Generation Mix datasets

    consumption_source = {};

    // Loop through each of the interval datasets for the meter
    for (const interval in meter_interval_data) {
        consumption_source[interval] = {};
        consumption_source[interval]['total'] = 0;

        if (meter_interval_data[interval].consumption_units.toUpperCase() !== 'KWH') {
            console.logstamp("Found non-standard consumption unit in interval " + interval);
            return undefined;
        }

        // Increase the total Kwh's with the intervals total usage
        consumption_source[interval]['total'] = Number(consumption_source[interval]['total']) + Number(meter_interval_data[interval].consumption);

        // Break the total consumption into individual fuel sources 
        // as per the NationalGrid's generation for the interval

        for (const fuel_type in generation_mix_data[interval]) {
            if (!(fuel_type in consumption_source[interval])) {
                consumption_source[interval][fuel_type] = 0;
            }

            // Add that percentage of kWh to the total for that specific fuel type
            consumption_source[interval][fuel_type] = consumption_source[interval][fuel_type] + (((meter_interval_data[interval].consumption) / 100) * generation_mix_data[interval][fuel_type]);
        }

    }

    // return report with total Kwh's per fuel type
    return consumption_source;
}

function get_carbon_emissions_report(meter_interval_data, consumption_source, carbon_emission_factors) {

    // Create a report for CO2(g) emission using Consumption Source report
    // and the carbon emission factors from NG

    carbon_emissions = {};

    // Loop through each of the interval datasets for the meter
    for (const interval in meter_interval_data) {

        carbon_emissions[interval] = {}

        if (meter_interval_data[interval].consumption_units.toUpperCase() !== 'KWH') {
            console.logstamp("Found non-standard consumption unit in interval " + interval);
            return undefined;
        }

        // Loop through each of the fuel types and calculate emissions for the interval
        carbon_emissions[interval]['total'] = 0;

        for (const fuel_type in consumption_source[interval]) {
            if (fuel_type !== "total") {
                if (!(fuel_type in carbon_emissions)) {
                    carbon_emissions[interval][fuel_type] = 0;
                }

                //  Calculate the carbon emissions and add them to current fuel type total and overall total
                generated_carbon = consumption_source[interval][fuel_type] * carbon_emission_factors[fuel_type];
                carbon_emissions[interval][fuel_type] = carbon_emissions[interval][fuel_type] + generated_carbon;
                carbon_emissions[interval]['total'] = carbon_emissions[interval].total + generated_carbon;
            }
        }

    }

    // Return raw report of emissions in gCO2/kWh
    return carbon_emissions;
}

async function generate_reports(start_date, end_date, customer_id, meter_id, validate_dataset) {

    // Main function to generate the required reports for the test scenario

    var consumption_source_report = {};
    var carbon_emissions_report = {};

    var consumption_source_report_totals = {};
    var carbon_emissions_report_totals = {};

    // Get a list of meters filtering on customer and/or meter id
    console.logstamp("Retrieving meter interval data...");
    meters = await dataset.get_meters(customer_id = customer_id, meter_id = meter_id);

    // Get the factors for emissions generated per Kwh for fuel types
    console.logstamp("Retrieving carbon emission factors...");
    carbon_emission_factors = await dataset.get_carbon_emission_factors();

    // Loop through the meters found to cover single customer with multiple meters
    for (const meter in meters) {

        // Pull the postcode from the address to use with regional NationalGrid data
        postcode_region = helper.uk_address_to_region(meters[meter].address);

        // Generate both the OpenVolt meter interval data and National Grid Generation / Emission data
        // Does these async as the rest api calls can benefit from running in parallel

        await Promise.all([dataset.get_meter_interval_data(start_date, end_date, meter),
        dataset.get_generation_mix_data(start_date, end_date, postcode_region)]).then((values) => {
            meter_interval_data = values[0];
            generation_mix_data = values[1];
        }
        )

        // Validate dataset to ensure each meter interval has a corresponding entry
        if (validate_dataset) {
            if (!(dataset.validate_openvolt_nationalgrid_datasets(meter_interval_data, generation_mix_data))) {
                console.logstamp("Missing intervals from NationalGrid dataset, dataset validation failed");
                return undefined;
            }
        }

        // Generate raw reports for both Consumption Source(Generation Mix) and Carbon Emissions for the OpenVolt meter data
        console.logstamp("Generating consumption source report...");
        consumption_source_report[meter] = get_consumption_source_report(meter_interval_data, generation_mix_data);
        console.logstamp("Generating carbon emissions report...");
        carbon_emissions_report[meter] = get_carbon_emissions_report(meter_interval_data, consumption_source_report[meter], carbon_emission_factors);

        console.logstamp("Finished Consumption Source and Carbon Emissions threads");

        console.logstamp("Building final report...");
        // Build the final dataset to deliver the required report
        consumption_source_report_totals[meter] = {};
        carbon_emissions_report_totals[meter] = {};

        // Loop through the intervals of the merged datasets to calculate totals
        for (const interval in consumption_source_report[meter]) {

            // Generate the consumption report, overall total kWh including breakdown by fuel type
            for (const fuel_type in consumption_source_report[meter][interval]) {
                if (!(fuel_type in consumption_source_report_totals[meter])) {
                    consumption_source_report_totals[meter][fuel_type] = 0;
                }
                consumption_source_report_totals[meter][fuel_type] = (consumption_source_report_totals[meter][fuel_type] + consumption_source_report[meter][interval][fuel_type]);
            }

            // Generate the Carbon Emissions generated (in KG of CO2 per kWh), overall total including breakdown
            for (const fuel_type in carbon_emissions_report[meter][interval]) {
                if (!(fuel_type in carbon_emissions_report_totals[meter])) {
                    carbon_emissions_report_totals[meter][fuel_type] = 0;
                }
                carbon_emissions_report_totals[meter][fuel_type] = carbon_emissions_report_totals[meter][fuel_type] + (carbon_emissions_report[meter][interval][fuel_type] / 1000);
            }
        }
    }

    // return the datasets ready to be consumed
    return [consumption_source_report_totals, carbon_emissions_report_totals];
}

function display_report(consumption_source_report_totals, carbon_emissions_report_totals, start_date, end_date) {

    console.log("\nReport for OpenVolt Test API\n");
    console.log(`Start Date: ${start_date} -> End Data: ${end_date}`);
    console.log("-------------------------------");

    for (const meter in consumption_source_report_totals) {
        console.log(`\nMeter ID ${meter}\n`);

        console.log(`Total Consumption: ${Number(consumption_source_report_totals[meter]['total']).toFixed(2)} kWh`);
        for (const fuel_type in consumption_source_report_totals[meter]) {
            if (fuel_type !== "total") {
                console.log(`  ${fuel_type} ${Number(consumption_source_report_totals[meter][fuel_type]).toFixed(2)} kWh  (${helper.percent(consumption_source_report_totals[meter][fuel_type], consumption_source_report_totals[meter]['total'])} %)`);
            }
        }

        console.log(`\n\nTotal Emissions: ${Number(carbon_emissions_report_totals[meter]['total']).toFixed(2)} CO2 kg's `)
        for (const fuel_type in carbon_emissions_report_totals[meter]) {
            if (fuel_type !== "total") {
                console.log(`  ${fuel_type} ${Number(carbon_emissions_report_totals[meter][fuel_type]).toFixed(2)} CO2 kg's  (${helper.percent(carbon_emissions_report_totals[meter][fuel_type], carbon_emissions_report_totals[meter]['total'])} %)`);
            }
        }
    }
    console.log("\n");

}

function process_cmdline_parser() {

    // Set up the parser to accept command line argument

    const args = util.parseArgs({
        options: {
            customerid: {
                type: "string",
                short: "c"
            },
            meterid: {
                type: "string",
                short: "m"
            },
            startdate: {
                type: "string",
                short: "s"
            },
            enddate: {
                type: "string",
                short: "e"
            },
            help: {
                type: "boolean",
                short: "h"
            }
        }
    })

    if ("help" in args.values) {
        console.log("\nOpenVolt API Test\n");
        console.log("Usage: node index.js [-h] [-c/-customerid CUSTOMERID] [-m/-meterid METERID] [-s/-startdate STARTDATE] [-e/-enddate ENDDATE]\n\n");
        process.exit();
    }

    return args;
}

async function main() {

    console.log("\nOpenVolt API Test\n");

    app_start_time = Date.now();

    // Get list of command line arguements
    args = process_cmdline_parser();


    // Set default target customer/meter and timeframe's to match the test scope
    if ("startdate" in args.values) {
        var start_date = new Date(args.values['startdate']).toISOString();
    } else {
        var start_date = "2023-01-01";
        console.logstamp("Missing startdate, set to default: " + start_date);
    }

    if ("enddate" in args.values) {
        var end_date = new Date(args.values['enddate']).toISOString();
    } else {
        var end_date = "2023-02-01";
        console.logstamp("Missing enddate, set to default: " + end_date);
    }

    if ("customerid" in args.values) {
        var customer_id = args.values['customerid'];
    } else {
        var customer_id = undefined;
    }

    if ("meterid" in args.values) {
        var meter_id = args.values['meterid'];
    } else {
        var meter_id = undefined;
    }

    if (!("customerid" in args.values) && !("meterid" in args.values)) {
        var customer_id = "6514153c23e3d1424bf82738";
        console.logstamp("Missing customerid and meterid parameters, customerid set to test customer id: " + args['customerid']);
    }

    console.logstamp("start_date: " + start_date + " end_date: " + end_date + " customer_id: " + customer_id + " meter_id: " + meter_id);

    // Generate the reports as per requirements

    console.logstamp("Starting Report Generation...");
    var [consumption_source_report_totals, carbon_emissions_report_totals] = await generate_reports(start_date, end_date, customer_id, meter_id);

    // Dump the raw data to debug

    console.logstamp("Consumption Source Report Data:");
    console.logstamp(consumption_source_report_totals);

    console.logstamp("Carbon Emissions Report Data:");
    console.logstamp(carbon_emissions_report_totals);

    // Display final report
    display_report(consumption_source_report_totals, carbon_emissions_report_totals, start_date, end_date)

    app_end_time = Date.now();
    console.logstamp(`${((app_end_time - app_start_time) / 1000)} seconds runtime`);

}

// Add timestamp to console.logstamp
console.logstamp = helper.logstamp();

main();

