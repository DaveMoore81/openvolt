import unittest
from unittest.mock import patch

import openvolt_reporting

from datetime import datetime


class TestReporting(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.start_date = datetime.strptime("2023-01-01 00:00", "%Y-%m-%d %H:%M")
        self.end_date = datetime.strptime("2023-01-01 02:00", "%Y-%m-%d %H:%M")
        self.customer_id = "12345678901234567890"

        self.meters = {
            "1234": {
                "_id": "1234",
                "object": "meter",
                "account": "12312312c83234eb77b943c2",
                "meter_number": "11111111111",
                "customer": {
                    "_id": "12345678901234567890",
                    "object": "customer",
                    "account": "987654321987654321",
                    "name": "Daves Warehouse",
                    "email": "daveydave@dave.com",
                    "address": "11 Downing St, London SW1A 2AB, UK",
                    "notes": [],
                    "created_at": "2023-09-27T11:42:52.845Z",
                    "__v": 0,
                },
                "address": "123 Fake Street, London, SW1A 3AB",
                "update_frequency": "weekly",
                "data_source": "electralink",
                "status": "active",
                "notes": [],
                "created_at": "2023-09-27T11:48:02.979Z",
                "__v": 0,
                "description": "Daves Personal Test Meter",
            }
        }

        self.meters_empty = {}

        self.carbon_emission_factors = {
            "biomass": 120,
            "coal": 937,
            "dutch imports": 474,
            "french imports": 53,
            "gas (combined cycle)": 394,
            "gas (open cycle)": 651,
            "hydro": 0,
            "irish imports": 458,
            "nuclear": 0,
            "oil": 935,
            "other": 300,
            "pumped storage": 0,
            "solar": 0,
            "wind": 0,
            "imports": 328.3333333333333,
            "gas": 522.5,
        }

        self.meter_interval_data = {
            "2023-01-01T0000": {
                "start_interval": "2023-01-01T00:00:00.000Z",
                "meter_id": "6514167223e3d1424bf82742",
                "meter_number": "9999999999999",
                "customer_id": "6514153c23e3d1424bf82738",
                "consumption": "54",
                "consumption_units": "kWh",
            },
            "2023-01-01T0030": {
                "start_interval": "2023-01-01T00:30:00.000Z",
                "meter_id": "6514167223e3d1424bf82742",
                "meter_number": "9999999999999",
                "customer_id": "6514153c23e3d1424bf82738",
                "consumption": "54",
                "consumption_units": "kWh",
            },
            "2023-01-01T0100": {
                "start_interval": "2023-01-01T01:00:00.000Z",
                "meter_id": "6514167223e3d1424bf82742",
                "meter_number": "9999999999999",
                "customer_id": "6514153c23e3d1424bf82738",
                "consumption": "54",
                "consumption_units": "kWh",
            },
            "2023-01-01T0130": {
                "start_interval": "2023-01-01T01:30:00.000Z",
                "meter_id": "6514167223e3d1424bf82742",
                "meter_number": "9999999999999",
                "customer_id": "6514153c23e3d1424bf82738",
                "consumption": "54",
                "consumption_units": "kWh",
            },
            "2023-01-01T0200": {
                "start_interval": "2023-01-01T02:00:00.000Z",
                "meter_id": "6514167223e3d1424bf82742",
                "meter_number": "9999999999999",
                "customer_id": "6514153c23e3d1424bf82738",
                "consumption": "54",
                "consumption_units": "kWh",
            },
        }
        self.generation_mix_data = {
            "2023-01-01T0000": {
                "biomass": 1.7,
                "coal": 2.5,
                "imports": 6.7,
                "gas": 11.2,
                "nuclear": 21.5,
                "other": 0,
                "hydro": 1.3,
                "solar": 0,
                "wind": 55.1,
            },
            "2023-01-01T0030": {
                "biomass": 2.1,
                "coal": 2.6,
                "imports": 6.3,
                "gas": 12.1,
                "nuclear": 21.1,
                "other": 0,
                "hydro": 1.1,
                "solar": 0,
                "wind": 54.6,
            },
            "2023-01-01T0100": {
                "biomass": 2.2,
                "coal": 2.3,
                "imports": 3.9,
                "gas": 11,
                "nuclear": 21.7,
                "other": 0,
                "hydro": 1.1,
                "solar": 0,
                "wind": 57.8,
            },
            "2023-01-01T0130": {
                "biomass": 1.8,
                "coal": 1.9,
                "imports": 2.8,
                "gas": 10.8,
                "nuclear": 22.5,
                "other": 0,
                "hydro": 1.1,
                "solar": 0,
                "wind": 59,
            },
            "2023-01-01T0200": {
                "biomass": 1.8,
                "coal": 1.9,
                "imports": 3.4,
                "gas": 10.8,
                "nuclear": 22.7,
                "other": 0,
                "hydro": 1.1,
                "solar": 0,
                "wind": 58.2,
            },
        }

        self.consumption_source_report_totals = {
            "1234": {
                "total": 270,
                "biomass": 5.184000000000001,
                "coal": 6.048,
                "imports": 12.474000000000002,
                "gas": 30.186000000000003,
                "nuclear": 59.13000000000001,
                "other": 0.0,
                "hydro": 3.0780000000000003,
                "solar": 0.0,
                "wind": 153.738,
            }
        }
        self.carbon_emissions_report_totals = {
            "1234": {
                "total": 26.156871000000002,
                "biomass": 0.6220800000000002,
                "coal": 5.666976000000001,
                "imports": 4.09563,
                "gas": 15.772185,
                "nuclear": 0.0,
                "other": 0.0,
                "hydro": 0.0,
                "solar": 0.0,
                "wind": 0.0,
            }
        }

    @patch("dataset.get_meters")
    @patch("dataset.get_carbon_emission_factors")
    @patch("dataset.get_meter_interval_data")
    @patch("dataset.get_generation_mix_data")
    @patch("dataset.validate_openvolt_nationalgrid_datasets")
    async def test_happy_path(
        self,
        validate_openvolt_ng_datasets,
        get_generation_mix_data,
        get_meter_interval_data,
        get_carbon_emission_factors,
        get_meters,
    ):
        get_meters.return_value = self.meters
        get_carbon_emission_factors.return_value = self.carbon_emission_factors
        get_meter_interval_data.return_value = self.meter_interval_data
        get_generation_mix_data.return_value = self.generation_mix_data
        validate_openvolt_ng_datasets.return_value = True

        (
            test_consumption_source_report_totals,
            test_carbon_emissions_report_totals,
        ) = await openvolt_reporting.generate_reports(
            self.start_date,
            self.end_date,
            self.customer_id,
            meter_id=None,
            output_file=None,
        )

        get_meters.assert_called_once()
        get_carbon_emission_factors.assert_called_once()
        get_meter_interval_data.assert_called_once()
        get_generation_mix_data.assert_called_once()
        validate_openvolt_ng_datasets.assert_called_once()

        self.assertDictEqual(
            test_consumption_source_report_totals,
            self.consumption_source_report_totals,
            "Consumption source TOTAL results are wrong",
        )

        self.assertDictEqual(
            test_carbon_emissions_report_totals,
            self.carbon_emissions_report_totals,
            "Carbon emission TOTAL results are wrong",
        )


if __name__ == "__main__":
    unittest.main()
