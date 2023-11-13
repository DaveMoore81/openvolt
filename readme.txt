Openvolt Test API

Python version in python_test, to run switch to that directory:

	python openvolt_reporting.py
	
By default it will run with a start and end date covering January 2023
It will also default to Start Industries Customer_id
To alter input run with a -h switch for help.

	python openvolt_reporting.py -h 

Python extras include an example unit test, to run switch to main python directory:

	python -m unittest

Other addition to the python version is the -o/-output flag which will export the datasets
to csv files so they can be used for validation. 
As both the Javascript and Python versions provide the same output this was used to validate both.

----

Node.js Javascript version in nodejs_test, to run switch to that directory:

	node index.js
	
By default it will run with a start and end date covering January 2023
It will also default to Start Industries Customer_id
To alter input run with a -h switch for help.

----

Final Report Output

Report for OpenVolt Test API

Start Date: 2023-01-01 00:00:00 -> End Data: 2023-02-01 00:00:00
-------------------------------

Meter ID 6514167223e3d1424bf82742

Total Consumption: 100340 kWh
  biomass 4557.58 kWh (4.54 %)
  coal 1836.02 kWh (1.83 %)
  imports 9367.37 kWh (9.34 %)
  gas 27650.41 kWh (27.56 %)
  nuclear 14849.51 kWh (14.8 %)
  other 0.0 kWh (0.0 %)
  hydro 2806.46 kWh (2.8 %)
  solar 1409.08 kWh (1.4 %)
  wind 37864.48 kWh (37.74 %)


Total Emissions: 19790.22 CO2 kg's
  biomass 546.91 CO2 kg's (2.76 %)
  coal 1720.35 CO2 kg's (8.69 %)
  imports 3075.62 CO2 kg's (15.54 %)
  gas 14447.34 CO2 kg's (73.0 %)
  nuclear 0.0 CO2 kg's (0.0 %)
  other 0.0 CO2 kg's (0.0 %)
  hydro 0.0 CO2 kg's (0.0 %)
  solar 0.0 CO2 kg's (0.0 %)
  wind 0.0 CO2 kg's (0.0 %)


2023-11-13 11:47:20 INFO - 8.019006  seconds runtime

