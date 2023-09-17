# ZEDI-Segregation


The repository has code that is exclusively written for the ZEDI project. 

A raw data file directly from the cloud base would be as follows:

Node No |Device ID|City|Location ID|ID|.......(Data Fields).....

IDs 1,2,3,4,5,6: Current-Voltage / Frequency data sampled at 1 minute
ID 7: Other sensor data sampled at 5 minutes


The segregator will generate the ZEDI files, which will be in the following format:

"|" is the separator used.

For 3 phase Current-voltage data:
Time|Curr(IR)|Volt(VR)|Curr(IY)|Volt(VY)|Curr(IB)|Volt(VB)

![image](https://github.com/24Lights/ZEDI-Segregation/assets/134679427/bd2360fc-c3e2-428d-aba5-58f31f623329)


For 3 phase frequency data:
Time|Freq(R)|Freq(Y)|Freq(B)

![image](https://github.com/24Lights/ZEDI-Segregation/assets/134679427/a8f1c6fe-889b-4eb4-97f2-511f0cad396f)


For 3 phase other sensor data, which include the Temperature, Humidity, Pressure, PIR1, PIR2, Co2, Therm1, Therm2, and Therm3:

![image](https://github.com/24Lights/ZEDI-Segregation/assets/134679427/88d4e393-ca98-47e3-a623-d404c361234f)

## The Segregator code 

This contains the segy.py file, which does the file segregation process from the raw data file to the .csv files in the orders, as mentioned earlier.

## The cleanser code

This contains the cleanser.py file, which sorts according to the date-time in each file, which the segregator generates.

PATCH 1: Integrated Multiprocessing in the segy.py. 
New benchmark - ~18k files segregated in 6 mins


PATCH 2: segy.py is now segy_patch1.py, and cleansing code.py is added

(Ongoing) : GPU integration and large-scale parallelization via the cloud.
(Ongoing) : Conversion of raw files of .txt to .parquet 





