# Data Folder

This folder is intended to store raw and reference data for the NYC Green Taxi Data Warehouse.

> **Note:** Raw data files are **not included** in this repository due to size.

Please refer to the official source to download the data:  
[NYC TLC Green Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)


## Data Catalog
| **Field Name**          | **Description**                                               | **Values / Notes**                                                                                                                 |
| ----------------------- | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `VendorID`              | LPEP provider that provided the record                        | 1 = Creative Mobile Technologies, LLC<br>2 = Curb Mobility, LLC<br>6 = Myle Technologies Inc                                       |
| `lpep_pickup_datetime`  | Date and time when the meter was engaged                      | Timestamp                                                                                                                          |
| `lpep_dropoff_datetime` | Date and time when the meter was disengaged                   | Timestamp                                                                                                                          |
| `store_and_fwd_flag`    | Indicates if trip was stored in vehicle memory before sending | `Y` = Yes, `N` = No                                                                                                                |
| `RatecodeID`            | Final rate code at end of trip                                | 1 = Standard rate<br>2 = JFK<br>3 = Newark<br>4 = Nassau/Westchester<br>5 = Negotiated fare<br>6 = Group ride<br>99 = Null/unknown |
| `PULocationID`          | TLC Taxi Zone where meter was engaged                         | Integer → join with Taxi Zone lookup                                                                                               |
| `DOLocationID`          | TLC Taxi Zone where meter was disengaged                      | Integer → join with Taxi Zone lookup                                                                                               |
| `passenger_count`       | Number of passengers in the vehicle                           | Integer                                                                                                                            |
| `trip_distance`         | Trip distance in miles (taximeter reading)                    | Decimal                                                                                                                            |
| `fare_amount`           | Time-and-distance fare calculated by meter                    | Decimal                                                                                                                            |
| `extra`                 | Miscellaneous extras and surcharges                           | Decimal                                                                                                                            |
| `mta_tax`               | Automatic tax based on metered rate                           | Decimal                                                                                                                            |
| `tip_amount`            | Tip amount (credit card only)                                 | Decimal                                                                                                                            |
| `tolls_amount`          | Total tolls paid during trip                                  | Decimal                                                                                                                            |
| `improvement_surcharge` | Improvement surcharge at flag drop (since 2015)               | Decimal                                                                                                                            |
| `total_amount`          | Total amount charged (excludes cash tips)                     | Decimal                                                                                                                            |
| `payment_type`          | Payment method                                                | 0 = Flex Fare trip<br>1 = Credit card<br>2 = Cash<br>3 = No charge<br>4 = Dispute<br>5 = Unknown<br>6 = Voided trip                |
| `trip_type`             | Type of trip                                                  | 1 = Street-hail<br>2 = Dispatch                                                                                                    |
| `congestion_surcharge`  | NYS congestion surcharge collected for the trip               | Decimal                                                                                                                            |
| `cbd_congestion_fee`    | MTA Congestion Relief Zone fee (from Jan 5, 2025)             | Decimal                                                                                                                            |
