# NEM12 Data Loader

This CLI program will process in a local or remote NEM12 file and and generates `INSERT` statements for `meter_readings` table.

## CLI Options

* `--filepath`: path to NEM12 file.
* `--output-format`: either output to csv file or sql file with `INSERT` statements
* `--num-workers`: number of workers to process NMIs


## NEM12 Data Format

NEM12 is a file format for storing interval metering data for a NMI (National Meter Identifier).


### NMI Data Details (200)

* Multiple 300-500 record blocks are allowed within a single 200 record.
* If any data changes in the 200 record, a new 200 record must be provided for the subsequent 300 record (e.g. if the UOM, IntervalLength or NMISuffix changes).

| Column | Field | Format | Example | Description |
|--------|-------|--------|---------|-------------|
| 0 | RecordIndicator | Numeric(3)| 200 | NMI data details record indicator (allowed: 200)|
| 1 | NMI |  Char(10) | NEM1201009 | NMI for the connection point. |
| 8 | IntervalLength | Numeric(2) | 30 | Time in minutes of each interval period: 5, 15, 30. |


### Interval Meter Data Record (300)

* 300 records must be presented in date sequential order

| Column | Field | Format | Example | Description |
|--------|-------|--------|---------|-------------|
| 0 | RecordIndicator | Numeric(3)| 200 | NMI data details record indicator (allowed: 300)|
| 1 | IntervalDate | Date(8)| 20050301 | Date of the interval meter reading data, date format is `YYYYMMDD` |
| 2-49 | IntervalValues | Numeric | 20050301 | The total amount of energy or other measured value for the Interval inclusive of any multiplier or scaling factor. The number of values provided must equal 1440 divided by the IntervalLength. This is a repeating field with individual field values separated by comma delimiters |


* The first Interval (1) for a meter programmed to record 30-minute interval metering data would relate to the period ending 00:30 of the `IntervalDate`.
* The last Interval (48) for a meter programmed to record 30-minute interval metering data would relate to the period ending 00:00 of the `IntervalDate+1`.

## SQL Database Schema

```sql
create table meter_readings (
    id uuid default gen_random_uuid() not null,
    "nmi" varchar(10) not null,
    "timestamp" timestamp not null,
    "consumption" numeric not null,
    constraint meter_readings_pk primary key (id),
    constraint meter_readings_unique_consumption unique ("nmi", "timestamp")
);
```

| Field         | Description                          |
|---------------|--------------------------------------|
| `nmi`         | NMI for the connection point         |
| `consumption` | sum of energy usage of all intervals |
| `timestamp`   | date of consumption                  |



## Household Statistics and NEM12 file sizes

The table below lists the number of households in various cities and the estimated sizes of their NEM12 files for a period of 30 days, with E1E2 NMI Configuration.

* NME12 File Size = `30 * (number of households) * (86B) * 2`


| City      | Households | Estimated Data Size |
|-----------|------------|---------------------|
| Singapore | 1,425,100  |      7.35 GB        |
| Sydney    | 2,076,284  |      10.71 GB       |
| Tokyo     | 6,946,000  |      35.841 GB      |


## References

* https://www.energyaustralia.com.au/resources/PDFs/User%20Guide_v3.pdf
* https://devblogs.microsoft.com/cosmosdb/scaling-iot-time-series-metering-workloads-with-azure-cosmos-db-for-postgresql/
* https://github.com/aguinane/nem-reader