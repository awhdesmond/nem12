# NEM12 Data Loader

* Please ensure the implementation is prepared to handle files of very large sizes.
* For this assignment success would mean as close to production grade implementation as possible, which means
    * idiomatic code structuring,
    * readability,
    * performance optimization,
    * basic testing,
    * and anything else that you deem feasible.


Functional requirements for energy metering systems includes:

* Scalable ingestion
* Low latency ad-hoc queries for individual meters
* Analytics and aggregations


## CLI Options

This CLI program will process a local NEM12 file and and generates `INSERT` statements for `meter_readings` table.

| Option | Description |
|--------|-------------|
| `--filepath` | path to NEM12 file |
| `--output-format` | `csv` or `sql`. If `sql` is selected, it will generate text files with `INSERT` statements |

## NEM12 Data Format

NEM12 is a csv file format for distributing metering data for a NMI (National Meter Identifier) as specified by the Australian Energy Market Operator (AEMO).

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


| City      | Households | # SQL Insert Lines  |
|-----------|------------|---------------------|
| Singapore | 1,425,100  |     42,750,00       |
| Sydney    | 2,076,284  |     62,288,520      |
| Tokyo     | 6,946,000  |     208,380,000     |


## References

* https://www.energyaustralia.com.au/resources/PDFs/User%20Guide_v3.pdf
* https://devblogs.microsoft.com/cosmosdb/scaling-iot-time-series-metering-workloads-with-azure-cosmos-db-for-postgresql/
* https://www.yurika.com.au/wp-content/uploads/2022/01/NEM12-Fact-Sheet.pdf

```bash
find . -type f | time xargs -n1 -P32 sh -c “psql -U citus -c \”\\copy demo.meter_data from ‘\$0’ with csv\””;
```