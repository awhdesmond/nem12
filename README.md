# NEM12 Data Loader

* Please ensure the implementation is prepared to handle files of very large sizes.
* Success would mean as close to production grade implementation as possible, which means
    * idiomatic code structuring,
    * readability,
    * performance optimization,
    * basic testing,
    * and anything else that you deem feasible.

Possible functional requirements for energy metering systems includes:

* Scalable ingestion
* Low latency ad-hoc queries for individual meters
* Analytics and aggregations


## CLI Options

```bash
Usage: main.py [OPTIONS]

Options:
  -f, --file TEXT                path to NEM12 file.
  --log-level TEXT               log level
  --num-executors INTEGER        number of executors, by default is number of CPU cores
  -o, --output-format [csv|sql]  either output to csv or sql with `INSERT`
                                 statements
  --help                         Show this message and exit.
```

## Setting up PostgreSQL

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



```bash
mkdir output

docker run -d --name postgres \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -p 5432:5432 \
    -v `pwd`/output:/output \
    postgres:16.2

docker run -d --name pgadmin \
    -e PGADMIN_DEFAULT_EMAIL=postgres@email.com \
    -e PGADMIN_DEFAULT_PASSWORD=postgres \
    -p 5051:80 \
    dpage/pgadmin4:8

brew install flyway
make db
```

## Example usage
```bash
python3 main.py --file <path-to-nem12> --num-executors=4

pushd output
# Import SQL statements
find . -type f | xargs -n1 -P32 sh -c "psql -U postgres -f \$0"

# OR Use postgres COPY command to bulk load data
find . -type f | xargs -n1 -P32 sh -c "psql -U postgres -c \"\\copy meter_readings (nmi, timestamp, consumption) from '\$0' DELIMITERS ',' CSV\""
popd
```

## Testing

```bash
make test
```


## Household Statistics

The table below lists the number of households in various cities and the estimated sizes of their NEM12 files for a period of 30 days.


| City      | Households | # SQL Insert Lines  |
|-----------|------------|---------------------|
| Singapore | 1,425,100  |     42,750,00       |
| Sydney    | 2,076,284  |     62,288,520      |
| Tokyo     | 6,946,000  |     208,380,000     |

To generate simulated data: run `make data`.

> `gen_data.py` generates 90 million records (1,500,000 * 30 * 2) by default and it takes around ~15-20 mins to process it on 4 cores.

## References

* https://www.energyaustralia.com.au/resources/PDFs/User%20Guide_v3.pdf
* https://devblogs.microsoft.com/cosmosdb/scaling-iot-time-series-metering-workloads-with-azure-cosmos-db-for-postgresql/
* https://www.yurika.com.au/wp-content/uploads/2022/01/NEM12-Fact-Sheet.pdf

