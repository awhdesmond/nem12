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

