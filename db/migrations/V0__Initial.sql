CREATE TABLE IF NOT EXISTS meter_readings(
    id UUID DEFAULT gen_random_uuid() NOT NULL,
    nmi VARCHAR(10) NOT NULL,
    timestamp timestamp NOT NULL,
    consumption numeric NOT NULL,
    CONSTRAINT meter_readings_pk primary key (id),
    CONSTRAINT meter_readings_unique_consumption unique ("nmi", "timestamp")
);
