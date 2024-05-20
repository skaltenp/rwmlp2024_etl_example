# Building metadata
Bronze external table: ``bronze_building_metadata``
URI pattern: ``dagashrae/dso_data/building*.csv``

## DSO 1
### Meter 0
Bronze external table: ``bronze_dso1_meter0``
URI pattern: ``dagashrae/dso_data/*/0.csv``

Silver view:
```
CREATE VIEW `dagrwmlp-420511.greenspark.silver_dso1_meter0` AS
SELECT timestamp, meter_reading AS meter_0, SPLIT(_FILE_NAME, '/')[OFFSET(4)] AS site_id, SPLIT(_FILE_NAME, '/')[OFFSET(5)] AS building_id
FROM `dagrwmlp-420511.greenspark.bronze_dso1_meter0`
```

### Meter 2
Bronze external table: ``bronze_dso1_meter2``
URI pattern: ``dagashrae/dso_data/*/2.csv``

Silver view:
```
CREATE VIEW `dagrwmlp-420511.greenspark.silver_dso1_meter2` AS
SELECT timestamp, meter_reading AS meter_2, SPLIT(_FILE_NAME, '/')[OFFSET(4)] AS site_id, SPLIT(_FILE_NAME, '/')[OFFSET(5)] AS building_id
FROM `dagrwmlp-420511.greenspark.bronze_dso1_meter2`
```

### All meters
Silver view:
```
CREATE VIEW dagrwmlp-420511.greenspark.silver_dso1_all_meters AS
SELECT t1.site_id, t1.building_id, t1.timestamp, t1.meter_0, t2.meter_2
FROM dagrwmlp-420511.greenspark.silver_dso1_meter0 AS t1
FULL JOIN
dagrwmlp-420511.greenspark.silver_dso1_meter2 AS t2
ON t1.building_id = t2.building_id
AND t1.timestamp = t2.timestamp
```


## DSO 2
### All Meters
Bronze external table: ``bronze_dso2_all_meters``
URI pattern: ``dagashrae/dso_data/*/meter_reading.csv``

Silver view:
```
CREATE VIEW dagrwmlp-420511.greenspark.silver_dso2_all_meters AS
SELECT *, SPLIT(_FILE_NAME, '/')[OFFSET(4)] AS site_id, SPLIT(_FILE_NAME, '/')[OFFSET(5)] AS building_id,
FROM `dagrwmlp-420511.greenspark.bronze_dso2_all_meters`
```

## BOTH DSOs
```
CREATE VIEW dagrwmlp-420511.greenspark.silver_all_meters AS
SELECT site_id, building_id, timestamp, meter_0, meter_2 FROM dagrwmlp-420511.greenspark.silver_dso1_all_meters
UNION ALL
SELECT site_id, building_id, timestamp, meter_0, meter_2 FROM dagrwmlp-420511.greenspark.silver_dso2_all_meters
```