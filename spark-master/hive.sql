CREATE EXTERNAL TABLE logs(
bid_id STRING,
timestamp_date STRING,
ipinyou_id STRING,
user_agent STRING,
ip STRING,
region INT,
city INT,
ad_exchange INT,
domain STRING,
url STRING,
anonymous_url_id STRING,
ad_slot_id STRING,
ad_slot_width INT,
ad_slot_height INT,
ad_slot_visibility INT,
ad_slot_format INT,
paying_price INT,
creative_id STRING,
bidding_price INT,
advertiser_id INT,
user_tags BIGINT,
stream_id INT
)
COMMENT 'user logs table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/tmp/alex_dev/dataset/';

CREATE TABLE logs_orc
STORED AS ORC
TBLPROPERTIES("orc.compress"="SNAPPY")
AS SELECT * FROM logs;
