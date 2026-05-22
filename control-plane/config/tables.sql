CREATE TABLE auction (
        id  BIGINT,
    itemName  VARCHAR,
    description  VARCHAR,
    initialBid  BIGINT,
    reserve  BIGINT,
    expires  TIMESTAMP(3),
    seller  BIGINT,
    category  BIGINT,
    dateeTime  TIMESTAMP(3),
    extra  VARCHAR,
    WATERMARK FOR dateeTime AS dateeTime - INTERVAL '4' SECOND)
WITH (
    'connector' = 'kafka',
    'topic' = 'auction',
    'properties.bootstrap.servers' = 'kafka-edge1:9092',
    'properties.group.id' = 'nexmark',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner' = 'round-robin',
    'properties.metadata.max.age.ms' = '1800000',
    'format' = 'json'
);
CREATE TABLE person (
    id  BIGINT,
    name  VARCHAR,
    emailAddress  VARCHAR,
    creditCard  VARCHAR,
    city  VARCHAR,
    state  VARCHAR,
    dateeTime  TIMESTAMP(3),
    extra  VARCHAR,
    WATERMARK FOR dateeTime AS dateeTime - INTERVAL '4' SECOND)
WITH (
    'connector' = 'kafka',
    'topic' = 'person',
    'properties.bootstrap.servers' = 'kafka-edge1:9092',
    'properties.group.id' = 'nexmark',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner' = 'round-robin',
    'properties.metadata.max.age.ms' = '1800000',
    'format' = 'json'
);
CREATE TABLE bid (
    auction  BIGINT,
    bidder  BIGINT,
    price  BIGINT,
    dateeTime  TIMESTAMP(3),
    extra  VARCHAR,
    WATERMARK FOR dateeTime AS dateeTime - INTERVAL '4' SECOND)
WITH (
    'connector' = 'kafka',
    'topic' = 'bid',
    'properties.bootstrap.servers' = 'kafka-edge1:9092',
    'properties.group.id' = 'nexmark',
    'scan.startup.mode' = 'earliest-offset',
    'sink.partitioner' = 'round-robin',
    'properties.metadata.max.age.ms' = '1800000',
    'format' = 'json'
);


CREATE TABLE nexmark_q1 (
  auction  BIGINT,
  bidder  BIGINT,
  price  DECIMAL(23, 3),
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR
) WITH (
        'connector' = 'kafka',
        'topic' = 'q1',
        'properties.bootstrap.servers' = 'kafka-edge1:9092',
        'properties.group.id' = 'nexmark',
        'value.format' = 'json'      
    );

CREATE TABLE nexmark_q2 (
  auction  BIGINT,
  price  BIGINT
) WITH (
        'connector' = 'kafka',
        'topic' = 'q2',
        'properties.bootstrap.servers' = 'kafka-edge1:9092',
        'properties.group.id' = 'nexmark',
        'value.format' = 'json'      
    );

CREATE TABLE nexmark_q3 (
  name  VARCHAR,
  city  VARCHAR,
  state  VARCHAR,
  id  BIGINT
) WITH (
        'connector' = 'kafka',
        'topic' = 'q3',
        'properties.bootstrap.servers' = 'kafka-edge1:9092',
        'properties.group.id' = 'nexmark',
        'value.format' = 'json'      
    );

CREATE TABLE nexmark_q5 (
  auction  BIGINT,
  num  BIGINT
) WITH (
        'connector' = 'kafka',
        'topic' = 'q5',
        'properties.bootstrap.servers' = 'kafka-edge1:9092',
        'properties.group.id' = 'nexmark',
        'value.format' = 'json'      
    );

CREATE TABLE nexmark_q8 (
  id  BIGINT,
  name  VARCHAR,
  stime  TIMESTAMP(3)
) WITH (
        'connector' = 'kafka',
        'topic' = 'q8',
        'properties.bootstrap.servers' = 'kafka-edge1:9092',
        'properties.group.id' = 'nexmark',
        'value.format' = 'json'      
    );

CREATE TABLE nexmark_q11 (
    extra VARCHAR,
    auction  BIGINT,
    bidder BIGINT,
    bid_count BIGINT,
    starttime TIMESTAMP(3),
    endtime TIMESTAMP(3)
) WITH (
        'connector' = 'kafka',
        'topic' = 'q11',
        'properties.bootstrap.servers' = 'kafka-edge1:9092',
        'properties.group.id' = 'nexmark',
        'value.format' = 'json'      
    );