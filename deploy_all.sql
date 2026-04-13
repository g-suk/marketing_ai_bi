/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  deploy_all.sql
  
  One-click deployment script. Run as ACCOUNTADMIN.
  
  Execution order:
    1. RBAC + Database setup (ACCOUNTADMIN)
    2. Synthetic data (MARKETING_LAB_ROLE)
    3. Dynamic tables
    4. ML models (FORECAST + ANOMALY_DETECTION)
    5. AI functions
    6. Semantic view + Cortex Agent
    7. Stage backup Streamlit app
  
  NOTE: Marketplace listings must be installed manually before running
  02_marketplace_enrichment.sql. See lab guide for instructions.
=============================================================================*/

----------------------------------------------------------------------
-- STEP 1: RBAC + Database + Schema + Stages
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE MARKETING_AI_BI;
CREATE SCHEMA IF NOT EXISTS MARKETING_AI_BI.DEMO_DATA;

CREATE ROLE IF NOT EXISTS MARKETING_LAB_ROLE;
GRANT ROLE MARKETING_LAB_ROLE TO ROLE SYSADMIN;

GRANT OWNERSHIP ON DATABASE MARKETING_AI_BI TO ROLE MARKETING_LAB_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA MARKETING_AI_BI.DEMO_DATA TO ROLE MARKETING_LAB_ROLE COPY CURRENT GRANTS;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MARKETING_LAB_ROLE;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE MARKETING_LAB_ROLE;
GRANT ROLE MARKETING_LAB_ROLE TO USER GSUK;

-- Marketplace grants -- uncomment after installing each listing:
-- GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_PUBLIC_DATA_PAID TO ROLE MARKETING_LAB_ROLE;
-- GRANT IMPORTED PRIVILEGES ON DATABASE CUSTOMERCONNECT360__SAMPLE TO ROLE MARKETING_LAB_ROLE;
-- GRANT IMPORTED PRIVILEGES ON DATABASE GWI_OPEN_DATA              TO ROLE MARKETING_LAB_ROLE;
-- GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE    TO ROLE MARKETING_LAB_ROLE;

USE ROLE MARKETING_LAB_ROLE;
USE DATABASE MARKETING_AI_BI;
USE SCHEMA DEMO_DATA;
USE WAREHOUSE COMPUTE_WH;

CREATE STAGE IF NOT EXISTS STREAMLIT_STAGE DIRECTORY = (ENABLE = TRUE);
CREATE STAGE IF NOT EXISTS DATA_STAGE DIRECTORY = (ENABLE = TRUE);

----------------------------------------------------------------------
-- STEP 2: Create tables + Insert synthetic data
----------------------------------------------------------------------

-- === WHOLESALE_PARTNERS ===
CREATE OR REPLACE TABLE WHOLESALE_PARTNERS (
    partner_id INT, partner_name VARCHAR(100), region VARCHAR(20),
    tier VARCHAR(10), avg_sell_through_rate FLOAT, annual_volume INT
);

INSERT INTO WHOLESALE_PARTNERS VALUES
(1,'Summit Sports Outfitters','northeast','gold',0.82,1200000),
(2,'Rocky Mountain Gear Co','west','gold',0.79,1100000),
(3,'Alpine Peak Retailers','west','gold',0.85,1350000),
(4,'Cascade Outdoor Supply','pacific','gold',0.81,1050000),
(5,'Trailhead Trading Post','southeast','gold',0.77,980000),
(6,'Northwind Outfitters','northeast','silver',0.72,750000),
(7,'Blue Ridge Provisions','southeast','silver',0.68,680000),
(8,'Great Lakes Adventure','midwest','silver',0.71,720000),
(9,'Pacific Crest Supplies','pacific','silver',0.74,790000),
(10,'Timberline Trading Co','west','silver',0.70,700000),
(11,'Basecamp Wholesale','northeast','silver',0.69,660000),
(12,'Canyon Creek Distributors','west','silver',0.73,740000),
(13,'Evergreen Outdoor Depot','pacific','silver',0.67,630000),
(14,'Frontier Expedition Supply','midwest','silver',0.66,620000),
(15,'Ridgeline Retailers','southeast','silver',0.70,710000),
(16,'Prairie Wind Outfitters','midwest','bronze',0.58,420000),
(17,'Coastal Trail Supply','pacific','bronze',0.61,450000),
(18,'Heartland Adventure Gear','midwest','bronze',0.55,380000),
(19,'Sagebrush Outdoor Co','west','bronze',0.59,410000),
(20,'Lakeshore Sports Hub','midwest','bronze',0.57,390000),
(21,'Pinewood Provisions','southeast','bronze',0.60,440000),
(22,'Desert Sun Outfitters','west','bronze',0.54,360000),
(23,'Harbor Point Gear','northeast','bronze',0.62,460000),
(24,'Tundra Trail Supply','northeast','bronze',0.56,400000),
(25,'Redwood Outdoor Exchange','pacific','bronze',0.63,480000),
(26,'Appalachian Gear House','southeast','bronze',0.53,350000),
(27,'Bayou Adventure Supply','southeast','bronze',0.52,330000),
(28,'Glacier View Trading','west','bronze',0.58,430000),
(29,'Lone Star Outfitters','southeast','bronze',0.55,370000),
(30,'Northern Lights Outdoor','northeast','bronze',0.60,440000);

-- === CAMPAIGNS ===
CREATE OR REPLACE TABLE CAMPAIGNS (
    campaign_id INT, campaign_name VARCHAR(200), channel VARCHAR(20),
    sub_channel VARCHAR(30), start_date DATE, end_date DATE,
    budget FLOAT, target_conversions INT
);

INSERT INTO CAMPAIGNS VALUES
(1,'Summer Kickoff Email Blast','DTC','email','2024-07-01','2024-07-31',8000,400),
(2,'Back to Adventure Email','DTC','email','2024-08-15','2024-09-15',7500,350),
(3,'Fall Gear Preview Email','DTC','email','2024-09-20','2024-10-20',9000,450),
(4,'Black Friday Early Access Email','DTC','email','2024-11-01','2024-11-30',15000,800),
(5,'Holiday Gift Guide Email','DTC','email','2024-12-01','2024-12-25',12000,650),
(6,'New Year Resolution Email','DTC','email','2025-01-05','2025-01-31',8500,420),
(7,'Spring Gear Launch Email','DTC','email','2025-03-01','2025-03-31',10000,500),
(8,'Earth Day Sustainability Email','DTC','email','2025-04-15','2025-04-30',6000,300),
(9,'Summer Sale Announcement Email','DTC','email','2025-06-01','2025-06-30',9500,470),
(10,'Fall Preview Loyalty Email','DTC','email','2025-09-01','2025-09-30',8000,400),
(11,'Summer Vibes Social Campaign','DTC','social','2024-07-01','2024-08-31',18000,600),
(12,'Autumn Adventure Social','DTC','social','2024-09-15','2024-10-31',20000,700),
(13,'Holiday Wishlist Social','DTC','social','2024-11-15','2024-12-31',35000,1200),
(14,'Winter Wonderland Social','DTC','social','2025-01-01','2025-02-28',22000,750),
(15,'Spring Into Adventure Social','DTC','social','2025-03-01','2025-04-30',25000,850),
(16,'National Parks Week Social','DTC','social','2025-04-19','2025-04-26',5000,200),
(17,'Summer Solstice Social Push','DTC','social','2025-06-15','2025-07-15',16000,550),
(18,'Back to Trail Social','DTC','social','2025-08-01','2025-08-31',14000,480),
(19,'Labor Day Blowout Social','DTC','social','2025-09-01','2025-09-07',8000,350),
(20,'Holiday Countdown Social','DTC','social','2025-11-01','2025-12-31',30000,1100),
(21,'Camping Gear Search Campaign','DTC','search','2024-07-01','2024-09-30',25000,900),
(22,'Winter Boots Search','DTC','search','2024-10-01','2024-12-31',30000,1100),
(23,'Ski Equipment Search','DTC','search','2024-11-15','2025-02-28',28000,950),
(24,'Hiking Gear Search - Spring','DTC','search','2025-03-01','2025-05-31',22000,780),
(25,'Brand Terms Defense Search','DTC','search','2025-01-01','2025-12-31',12000,500),
(26,'Competitor Conquest Search','DTC','search','2025-01-01','2025-12-31',15000,600),
(27,'Climbing Gear Search','DTC','search','2025-04-01','2025-08-31',18000,650),
(28,'Outerwear Search - Fall','DTC','search','2025-09-01','2025-11-30',20000,750),
(29,'Gift Ideas Search','DTC','search','2025-11-15','2025-12-25',16000,700),
(30,'Clearance Sale Search','DTC','search','2025-07-01','2025-07-31',10000,500),
(31,'Trail Runner Collab - Jake Marsh','DTC','influencer','2024-08-01','2024-08-31',12000,300),
(32,'Mountain Vlogger Partnership','DTC','influencer','2024-10-01','2024-11-30',20000,500),
(33,'Ski Season Influencer Blitz','DTC','influencer','2024-12-01','2025-02-28',35000,800),
(34,'Spring Hike Challenge','DTC','influencer','2025-03-15','2025-05-15',15000,400),
(35,'Summer Camp Creator Series','DTC','influencer','2025-06-01','2025-07-31',18000,450),
(36,'Climbing Wall Challenge','DTC','influencer','2025-04-01','2025-04-30',10000,250),
(37,'Fall Colors Adventure Series','DTC','influencer','2025-09-15','2025-10-31',14000,350),
(38,'Holiday Gift Unboxing','DTC','influencer','2025-11-15','2025-12-25',25000,600),
(39,'Gear Review Partnership - Q3','DTC','influencer','2025-07-01','2025-09-30',16000,400),
(40,'Ultramarathon Sponsorship','DTC','influencer','2025-05-01','2025-05-31',8000,200),
(41,'Q3 Trade Show Promo','wholesale','trade_promo','2024-07-15','2024-09-15',40000,150),
(42,'Holiday Stocking Program','wholesale','trade_promo','2024-10-01','2024-11-30',60000,250),
(43,'Winter Season Buy-In','wholesale','trade_promo','2024-11-01','2025-01-31',55000,220),
(44,'Spring Preview Trade Push','wholesale','trade_promo','2025-02-01','2025-03-31',45000,180),
(45,'Summer Catalog Promo','wholesale','trade_promo','2025-04-01','2025-06-30',50000,200),
(46,'Back-to-School Retail Push','wholesale','trade_promo','2025-08-01','2025-09-15',35000,140),
(47,'Q4 Holiday Trade Incentive','wholesale','trade_promo','2025-10-01','2025-12-15',70000,280),
(48,'REI Co-op Ad Partnership','wholesale','co_op_ad','2024-09-01','2024-12-31',30000,120),
(49,'Dicks Sporting Goods Co-op','wholesale','co_op_ad','2025-01-01','2025-04-30',28000,110),
(50,'Bass Pro Shops Co-op','wholesale','co_op_ad','2025-03-01','2025-06-30',25000,100),
(51,'Academy Sports Co-op Ad','wholesale','co_op_ad','2025-05-01','2025-08-31',22000,90),
(52,'Nordstrom Co-op Holiday','wholesale','co_op_ad','2025-10-01','2025-12-31',32000,130),
(53,'Backcountry.com Co-op','wholesale','co_op_ad','2025-06-01','2025-09-30',20000,80),
(54,'Moosejaw Co-op Winter','wholesale','co_op_ad','2024-11-01','2025-02-28',18000,70),
(55,'Outdoor Retailer Show - Summer','wholesale','distributor_event','2024-08-10','2024-08-15',15000,60),
(56,'Outdoor Retailer Show - Winter','wholesale','distributor_event','2025-01-20','2025-01-25',15000,60),
(57,'Regional Dealer Summit - East','wholesale','distributor_event','2025-03-10','2025-03-12',10000,40),
(58,'Regional Dealer Summit - West','wholesale','distributor_event','2025-04-14','2025-04-16',10000,40),
(59,'Annual Buyer Conference','wholesale','distributor_event','2025-06-15','2025-06-18',20000,80),
(60,'Holiday Preview Showcase','wholesale','distributor_event','2025-09-08','2025-09-10',12000,50);

-- === CUSTOMERS (8000 rows via GENERATOR) ===
CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id INT, first_name VARCHAR(50), last_name VARCHAR(50),
    email VARCHAR(150), state VARCHAR(2), zip_code VARCHAR(5),
    age INT, gender VARCHAR(10), channel_preference VARCHAR(10),
    signup_date DATE, lifetime_value FLOAT
);

INSERT INTO CUSTOMERS
SELECT
    SEQ4()+1 AS customer_id,
    CASE MOD(SEQ4(),40) WHEN 0 THEN 'James' WHEN 1 THEN 'Mary' WHEN 2 THEN 'Robert' WHEN 3 THEN 'Patricia' WHEN 4 THEN 'John' WHEN 5 THEN 'Jennifer' WHEN 6 THEN 'Michael' WHEN 7 THEN 'Linda' WHEN 8 THEN 'David' WHEN 9 THEN 'Elizabeth' WHEN 10 THEN 'William' WHEN 11 THEN 'Barbara' WHEN 12 THEN 'Richard' WHEN 13 THEN 'Susan' WHEN 14 THEN 'Joseph' WHEN 15 THEN 'Jessica' WHEN 16 THEN 'Thomas' WHEN 17 THEN 'Sarah' WHEN 18 THEN 'Chris' WHEN 19 THEN 'Karen' WHEN 20 THEN 'Daniel' WHEN 21 THEN 'Lisa' WHEN 22 THEN 'Matthew' WHEN 23 THEN 'Nancy' WHEN 24 THEN 'Anthony' WHEN 25 THEN 'Betty' WHEN 26 THEN 'Mark' WHEN 27 THEN 'Margaret' WHEN 28 THEN 'Andrew' WHEN 29 THEN 'Sandra' WHEN 30 THEN 'Steven' WHEN 31 THEN 'Ashley' WHEN 32 THEN 'Paul' WHEN 33 THEN 'Emily' WHEN 34 THEN 'Joshua' WHEN 35 THEN 'Donna' WHEN 36 THEN 'Kevin' WHEN 37 THEN 'Michelle' WHEN 38 THEN 'Brian' ELSE 'Amanda' END AS first_name,
    CASE MOD(SEQ4(),30) WHEN 0 THEN 'Smith' WHEN 1 THEN 'Johnson' WHEN 2 THEN 'Williams' WHEN 3 THEN 'Brown' WHEN 4 THEN 'Jones' WHEN 5 THEN 'Garcia' WHEN 6 THEN 'Miller' WHEN 7 THEN 'Davis' WHEN 8 THEN 'Rodriguez' WHEN 9 THEN 'Martinez' WHEN 10 THEN 'Anderson' WHEN 11 THEN 'Taylor' WHEN 12 THEN 'Thomas' WHEN 13 THEN 'Moore' WHEN 14 THEN 'Jackson' WHEN 15 THEN 'Lee' WHEN 16 THEN 'Perez' WHEN 17 THEN 'White' WHEN 18 THEN 'Harris' WHEN 19 THEN 'Clark' WHEN 20 THEN 'Lewis' WHEN 21 THEN 'Young' WHEN 22 THEN 'Walker' WHEN 23 THEN 'Hall' WHEN 24 THEN 'Allen' WHEN 25 THEN 'King' WHEN 26 THEN 'Wright' WHEN 27 THEN 'Scott' WHEN 28 THEN 'Green' ELSE 'Baker' END AS last_name,
    LOWER(first_name)||'.'||LOWER(last_name)||(SEQ4()+1)::VARCHAR||'@summitgearfans.com' AS email,
    CASE MOD(ABS(HASH(SEQ4())),50) WHEN 0 THEN 'CA' WHEN 1 THEN 'CA' WHEN 2 THEN 'CA' WHEN 3 THEN 'CA' WHEN 4 THEN 'CA' WHEN 5 THEN 'CA' WHEN 6 THEN 'CA' WHEN 7 THEN 'TX' WHEN 8 THEN 'TX' WHEN 9 THEN 'TX' WHEN 10 THEN 'TX' WHEN 11 THEN 'TX' WHEN 12 THEN 'NY' WHEN 13 THEN 'NY' WHEN 14 THEN 'NY' WHEN 15 THEN 'NY' WHEN 16 THEN 'FL' WHEN 17 THEN 'FL' WHEN 18 THEN 'FL' WHEN 19 THEN 'CO' WHEN 20 THEN 'CO' WHEN 21 THEN 'CO' WHEN 22 THEN 'WA' WHEN 23 THEN 'WA' WHEN 24 THEN 'OR' WHEN 25 THEN 'OR' WHEN 26 THEN 'UT' WHEN 27 THEN 'UT' WHEN 28 THEN 'MT' WHEN 29 THEN 'ID' WHEN 30 THEN 'AZ' WHEN 31 THEN 'NC' WHEN 32 THEN 'VA' WHEN 33 THEN 'GA' WHEN 34 THEN 'PA' WHEN 35 THEN 'IL' WHEN 36 THEN 'OH' WHEN 37 THEN 'MI' WHEN 38 THEN 'MN' WHEN 39 THEN 'WI' WHEN 40 THEN 'MA' WHEN 41 THEN 'NJ' WHEN 42 THEN 'CT' WHEN 43 THEN 'NH' WHEN 44 THEN 'VT' WHEN 45 THEN 'ME' WHEN 46 THEN 'NM' WHEN 47 THEN 'NV' WHEN 48 THEN 'TN' ELSE 'SC' END AS state,
    LPAD((10000+MOD(ABS(HASH(SEQ4()*7+13)),90000))::VARCHAR,5,'0') AS zip_code,
    18+MOD(ABS(HASH(SEQ4()*3)),52) AS age,
    CASE WHEN MOD(ABS(HASH(SEQ4()*11)),3)=0 THEN 'male' WHEN MOD(ABS(HASH(SEQ4()*11)),3)=1 THEN 'female' ELSE 'non-binary' END AS gender,
    CASE WHEN MOD(ABS(HASH(SEQ4()*17)),10)<5 THEN 'DTC' WHEN MOD(ABS(HASH(SEQ4()*17)),10)<8 THEN 'wholesale' ELSE 'both' END AS channel_preference,
    DATEADD(DAY,-MOD(ABS(HASH(SEQ4()*23)),1095),'2025-12-31')::DATE AS signup_date,
    ROUND(50+UNIFORM(0::FLOAT,2950::FLOAT,RANDOM(42)),2) AS lifetime_value
FROM TABLE(GENERATOR(ROWCOUNT=>8000));

-- === ORDERS (25000 rows via GENERATOR with seasonal patterns + anomalies) ===
CREATE OR REPLACE TABLE ORDERS (
    order_id INT, customer_id INT, campaign_id INT, order_date DATE,
    channel VARCHAR(20), product_category VARCHAR(20), product_name VARCHAR(150),
    quantity INT, revenue FLOAT, wholesale_partner_id INT
);

INSERT INTO ORDERS
WITH date_spine AS (
    SELECT DATEADD(DAY,SEQ4(),'2024-07-01')::DATE AS d FROM TABLE(GENERATOR(ROWCOUNT=>549)) WHERE DATEADD(DAY,SEQ4(),'2024-07-01')<='2025-12-31'
),
base_orders AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY RANDOM(100)) AS order_id,
        1+MOD(ABS(HASH(RANDOM(101))),8000) AS customer_id,
        ds.d AS order_date,
        CASE WHEN UNIFORM(0::FLOAT,1::FLOAT,RANDOM(102))<0.65 THEN 'DTC' ELSE 'wholesale' END AS channel,
        CASE MOD(ABS(HASH(RANDOM(103))),100) WHEN 0 THEN 'outerwear' WHEN 1 THEN 'outerwear' WHEN 2 THEN 'outerwear' WHEN 3 THEN 'outerwear' WHEN 4 THEN 'outerwear' WHEN 5 THEN 'outerwear' WHEN 6 THEN 'outerwear' WHEN 7 THEN 'outerwear' WHEN 8 THEN 'outerwear' WHEN 9 THEN 'outerwear' WHEN 10 THEN 'outerwear' WHEN 11 THEN 'outerwear' WHEN 12 THEN 'outerwear' WHEN 13 THEN 'outerwear' WHEN 14 THEN 'outerwear' WHEN 15 THEN 'outerwear' WHEN 16 THEN 'outerwear' WHEN 17 THEN 'outerwear' WHEN 18 THEN 'outerwear' WHEN 19 THEN 'outerwear' WHEN 20 THEN 'footwear' WHEN 21 THEN 'footwear' WHEN 22 THEN 'footwear' WHEN 23 THEN 'footwear' WHEN 24 THEN 'footwear' WHEN 25 THEN 'footwear' WHEN 26 THEN 'footwear' WHEN 27 THEN 'footwear' WHEN 28 THEN 'footwear' WHEN 29 THEN 'footwear' WHEN 30 THEN 'footwear' WHEN 31 THEN 'footwear' WHEN 32 THEN 'footwear' WHEN 33 THEN 'footwear' WHEN 34 THEN 'footwear' WHEN 35 THEN 'footwear' WHEN 36 THEN 'footwear' WHEN 37 THEN 'footwear' WHEN 38 THEN 'camping' WHEN 39 THEN 'camping' WHEN 40 THEN 'camping' WHEN 41 THEN 'camping' WHEN 42 THEN 'camping' WHEN 43 THEN 'camping' WHEN 44 THEN 'camping' WHEN 45 THEN 'camping' WHEN 46 THEN 'camping' WHEN 47 THEN 'camping' WHEN 48 THEN 'camping' WHEN 49 THEN 'camping' WHEN 50 THEN 'camping' WHEN 51 THEN 'camping' WHEN 52 THEN 'camping' WHEN 53 THEN 'climbing' WHEN 54 THEN 'climbing' WHEN 55 THEN 'climbing' WHEN 56 THEN 'climbing' WHEN 57 THEN 'climbing' WHEN 58 THEN 'climbing' WHEN 59 THEN 'climbing' WHEN 60 THEN 'climbing' WHEN 61 THEN 'climbing' WHEN 62 THEN 'climbing' WHEN 63 THEN 'climbing' WHEN 64 THEN 'climbing' WHEN 65 THEN 'winter_sports' WHEN 66 THEN 'winter_sports' WHEN 67 THEN 'winter_sports' WHEN 68 THEN 'winter_sports' WHEN 69 THEN 'winter_sports' WHEN 70 THEN 'winter_sports' WHEN 71 THEN 'winter_sports' WHEN 72 THEN 'winter_sports' WHEN 73 THEN 'winter_sports' WHEN 74 THEN 'winter_sports' WHEN 75 THEN 'winter_sports' WHEN 76 THEN 'winter_sports' WHEN 77 THEN 'winter_sports' WHEN 78 THEN 'winter_sports' ELSE 'accessories' END AS product_category
    FROM date_spine ds, TABLE(GENERATOR(ROWCOUNT=>50)) g
)
SELECT
    order_id, customer_id,
    CASE WHEN channel='DTC' THEN CASE MOD(ABS(HASH(order_id)),4) WHEN 0 THEN 1 WHEN 1 THEN 11 WHEN 2 THEN 21 ELSE 31 END+MOD(ABS(HASH(order_id*7)),10) ELSE 41+MOD(ABS(HASH(order_id*13)),20) END AS campaign_id,
    order_date, channel, product_category,
    CASE product_category WHEN 'outerwear' THEN CASE MOD(ABS(HASH(order_id*2)),5) WHEN 0 THEN 'Alpine Pro Shell Jacket' WHEN 1 THEN 'Summit Puffer Vest' WHEN 2 THEN 'Trailbreaker Rain Coat' WHEN 3 THEN 'Peak Insulated Parka' ELSE 'Ridge Softshell Hoodie' END WHEN 'footwear' THEN CASE MOD(ABS(HASH(order_id*3)),5) WHEN 0 THEN 'Trailmaster Hiking Boots' WHEN 1 THEN 'Summit Approach Shoes' WHEN 2 THEN 'Glacier Insulated Boots' WHEN 3 THEN 'Canyon Trail Runners' ELSE 'Alpine Crampon Boots' END WHEN 'camping' THEN CASE MOD(ABS(HASH(order_id*5)),5) WHEN 0 THEN 'Basecamp 3-Person Tent' WHEN 1 THEN 'Summit Zero Sleeping Bag' WHEN 2 THEN 'Trailhead Camp Stove' WHEN 3 THEN 'Ridge Ultralight Hammock' ELSE 'Peak Water Filter System' END WHEN 'climbing' THEN CASE MOD(ABS(HASH(order_id*7)),5) WHEN 0 THEN 'Vertical Pro Harness' WHEN 1 THEN 'Summit Chalk Bag Set' WHEN 2 THEN 'Alpine Dynamic Rope 60m' WHEN 3 THEN 'Crag Quickdraw Set' ELSE 'Peak Belay Device' END WHEN 'winter_sports' THEN CASE MOD(ABS(HASH(order_id*11)),5) WHEN 0 THEN 'Powder Pro Ski Package' WHEN 1 THEN 'Summit Snowboard Combo' WHEN 2 THEN 'Backcountry Touring Skis' WHEN 3 THEN 'Alpine Goggles Pro' ELSE 'Glacier Avalanche Kit' END ELSE CASE MOD(ABS(HASH(order_id*13)),5) WHEN 0 THEN 'Trail Navigation Watch' WHEN 1 THEN 'Summit Hydration Pack' WHEN 2 THEN 'Peak Trekking Poles' WHEN 3 THEN 'Alpine Headlamp Pro' ELSE 'Ridge Dry Bag Set' END END AS product_name,
    CASE WHEN channel='wholesale' THEN 5+MOD(ABS(HASH(order_id*19)),46) ELSE 1+MOD(ABS(HASH(order_id*19)),3) END AS quantity,
    ROUND((CASE product_category WHEN 'outerwear' THEN 120+UNIFORM(0::FLOAT,180::FLOAT,RANDOM(200)) WHEN 'footwear' THEN 90+UNIFORM(0::FLOAT,160::FLOAT,RANDOM(201)) WHEN 'camping' THEN 60+UNIFORM(0::FLOAT,240::FLOAT,RANDOM(202)) WHEN 'climbing' THEN 40+UNIFORM(0::FLOAT,160::FLOAT,RANDOM(203)) WHEN 'winter_sports' THEN 80+UNIFORM(0::FLOAT,420::FLOAT,RANDOM(204)) ELSE 20+UNIFORM(0::FLOAT,80::FLOAT,RANDOM(205)) END)*quantity*CASE WHEN MONTH(order_date) IN (11,12) THEN 1.0+0.40*(CASE WHEN channel='DTC' THEN 1.0 ELSE 0.625 END) WHEN MONTH(order_date) IN (3,4) THEN 1.20 WHEN MONTH(order_date) IN (1,2) AND product_category='winter_sports' THEN 1.30 WHEN MONTH(order_date) IN (6,7,8) THEN 0.85 ELSE 1.0 END*CASE WHEN channel='wholesale' AND order_date BETWEEN '2025-09-15' AND '2025-09-26' THEN 5.0 ELSE 1.0 END,2) AS revenue,
    CASE WHEN channel='wholesale' THEN 1+MOD(ABS(HASH(order_id*31)),30) ELSE NULL END AS wholesale_partner_id
FROM base_orders ORDER BY RANDOM(999) LIMIT 25000;

-- === MARKETING_SPEND ===
CREATE OR REPLACE TABLE MARKETING_SPEND (
    spend_id INT, campaign_id INT, spend_date DATE, channel VARCHAR(20),
    sub_channel VARCHAR(30), amount FLOAT, impressions INT, clicks INT, conversions INT
);

INSERT INTO MARKETING_SPEND
WITH date_spine AS (
    SELECT DATEADD(DAY,SEQ4(),'2024-07-01')::DATE AS d FROM TABLE(GENERATOR(ROWCOUNT=>549)) WHERE DATEADD(DAY,SEQ4(),'2024-07-01')<='2025-12-31'
),
campaign_dates AS (
    SELECT c.campaign_id,c.campaign_name,c.channel,c.sub_channel,c.budget,c.target_conversions,ds.d AS spend_date,DATEDIFF(DAY,c.start_date,c.end_date)+1 AS campaign_days
    FROM CAMPAIGNS c JOIN date_spine ds ON ds.d BETWEEN c.start_date AND c.end_date
)
SELECT
    ROW_NUMBER() OVER (ORDER BY spend_date,campaign_id) AS spend_id, campaign_id, spend_date, channel, sub_channel,
    ROUND((budget/campaign_days)*(0.7+UNIFORM(0::FLOAT,0.6::FLOAT,RANDOM(300)))*CASE WHEN sub_channel='influencer' AND spend_date BETWEEN '2024-11-11' AND '2024-11-22' THEN 3.0 ELSE 1.0 END,2) AS amount,
    GREATEST(100,ROUND((CASE sub_channel WHEN 'email' THEN 15000 WHEN 'social' THEN 25000 WHEN 'search' THEN 8000 WHEN 'influencer' THEN 50000 WHEN 'trade_promo' THEN 5000 WHEN 'co_op_ad' THEN 12000 ELSE 3000 END)*(0.6+UNIFORM(0::FLOAT,0.8::FLOAT,RANDOM(400)))/campaign_days)) AS impressions,
    GREATEST(5,ROUND(impressions*CASE sub_channel WHEN 'email' THEN 0.035 WHEN 'social' THEN 0.018 WHEN 'search' THEN 0.045 WHEN 'influencer' THEN 0.008 WHEN 'trade_promo' THEN 0.025 WHEN 'co_op_ad' THEN 0.020 ELSE 0.015 END*(0.7+UNIFORM(0::FLOAT,0.6::FLOAT,RANDOM(500))))) AS clicks,
    GREATEST(0,ROUND(clicks*CASE sub_channel WHEN 'email' THEN 0.12 WHEN 'social' THEN 0.06 WHEN 'search' THEN 0.10 WHEN 'influencer' THEN 0.04 WHEN 'trade_promo' THEN 0.08 WHEN 'co_op_ad' THEN 0.07 ELSE 0.05 END*(0.5+UNIFORM(0::FLOAT,1.0::FLOAT,RANDOM(600)))*CASE WHEN channel='DTC' AND spend_date BETWEEN '2025-02-08' AND '2025-02-16' THEN 0.20 ELSE 1.0 END)) AS conversions
FROM campaign_dates;

-- === PRODUCT_REVIEWS (1500 rows) ===
CREATE OR REPLACE TABLE PRODUCT_REVIEWS (
    review_id INT, customer_id INT, product_category VARCHAR(20),
    product_name VARCHAR(150), channel VARCHAR(20), review_date DATE,
    rating INT, review_text VARCHAR(2000)
);

INSERT INTO PRODUCT_REVIEWS
WITH review_base AS (
    SELECT SEQ4()+1 AS review_id, 1+MOD(ABS(HASH(SEQ4()*41)),8000) AS customer_id,
        CASE MOD(ABS(HASH(SEQ4()*43)),6) WHEN 0 THEN 'outerwear' WHEN 1 THEN 'footwear' WHEN 2 THEN 'camping' WHEN 3 THEN 'climbing' WHEN 4 THEN 'winter_sports' ELSE 'accessories' END AS product_category,
        CASE WHEN UNIFORM(0::FLOAT,1::FLOAT,RANDOM(700))<0.65 THEN 'dtc_website' ELSE 'retail_partner' END AS channel,
        DATEADD(DAY,MOD(ABS(HASH(SEQ4()*47)),549),'2024-07-01')::DATE AS review_date,
        CASE WHEN UNIFORM(0::FLOAT,1::FLOAT,RANDOM(800))<0.10 THEN 1 WHEN UNIFORM(0::FLOAT,1::FLOAT,RANDOM(801))<0.15 THEN 2 WHEN UNIFORM(0::FLOAT,1::FLOAT,RANDOM(802))<0.30 THEN 3 WHEN UNIFORM(0::FLOAT,1::FLOAT,RANDOM(803))<0.55 THEN 4 ELSE 5 END AS rating,
        MOD(ABS(HASH(SEQ4()*51)),50) AS review_variant
    FROM TABLE(GENERATOR(ROWCOUNT=>1500))
)
SELECT review_id, customer_id, product_category,
    CASE product_category WHEN 'outerwear' THEN CASE MOD(review_variant,5) WHEN 0 THEN 'Alpine Pro Shell Jacket' WHEN 1 THEN 'Summit Puffer Vest' WHEN 2 THEN 'Trailbreaker Rain Coat' WHEN 3 THEN 'Peak Insulated Parka' ELSE 'Ridge Softshell Hoodie' END WHEN 'footwear' THEN CASE MOD(review_variant,5) WHEN 0 THEN 'Trailmaster Hiking Boots' WHEN 1 THEN 'Summit Approach Shoes' WHEN 2 THEN 'Glacier Insulated Boots' WHEN 3 THEN 'Canyon Trail Runners' ELSE 'Alpine Crampon Boots' END WHEN 'camping' THEN CASE MOD(review_variant,5) WHEN 0 THEN 'Basecamp 3-Person Tent' WHEN 1 THEN 'Summit Zero Sleeping Bag' WHEN 2 THEN 'Trailhead Camp Stove' WHEN 3 THEN 'Ridge Ultralight Hammock' ELSE 'Peak Water Filter System' END WHEN 'climbing' THEN CASE MOD(review_variant,5) WHEN 0 THEN 'Vertical Pro Harness' WHEN 1 THEN 'Summit Chalk Bag Set' WHEN 2 THEN 'Alpine Dynamic Rope 60m' WHEN 3 THEN 'Crag Quickdraw Set' ELSE 'Peak Belay Device' END WHEN 'winter_sports' THEN CASE MOD(review_variant,5) WHEN 0 THEN 'Powder Pro Ski Package' WHEN 1 THEN 'Summit Snowboard Combo' WHEN 2 THEN 'Backcountry Touring Skis' WHEN 3 THEN 'Alpine Goggles Pro' ELSE 'Glacier Avalanche Kit' END ELSE CASE MOD(review_variant,5) WHEN 0 THEN 'Trail Navigation Watch' WHEN 1 THEN 'Summit Hydration Pack' WHEN 2 THEN 'Peak Trekking Poles' WHEN 3 THEN 'Alpine Headlamp Pro' ELSE 'Ridge Dry Bag Set' END END AS product_name,
    channel, review_date, rating,
    CASE
        WHEN rating=5 AND product_category='outerwear' AND review_variant<10 THEN 'Absolutely love this jacket! Took it on a week-long backpacking trip in the Cascades and it kept me bone dry through three days of rain. The ventilation zips are a game-changer for uphill climbs. Worth every penny.'
        WHEN rating=5 AND product_category='outerwear' AND review_variant<20 THEN 'Best shell jacket I have ever owned. I was skeptical about the price but after surviving a freak snowstorm in April wearing nothing but a base layer underneath, I am a believer. Highly recommend Summit Gear.'
        WHEN rating=5 AND product_category='footwear' AND review_variant<10 THEN 'These hiking boots are incredible. No break-in period at all, just laced up and hit a 14-mile trail. Ankle support is phenomenal and they still look brand new after 200 miles.'
        WHEN rating=5 AND product_category='footwear' AND review_variant<20 THEN 'Replaced my worn-out Salomons with these and never looked back. The grip on wet rock is outstanding. My feet stayed dry crossing three stream crossings.'
        WHEN rating=5 AND product_category='camping' AND review_variant<10 THEN 'This tent survived 40 mph winds at 11,000 feet. Setup took about 4 minutes solo. The vestibule is large enough for two packs and boots. Incredible value for a 3-season tent.'
        WHEN rating=5 AND product_category='camping' AND review_variant<20 THEN 'The sleeping bag is rated to 15F and I tested it at 18F -- slept like a baby. Packs down incredibly small and the draft collar really works. My go-to for alpine camping.'
        WHEN rating=5 AND product_category='climbing' THEN 'Lightweight, comfortable harness with generous gear loops. Used it for a full season of sport climbing and multi-pitch routes. The adjustable leg loops are clutch for layering in cold weather.'
        WHEN rating=5 AND product_category='winter_sports' AND review_variant<10 THEN 'These skis carve beautifully on groomers but still float in powder. Best all-mountain ski I have tried under $700.'
        WHEN rating=5 AND product_category='winter_sports' THEN 'The goggles have amazing clarity and the magnetic lens swap is so easy even with gloves on. No fogging issues all season.'
        WHEN rating=5 AND product_category='accessories' THEN 'GPS accuracy on this watch is spot-on and the battery lasts 5 days in GPS mode. The altimeter matched my phone within 10 feet. Love the sunrise/sunset alerts.'
        WHEN rating=4 AND product_category='outerwear' AND review_variant<15 THEN 'Really solid jacket overall. The waterproofing held up great, though I wish the hood had a stiffer brim. Fit is true to size and the pockets are well-placed.'
        WHEN rating=4 AND product_category='outerwear' THEN 'Good quality puffer vest. Warm for its weight and packable. Took off one star because the zipper occasionally snags. Would still buy again.'
        WHEN rating=4 AND product_category='footwear' AND review_variant<15 THEN 'Comfortable right out of the box. Great traction on most surfaces. Only complaint is the laces could be better quality -- replaced them with aftermarket ones.'
        WHEN rating=4 AND product_category='footwear' THEN 'Solid trail runners for the price. Cushioning is good for rocky terrain. They run about a half size small so order up.'
        WHEN rating=4 AND product_category='camping' THEN 'Good stove with fast boil times. Wind resistance is decent. The piezo igniter worked reliably for the first 20 uses, then got finicky.'
        WHEN rating=4 AND product_category='climbing' THEN 'Nice rope with good handling. The dry treatment seems effective. Slightly heavier than comparable ropes but the durability trade-off is worth it.'
        WHEN rating=4 AND product_category='winter_sports' THEN 'Snowboard combo is great for intermediate riders. The board is forgiving in choppy conditions. Bindings could use softer padding on the ankle strap.'
        WHEN rating=4 AND product_category='accessories' THEN 'Hydration pack fits well and does not bounce on technical trail. The bite valve is easy to use. Wish it had a dedicated phone pocket.'
        WHEN rating=3 AND product_category='outerwear' AND review_variant<15 THEN 'Decent rain coat for casual use but not for extended backcountry exposure. Seams started showing wear after a few trips. Customer service was helpful.'
        WHEN rating=3 AND product_category='outerwear' THEN 'The parka is warm but incredibly bulky. Hard to move freely with a pack on. Looks great for around-town wear though.'
        WHEN rating=3 AND product_category='footwear' THEN 'Mixed feelings. Comfortable for day hikes but waterproofing failed on a multi-day trip. The tread pattern collects mud like crazy.'
        WHEN rating=3 AND product_category='camping' THEN 'Tent is fine for fair weather but leaked at the seam during heavy rain. Had to seam seal it myself. Not what I expect at this price.'
        WHEN rating=3 AND product_category='climbing' THEN 'Chalk bag is okay, nothing special. The closure does not stay shut and chalk gets everywhere. The fleece lining is nice though.'
        WHEN rating=3 AND product_category='winter_sports' THEN 'Avalanche kit has everything you need but the probe is slow to deploy compared to BCA. The beacon works well and the shovel is sturdy.'
        WHEN rating=3 AND product_category='accessories' THEN 'Trekking poles are average. They adjust smoothly but feel flimsy compared to Black Diamond. The cork grips are comfortable though.'
        WHEN rating=2 AND product_category='outerwear' THEN 'Zipper broke on the second use. The fabric feels thin and cheap. Returning for a refund. Disappointed.'
        WHEN rating=2 AND product_category='footwear' THEN 'Sole separated from the boot after only 50 miles. Should not be marketed as serious hiking boots. Durability is unacceptable.'
        WHEN rating=2 AND product_category='camping' THEN 'Hammock straps are too short for most trees. The fabric is comfortable but the suspension system needs a redesign.'
        WHEN rating=2 AND product_category='climbing' THEN 'Quickdraws feel bulky and the gates stick occasionally. Better options from Petzl or Black Diamond for the price.'
        WHEN rating=2 AND product_category='winter_sports' THEN 'Goggles fog constantly. The anti-fog coating wore off after three ski days. Looking at Smith or Oakley next.'
        WHEN rating=2 AND product_category='accessories' THEN 'Headlamp is dim compared to Petzl Actik. Battery life okay but beam pattern is poor. Feels like a $15 headlamp at $60.'
        WHEN rating=1 AND product_category='outerwear' THEN 'Complete waste of money. Soaked through in light rain within 20 minutes. Bought a Patagonia instead and problem solved.'
        WHEN rating=1 AND product_category='footwear' THEN 'Worst boots I have ever owned. Blisters on day one, sole came apart in a month. Twisted my ankle because of these. Avoid.'
        WHEN rating=1 AND product_category='camping' THEN 'Water filter clogged after 5 liters. Could not backflush in the field. Completely unreliable for backcountry use.'
        WHEN rating=1 AND product_category='climbing' THEN 'Belay device arrived with a rough edge that could damage rope. Quality control issue. Returned immediately.'
        WHEN rating=1 AND product_category='winter_sports' THEN 'Ski bindings did not release during a fall and I tweaked my knee. Terrifying. Switched to Marker bindings.'
        WHEN rating=1 AND product_category='accessories' THEN 'Dry bag leaked on a kayaking trip and my phone got soaked. What is the point of a dry bag that is not waterproof?'
        WHEN rating=5 THEN 'Outstanding product quality. Exceeded my expectations in every way. Summit Gear has earned a loyal customer. Already recommended to three friends.'
        WHEN rating=4 THEN 'Good overall product with minor room for improvement. The materials feel premium and it performs well in the field.'
        WHEN rating=3 THEN 'Average product for the price. Works fine but does not stand out compared to competitors.'
        WHEN rating=2 THEN 'Below expectations. Quality issues showed up quickly and performance was underwhelming.'
        ELSE 'Terrible experience. Product failed immediately and customer service was slow to respond. Would not recommend.'
    END AS review_text
FROM review_base;

----------------------------------------------------------------------
-- STEP 3: Dynamic Tables
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_DAILY_REVENUE TARGET_LAG='1 hour' WAREHOUSE=COMPUTE_WH AS
SELECT order_date, channel, COUNT(*) AS order_count, SUM(quantity) AS total_units, ROUND(SUM(revenue),2) AS total_revenue, ROUND(AVG(revenue),2) AS avg_order_value
FROM ORDERS GROUP BY order_date, channel;

CREATE OR REPLACE DYNAMIC TABLE DT_CAMPAIGN_METRICS TARGET_LAG='1 hour' WAREHOUSE=COMPUTE_WH AS
SELECT c.campaign_id,c.campaign_name,c.channel,c.sub_channel,c.start_date,c.end_date,c.budget,c.target_conversions,
    COALESCE(s.total_spend,0) AS total_spend, COALESCE(s.total_impressions,0) AS total_impressions,
    COALESCE(s.total_clicks,0) AS total_clicks, COALESCE(s.total_conversions,0) AS total_conversions,
    COALESCE(o.campaign_revenue,0) AS campaign_revenue,
    CASE WHEN COALESCE(s.total_conversions,0)>0 THEN ROUND(COALESCE(s.total_spend,0)/s.total_conversions,2) ELSE NULL END AS cpa,
    CASE WHEN COALESCE(s.total_spend,0)>0 THEN ROUND(COALESCE(o.campaign_revenue,0)/s.total_spend,2) ELSE NULL END AS roas,
    CASE WHEN COALESCE(s.total_impressions,0)>0 THEN ROUND(COALESCE(s.total_clicks,0)::FLOAT/s.total_impressions*100,3) ELSE NULL END AS ctr_pct
FROM CAMPAIGNS c
LEFT JOIN (SELECT campaign_id,SUM(amount) AS total_spend,SUM(impressions) AS total_impressions,SUM(clicks) AS total_clicks,SUM(conversions) AS total_conversions FROM MARKETING_SPEND GROUP BY campaign_id) s ON c.campaign_id=s.campaign_id
LEFT JOIN (SELECT campaign_id,SUM(revenue) AS campaign_revenue FROM ORDERS GROUP BY campaign_id) o ON c.campaign_id=o.campaign_id;

CREATE OR REPLACE DYNAMIC TABLE DT_PARTNER_PERFORMANCE TARGET_LAG='1 hour' WAREHOUSE=COMPUTE_WH AS
SELECT wp.partner_id,wp.partner_name,wp.region,wp.tier,wp.avg_sell_through_rate,wp.annual_volume AS target_annual_volume,
    COUNT(o.order_id) AS total_orders,SUM(o.quantity) AS total_units_sold,ROUND(SUM(o.revenue),2) AS total_revenue,ROUND(AVG(o.revenue),2) AS avg_order_value,
    ROUND(SUM(o.revenue)/NULLIF(wp.annual_volume,0)*100,1) AS revenue_attainment_pct
FROM WHOLESALE_PARTNERS wp LEFT JOIN ORDERS o ON wp.partner_id=o.wholesale_partner_id
GROUP BY wp.partner_id,wp.partner_name,wp.region,wp.tier,wp.avg_sell_through_rate,wp.annual_volume;

CREATE OR REPLACE DYNAMIC TABLE DT_CUSTOMER_SEGMENTS TARGET_LAG='DOWNSTREAM' WAREHOUSE=COMPUTE_WH AS
SELECT c.customer_id,c.state,c.zip_code,c.age,c.gender,c.channel_preference,c.lifetime_value,
    CASE WHEN c.age BETWEEN 18 AND 24 THEN '18-24' WHEN c.age BETWEEN 25 AND 34 THEN '25-34' WHEN c.age BETWEEN 35 AND 44 THEN '35-44' WHEN c.age BETWEEN 45 AND 54 THEN '45-54' ELSE '55+' END AS age_group,
    CASE WHEN c.lifetime_value>=2000 THEN 'high_value' WHEN c.lifetime_value>=500 THEN 'mid_value' ELSE 'low_value' END AS value_tier,
    o.total_orders, o.total_revenue
FROM CUSTOMERS c LEFT JOIN (SELECT customer_id,COUNT(*) AS total_orders,SUM(revenue) AS total_revenue FROM ORDERS GROUP BY customer_id) o ON c.customer_id=o.customer_id;

CREATE OR REPLACE DYNAMIC TABLE DT_FORECAST_INPUT TARGET_LAG='DOWNSTREAM' WAREHOUSE=COMPUTE_WH AS
SELECT dr.order_date AS ds, dr.channel AS series, dr.total_revenue AS y,
    CASE WHEN DAYOFWEEK(dr.order_date) IN (0,6) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN dr.order_date IN ('2024-07-04','2024-09-02','2024-11-28','2024-11-29','2024-12-25','2025-01-01','2025-01-20','2025-02-17','2025-05-26','2025-07-04','2025-09-01','2025-11-27','2025-11-28','2025-12-25') THEN 1 ELSE 0 END AS is_holiday
FROM DT_DAILY_REVENUE dr;

CREATE OR REPLACE DYNAMIC TABLE DT_SPEND_DAILY TARGET_LAG='DOWNSTREAM' WAREHOUSE=COMPUTE_WH AS
SELECT spend_date AS ds, channel, ROUND(SUM(amount),2) AS total_spend, SUM(impressions) AS total_impressions, SUM(clicks) AS total_clicks, SUM(conversions) AS total_conversions,
    CASE WHEN SUM(clicks)>0 THEN ROUND(SUM(conversions)::FLOAT/SUM(clicks)*100,3) ELSE 0 END AS conversion_rate_pct
FROM MARKETING_SPEND GROUP BY spend_date, channel;

----------------------------------------------------------------------
-- STEP 4: Verification queries
----------------------------------------------------------------------
SELECT 'WHOLESALE_PARTNERS' AS tbl, COUNT(*) AS row_count FROM WHOLESALE_PARTNERS
UNION ALL SELECT 'CAMPAIGNS', COUNT(*) FROM CAMPAIGNS
UNION ALL SELECT 'CUSTOMERS', COUNT(*) FROM CUSTOMERS
UNION ALL SELECT 'ORDERS', COUNT(*) FROM ORDERS
UNION ALL SELECT 'MARKETING_SPEND', COUNT(*) FROM MARKETING_SPEND
UNION ALL SELECT 'PRODUCT_REVIEWS', COUNT(*) FROM PRODUCT_REVIEWS
ORDER BY tbl;
