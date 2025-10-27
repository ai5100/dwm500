-- ===========================================================
-- 🏨 HOTEL MANAGEMENT DATA WAREHOUSE (FULL ETL SCRIPT)
-- ===========================================================


-- -----------------------------------------------------------
-- 2️⃣ Dimension Tables
-- -----------------------------------------------------------

-- 📅 Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
  date_id INT PRIMARY KEY,
  full_date DATE,
  day INT,
  month INT,
  year INT,
  weekday_name VARCHAR(10),
  is_weekend BOOLEAN
);

-- 👥 Customer Dimension
CREATE TABLE IF NOT EXISTS dim_customers (
  cust_id INT PRIMARY KEY,
  full_name VARCHAR(100),
  gender VARCHAR(10),
  phone_number VARCHAR(20),
  email VARCHAR(100),
  country VARCHAR(50)
);

-- 🏨 Hotel Dimension
CREATE TABLE IF NOT EXISTS dim_hotel (
  hotel_id INT PRIMARY KEY,
  hotel_name VARCHAR(100),
  city VARCHAR(50),
  state VARCHAR(50),
  country VARCHAR(50),
  star_rating INT
);

-- 🛏️ Room Dimension
CREATE TABLE IF NOT EXISTS dim_room (
  room_id INT PRIMARY KEY,
  room_number VARCHAR(10),
  room_type VARCHAR(50),
  bed_type VARCHAR(50),
  price_per_night DECIMAL(10,2)
);

-- 👨‍🍳 Employee Dimension
CREATE TABLE IF NOT EXISTS dim_employee (
  employee_id INT PRIMARY KEY,
  full_name VARCHAR(100),
  position VARCHAR(50),
  department VARCHAR(50),
  contact_number VARCHAR(20)
);

-- -----------------------------------------------------------
-- 3️⃣ FACT TABLE
-- -----------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_booking (
  booking_id INT PRIMARY KEY,
  customer_id INT,
  hotel_id INT,
  room_id INT,
  employee_id INT,
  checkin_date_id INT,
  checkout_date_id INT,
  total_nights INT,
  total_amount DECIMAL(10,2),
  FOREIGN KEY (customer_id) REFERENCES dim_customers(cust_id),
  FOREIGN KEY (hotel_id) REFERENCES dim_hotel(hotel_id),
  FOREIGN KEY (room_id) REFERENCES dim_room(room_id),
  FOREIGN KEY (employee_id) REFERENCES dim_employee(employee_id),
  FOREIGN KEY (checkin_date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY (checkout_date_id) REFERENCES dim_date(date_id)
);

-- -----------------------------------------------------------
-- 4️⃣ Populate dim_date for July 2024 (Fixed Procedure Method)
-- -----------------------------------------------------------

DELIMITER $$

DROP PROCEDURE IF EXISTS populate_july2024 $$
CREATE PROCEDURE populate_july2024()
BEGIN
  DECLARE cur_date DATE;
  SET cur_date = '2024-07-01';

  WHILE cur_date <= '2024-07-31' DO
    INSERT INTO dim_date (date_id, full_date, day, month, year, weekday_name, is_weekend)
    VALUES (
      CAST(DATE_FORMAT(cur_date, '%Y%m%d') AS SIGNED),
      cur_date,
      DAY(cur_date),
      MONTH(cur_date),
      YEAR(cur_date),
      DAYNAME(cur_date),
      CASE WHEN DAYOFWEEK(cur_date) IN (1,7) THEN TRUE ELSE FALSE END
    )
    ON DUPLICATE KEY UPDATE full_date = VALUES(full_date);

    SET cur_date = DATE_ADD(cur_date, INTERVAL 1 DAY);
  END WHILE;
END $$
DELIMITER ;

CALL populate_july2024();
DROP PROCEDURE IF EXISTS populate_july2024;

-- -----------------------------------------------------------
-- 5️⃣ ETL: SOURCE TABLES (EXTRACTION)
-- -----------------------------------------------------------

CREATE TABLE IF NOT EXISTS source_customers (
  cust_id INT PRIMARY KEY,
  full_name VARCHAR(100),
  gender VARCHAR(10),
  phone_number VARCHAR(20),
  email VARCHAR(100),
  country VARCHAR(50)
);

INSERT INTO source_customers VALUES
(1, 'Aarav Sharma', 'Male', '9876543210', 'aarav.sharma@example.in', 'India'),
(2, 'Priya Singh', 'Female', '9123456789', 'priya.singh@example.in', 'India'),
(3, 'Rohit Kumar', 'Male', '9988776655', 'rohit.kumar@example.in', 'India'),
(4, 'Sneha Patel', 'Female', '9871122334', 'sneha.patel@example.in', 'India'),
(5, 'Vikram Joshi', 'Male', '9654321789', 'vikram.joshi@example.in', 'India');

CREATE TABLE IF NOT EXISTS source_hotels (
  hotel_id INT PRIMARY KEY,
  hotel_name VARCHAR(100),
  city VARCHAR(50),
  state VARCHAR(50),
  country VARCHAR(50),
  star_rating INT
);

INSERT INTO source_hotels VALUES
(1, 'Taj Mahal Palace', 'Mumbai', 'Maharashtra', 'India', 5),
(2, 'The Oberoi Udaivilas', 'Udaipur', 'Rajasthan', 'India', 5),
(3, 'ITC Grand Chola', 'Chennai', 'Tamil Nadu', 'India', 5),
(4, 'The Leela Palace', 'Bengaluru', 'Karnataka', 'India', 5),
(5, 'Radisson Blu Plaza', 'New Delhi', 'Delhi', 'India', 4);

CREATE TABLE IF NOT EXISTS source_rooms (
  room_id INT PRIMARY KEY,
  room_number VARCHAR(10),
  room_type VARCHAR(50),
  bed_type VARCHAR(50),
  price_per_night DECIMAL(10,2)
);

INSERT INTO source_rooms VALUES
(1, '101', 'Deluxe', 'King', 4500.00),
(2, '102', 'Standard', 'Queen', 3000.00),
(3, '201', 'Suite', 'King', 8000.00),
(4, '202', 'Deluxe', 'Twin', 4700.00),
(5, '301', 'Standard', 'Single', 2500.00);

CREATE TABLE IF NOT EXISTS source_employees (
  employee_id INT PRIMARY KEY,
  full_name VARCHAR(100),
  position VARCHAR(50),
  department VARCHAR(50),
  contact_number VARCHAR(20)
);

INSERT INTO source_employees VALUES
(1, 'Rahul Verma', 'Receptionist', 'Front Desk', '9876543210'),
(2, 'Neha Gupta', 'Housekeeping Supervisor', 'Housekeeping', '9123456789'),
(3, 'Anil Kumar', 'Manager', 'Operations', '9988776655'),
(4, 'Priya Reddy', 'Chef', 'Kitchen', '9871122334'),
(5, 'Suresh Patel', 'Security Officer', 'Security', '9654321789');

CREATE TABLE IF NOT EXISTS source_bookings (
  booking_id INT PRIMARY KEY,
  customer_id INT,
  hotel_id INT,
  room_id INT,
  employee_id INT,
  checkin_date DATE,
  checkout_date DATE,
  total_nights INT,
  total_amount DECIMAL(10,2)
);

INSERT INTO source_bookings VALUES
(1, 1, 1, 1, 1, '2024-07-10', '2024-07-13', 3, 13500.00),
(2, 2, 2, 3, 3, '2024-07-11', '2024-07-14', 3, 24000.00),
(3, 3, 3, 2, 2, '2024-07-12', '2024-07-15', 3, 9000.00),
(4, 4, 4, 4, 4, '2024-07-13', '2024-07-14', 1, 4700.00),
(5, 5, 5, 5, 5, '2024-07-14', '2024-07-16', 2, 5000.00);

-- -----------------------------------------------------------
-- 6️⃣ TRANSFORMATION QUERIES
-- -----------------------------------------------------------

-- Example 1: Transform Customer Names (Title Case)
SELECT
 cust_id,
 CONCAT(UPPER(SUBSTRING(full_name, 1, 1)), LOWER(SUBSTRING(full_name, 2))) AS full_name,
 gender,
 phone_number,
 email,
 country
FROM source_customers;

-- Example 2: Transform Hotel Names to Uppercase
SELECT
 hotel_id,
 UPPER(hotel_name) AS hotel_name,
 city,
 state,
 country,
 star_rating
FROM source_hotels;

-- -----------------------------------------------------------
-- 7️⃣ LOAD INTO DIMENSIONS (LOAD PHASE)
-- -----------------------------------------------------------

-- Load Customers
INSERT INTO dim_customers (cust_id, full_name, gender, phone_number, email, country)
SELECT s.cust_id, s.full_name, s.gender, s.phone_number, s.email, s.country
FROM source_customers s
WHERE NOT EXISTS (SELECT 1 FROM dim_customers d WHERE d.cust_id = s.cust_id);

-- Load Hotels
INSERT INTO dim_hotel (hotel_id, hotel_name, city, state, country, star_rating)
SELECT s.hotel_id, s.hotel_name, s.city, s.state, s.country, s.star_rating
FROM source_hotels s
WHERE NOT EXISTS (SELECT 1 FROM dim_hotel d WHERE d.hotel_id = s.hotel_id);

-- Load Rooms
INSERT INTO dim_room (room_id, room_number, room_type, bed_type, price_per_night)
SELECT s.room_id, s.room_number, s.room_type, s.bed_type, s.price_per_night
FROM source_rooms s
WHERE NOT EXISTS (SELECT 1 FROM dim_room d WHERE d.room_id = s.room_id);

-- Load Employees
INSERT INTO dim_employee (employee_id, full_name, position, department, contact_number)
SELECT s.employee_id, s.full_name, s.position, s.department, s.contact_number
FROM source_employees s
WHERE NOT EXISTS (SELECT 1 FROM dim_employee d WHERE d.employee_id = s.employee_id);

-- Load Fact Bookings
INSERT INTO fact_booking (
  booking_id, customer_id, hotel_id, room_id, employee_id,
  checkin_date_id, checkout_date_id, total_nights, total_amount
)
SELECT
  b.booking_id,
  b.customer_id,
  b.hotel_id,
  b.room_id,
  b.employee_id,
  CAST(DATE_FORMAT(b.checkin_date, '%Y%m%d') AS SIGNED),
  CAST(DATE_FORMAT(b.checkout_date, '%Y%m%d') AS SIGNED),
  b.total_nights,
  b.total_amount
FROM source_bookings b
WHERE NOT EXISTS (SELECT 1 FROM fact_booking f WHERE f.booking_id = b.booking_id);

-- -----------------------------------------------------------
-- ✅ Verification Queries
-- -----------------------------------------------------------
SELECT COUNT(*) AS dim_date_count FROM dim_date;
SELECT COUNT(*) AS dim_customer_count FROM dim_customers;
SELECT COUNT(*) AS dim_hotel_count FROM dim_hotel;
SELECT COUNT(*) AS dim_room_count FROM dim_room;
SELECT COUNT(*) AS dim_employee_count FROM dim_employee;
SELECT COUNT(*) AS fact_booking_count FROM fact_booking;

SELECT * FROM fact_booking;
