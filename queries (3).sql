-- ===========================================================
-- 🏨 HOTEL MANAGEMENT DATA WAREHOUSE (STAR SCHEMA + OLAP)
-- Corrected: replaced unsupported WITH CUBE with UNION ALL emulation
-- ===========================================================

-- -----------------------------------------------------------
-- 1️⃣ Create Database
-- -----------------------------------------------------------

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
-- 3️⃣ Fact Table
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
-- 4️⃣ Sample Data (Minimal for OLAP Testing)
-- -----------------------------------------------------------

INSERT INTO dim_date VALUES
(20240710, '2024-07-10', 10, 7, 2024, 'Wednesday', FALSE),
(20240711, '2024-07-11', 11, 7, 2024, 'Thursday', FALSE),
(20240712, '2024-07-12', 12, 7, 2024, 'Friday', FALSE),
(20240713, '2024-07-13', 13, 7, 2024, 'Saturday', TRUE),
(20240714, '2024-07-14', 14, 7, 2024, 'Sunday', TRUE);

INSERT INTO dim_customers VALUES
(1, 'Aarav Sharma', 'Male', '9876543210', 'aarav@example.in', 'India'),
(2, 'Priya Singh', 'Female', '9123456789', 'priya@example.in', 'India');

INSERT INTO dim_hotel VALUES
(1, 'Taj Mahal Palace', 'Mumbai', 'Maharashtra', 'India', 5),
(2, 'The Oberoi Udaivilas', 'Udaipur', 'Rajasthan', 'India', 5);

INSERT INTO dim_room VALUES
(1, '101', 'Deluxe', 'King', 4500.00),
(2, '102', 'Standard', 'Queen', 3000.00);

INSERT INTO dim_employee VALUES
(1, 'Rahul Verma', 'Receptionist', 'Front Desk', '9876543210'),
(2, 'Neha Gupta', 'Manager', 'Operations', '9123456789');

INSERT INTO fact_booking VALUES
(1, 1, 1, 1, 1, 20240710, 20240713, 3, 13500.00),
(2, 2, 2, 2, 2, 20240711, 20240714, 3, 9000.00);

-- -----------------------------------------------------------
-- 5️⃣ OLAP OPERATIONS
-- -----------------------------------------------------------

-- 🔹 (a) ROLLUP → Total Revenue by Hotel, City, Country
SELECT 
    h.country,
    h.city,
    h.hotel_name,
    SUM(f.total_amount) AS total_revenue
FROM fact_booking f
JOIN dim_hotel h ON f.hotel_id = h.hotel_id
GROUP BY h.country, h.city, h.hotel_name WITH ROLLUP;

-- 🔹 (b) CUBE emulation → Revenue by Hotel and Room Type
-- MySQL does not support WITH CUBE; emulate with UNION ALL:
SELECT h.hotel_name, r.room_type, SUM(f.total_amount) AS total_revenue, 'hotel+room' AS grouping_level
FROM fact_booking f
JOIN dim_hotel h ON f.hotel_id = h.hotel_id
JOIN dim_room r ON f.room_id = r.room_id
GROUP BY h.hotel_name, r.room_type

UNION ALL

SELECT h.hotel_name, NULL AS room_type, SUM(f.total_amount) AS total_revenue, 'hotel' AS grouping_level
FROM fact_booking f
JOIN dim_hotel h ON f.hotel_id = h.hotel_id
JOIN dim_room r ON f.room_id = r.room_id
GROUP BY h.hotel_name

UNION ALL

SELECT NULL AS hotel_name, r.room_type, SUM(f.total_amount) AS total_revenue, 'room' AS grouping_level
FROM fact_booking f
JOIN dim_hotel h ON f.hotel_id = h.hotel_id
JOIN dim_room r ON f.room_id = r.room_id
GROUP BY r.room_type

UNION ALL

SELECT NULL AS hotel_name, NULL AS room_type, SUM(f.total_amount) AS total_revenue, 'ALL' AS grouping_level
FROM fact_booking f
JOIN dim_hotel h ON f.hotel_id = h.hotel_id
JOIN dim_room r ON f.room_id = r.room_id

ORDER BY grouping_level, hotel_name, room_type;

-- 🔹 (c) DRILL-DOWN → Daily Revenue (Year → Month → Day)
SELECT 
    d.year,
    d.month,
    d.day,
    SUM(f.total_amount) AS daily_revenue
FROM fact_booking f
JOIN dim_date d ON f.checkin_date_id = d.date_id
GROUP BY d.year, d.month, d.day
ORDER BY d.year, d.month, d.day;

-- 🔹 (d) SLICE → Only 5-Star Hotels
SELECT 
    h.hotel_name,
    SUM(f.total_amount) AS total_revenue
FROM fact_booking f
JOIN dim_hotel h ON f.hotel_id = h.hotel_id
WHERE h.star_rating = 5
GROUP BY h.hotel_name;

-- 🔹 (e) DICE → July 2024 + Deluxe Rooms
SELECT 
    h.city,
    r.room_type,
    SUM(f.total_amount) AS total_revenue
FROM fact_booking f
JOIN dim_hotel h ON f.hotel_id = h.hotel_id
JOIN dim_room r ON f.room_id = r.room_id
JOIN dim_date d ON f.checkin_date_id = d.date_id
WHERE d.month = 7 AND d.year = 2024 AND r.room_type = 'Deluxe'
GROUP BY h.city, r.room_type;

-- -----------------------------------------------------------
-- ✅ Verification Queries
-- -----------------------------------------------------------
SELECT COUNT(*) AS total_bookings FROM fact_booking;
SELECT * FROM fact_booking;
