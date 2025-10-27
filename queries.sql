-- ===============================
-- CREATE DIMENSION TABLES
-- ===============================

CREATE TABLE dim_student (
    student_id INT PRIMARY KEY,
    full_name VARCHAR(100),
    gender VARCHAR(10),
    enrollment_year INT,
    department VARCHAR(50)
);

INSERT INTO dim_student (student_id, full_name, gender, enrollment_year, department)
VALUES
(1, 'Aarav Mehta', 'Male', 2022, 'Computer Science'),
(2, 'Ishita Sharma', 'Female', 2023, 'Physics'),
(3, 'Rohan Verma', 'Male', 2022, 'Mathematics'),
(4, 'Sneha Patel', 'Female', 2023, 'Computer Science'),
(5, 'Karan Singh', 'Male', 2022, 'Physics');

SELECT * FROM dim_student;

-- ===============================
-- COURSE DIMENSION
-- ===============================

CREATE TABLE dim_course (
    course_id INT PRIMARY KEY,
    course_name VARCHAR(100),
    department VARCHAR(50),
    credits INT
);

INSERT INTO dim_course (course_id, course_name, department, credits)
VALUES
(101, 'Data Structures', 'Computer Science', 4),
(102, 'Quantum Physics', 'Physics', 3),
(103, 'Linear Algebra', 'Mathematics', 4),
(104, 'Operating Systems', 'Computer Science', 4),
(105, 'Classical Mechanics', 'Physics', 3);

-- ===============================
-- TIME DIMENSION
-- ===============================

CREATE TABLE dim_time (
    time_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    year INT,
    semester VARCHAR(20)
);

INSERT INTO dim_time (time_id, full_date, day, month, year, semester)
VALUES
(20240101, '2024-01-01', 1, 1, 2024, 'Semester 1'),
(20240701, '2024-07-01', 1, 7, 2024, 'Semester 2'),
(20230101, '2023-01-01', 1, 1, 2023, 'Semester 1'),
(20230701, '2023-07-01', 1, 7, 2023, 'Semester 2');

-- ===============================
-- FACT TABLE
-- ===============================

CREATE TABLE fact_enrollment (
    enrollment_id INT PRIMARY KEY AUTO_INCREMENT,
    student_id INT,
    course_id INT,
    time_id INT,
    grade DECIMAL(5,2),
    FOREIGN KEY (student_id) REFERENCES dim_student(student_id),
    FOREIGN KEY (course_id) REFERENCES dim_course(course_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);

INSERT INTO fact_enrollment (student_id, course_id, time_id, grade)
VALUES
-- Semester 1 2024
(1, 101, 20240101, 85.0),
(1, 103, 20240101, 78.5),
(2, 102, 20240101, 88.0),
(3, 103, 20240101, 91.0),
(4, 101, 20240101, 82.5),
-- Semester 2 2024
(1, 104, 20240701, 89.0),
(2, 105, 20240701, 84.0),
(4, 104, 20240701, 88.5),
(5, 105, 20240701, 80.0),
-- Semester 1 2023
(3, 103, 20230101, 75.0),
(5, 102, 20230101, 77.0),
-- Semester 2 2023
(1, 101, 20230701, 90.0),
(2, 102, 20230701, 85.0),
(4, 101, 20230701, 91.0);

-- ===============================
-- OLAP QUERIES
-- ===============================

-- 1. ROLL UP: Average grade per department by year
SELECT
    c.department,
    t.year,
    ROUND(AVG(e.grade), 2) AS avg_grade
FROM fact_enrollment e
JOIN dim_course c ON e.course_id = c.course_id
JOIN dim_time t ON e.time_id = t.time_id
GROUP BY c.department, t.year
ORDER BY c.department, t.year;

-- 2. DRILL DOWN: Average grade per course per semester
SELECT
    c.course_name,
    t.year,
    t.semester,
    ROUND(AVG(e.grade), 2) AS avg_grade
FROM fact_enrollment e
JOIN dim_course c ON e.course_id = c.course_id
JOIN dim_time t ON e.time_id = t.time_id
GROUP BY c.course_name, t.year, t.semester
ORDER BY c.course_name, t.year, t.semester;

-- 3. SLICE: Average grades for Semester 1
SELECT
    c.course_name,
    ROUND(AVG(e.grade), 2) AS avg_grade
FROM fact_enrollment e
JOIN dim_course c ON e.course_id = c.course_id
JOIN dim_time t ON e.time_id = t.time_id
WHERE t.semester = 'Semester 1'
GROUP BY c.course_name;

-- 4. DICE: Average grades for female students in 2024 in CS courses
SELECT
    s.full_name,
    c.course_name,
    ROUND(e.grade, 2) AS grade
FROM fact_enrollment e
JOIN dim_student s ON e.student_id = s.student_id
JOIN dim_course c ON e.course_id = c.course_id
JOIN dim_time t ON e.time_id = t.time_id
WHERE s.gender = 'Female'
  AND t.year = 2024
  AND c.department = 'Computer Science';

-- 5. PIVOT: Semester-wise GPA per student
SELECT
    s.full_name,
    t.year,
    t.semester,
    ROUND(AVG(e.grade), 2) AS gpa
FROM fact_enrollment e
JOIN dim_student s ON e.student_id = s.student_id
JOIN dim_time t ON e.time_id = t.time_id
GROUP BY s.full_name, t.year, t.semester
ORDER BY s.full_name, t.year, t.semester;
