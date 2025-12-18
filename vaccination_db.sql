create database if not exists vaccination_db;
use vaccination_db;

CREATE TABLE vaccination_coverage (
    `group` VARCHAR(50), 
    code VARCHAR(10),
    name VARCHAR(100),
    year INT,
    antigen VARCHAR(50),
    antigen_description TEXT,
    coverage_category VARCHAR(50),
    coverage_category_description TEXT,
    target_number DECIMAL(15,2),
    doses DECIMAL(15,2),
    coverage DECIMAL(5,2)
);

CREATE TABLE disease_incidence (
    `group` VARCHAR(50),
    code VARCHAR(10),
    name VARCHAR(100),
    year INT,
    disease VARCHAR(50),
    disease_description TEXT,
    denominator VARCHAR(100),
    incidence_rate DECIMAL(10,2)
);

CREATE TABLE reported_cases (
    `group` VARCHAR(50),
    code VARCHAR(10),
    name VARCHAR(100),
    year INT,
    disease VARCHAR(50),
    disease_description TEXT,
    cases DECIMAL(15,2)
);

CREATE TABLE vaccine_introduction (
    iso_3_code VARCHAR(10),
    countryname VARCHAR(100),
    who_region VARCHAR(20),
    year INT,
    description TEXT,
    intro VARCHAR(10)
);

CREATE TABLE vaccine_schedule (
    iso_3_code VARCHAR(10),
    countryname VARCHAR(100),
    who_region VARCHAR(20),
    year INT,
    vaccinecode VARCHAR(50),
    vaccine_description TEXT,
    schedulerounds DECIMAL(5,1),
    targetpop VARCHAR(50),
    targetpop_description TEXT,
    geoarea VARCHAR(50),
    ageadministered VARCHAR(50),
    sourcecomment TEXT
);

select * from vaccination_db.vaccine_schedule;
SELECT COUNT(DISTINCT code) FROM vaccination_db.vaccination_coverage;


USE vaccination_db;

SELECT 
    t1.name AS Country, 
    t1.year, 
    t1.coverage AS DTP1_Coverage, 
    t3.coverage AS DTP3_Coverage,
    (t1.coverage - t3.coverage) AS Dropoff_Rate
FROM vaccination_coverage t1
JOIN vaccination_coverage t3 
    ON t1.code = t3.code AND t1.year = t3.year
WHERE t1.antigen = 'DTPCV1' AND t3.antigen = 'DTPCV3'
ORDER BY Dropoff_Rate DESC
LIMIT 20;

SELECT 
    v.who_region,
    ROUND(AVG(c.coverage), 2) as Avg_Coverage
FROM vaccination_coverage c
JOIN vaccine_introduction v ON c.code = v.iso_3_code
WHERE c.year = 2023
GROUP BY v.who_region
ORDER BY Avg_Coverage ASC;

select group from vaccination_db.vaccination_coverage;
SELECT DISTINCT year FROM vaccination_db.vaccination_coverage ORDER BY year;

USE vaccination_db;
CREATE OR REPLACE VIEW analytics_immunization_summary AS
SELECT 
    -- Geographic & Time Info
    vac.code AS country_code,
    vac.name AS country_name,
    vac.year,

    -- Vaccination Info
    vac.antigen AS vaccine_code,
    vac.antigen_description AS vaccine_name,
    vac.coverage AS vaccination_coverage_percent,

    -- Disease Outcome Info
    inc.disease AS disease_code,
    inc.disease_description AS disease_name,
    inc.incidence_rate AS disease_incidence_rate,
    inc.denominator AS population_denominator

FROM 
    vaccination_coverage vac

-- 1. JOIN Coverage to the Bridge (Connect Vaccine to Disease)
INNER JOIN 
    vaccine_disease_bridge bridge 
    ON vac.antigen = bridge.vaccine_code

-- 2. JOIN to Incidence (Connect Country+Year+Disease)
INNER JOIN 
    disease_incidence inc 
    ON vac.code = inc.code 
    AND vac.year = inc.year 
    AND bridge.target_disease_code = inc.disease

-- 3. FILTER for the best data quality (WUENIC)
WHERE 
    vac.coverage_category = 'WUENIC';
    
SELECT * FROM analytics_immunization_summary LIMIT 10;