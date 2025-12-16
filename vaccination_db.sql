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
