/* ----- DATASET ----- */
/* ----- https://www.kaggle.com/datasets/swaptr/layoffs-2022 ----- */


/* ----- CHECK IMPORTED TABLE ----- */
SELECT 
    *
FROM
    layoffs_present
ORDER BY `date` ASC;
-- date range 2022-01-20 to 2025-03

/* ----- 0. CREATE STAGING TABLE ----- */ 

DROP table IF EXISTS `layoffs_present_staging`;

CREATE TABLE `layoffs_present_staging` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT DEFAULT NULL,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised` INT DEFAULT NULL
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

ALTER TABLE layoffs_present_staging
ADD row_num int;

INSERT INTO world_layoffs.layoffs_present_staging
SELECT *,
ROW_NUMBER() 
	OVER(ORDER BY `date`) 
	AS row_num
FROM world_layoffs.layoffs_present;


/* 1. STANDARDISE */

-- date
UPDATE layoffs_present_staging AS p1 
SET 
    p1.`date` = SUBSTRING(p1.`date`, 1, 10);

UPDATE layoffs_present_staging AS p1 
SET 
    p1.`date` = STR_TO_DATE(`date`, '%Y-%m-%d');

ALTER TABLE layoffs_present_staging
MODIFY COLUMN `date` DATE;


-- location
SELECT 
    location,
    SUBSTRING(location,
        3,
        (LOCATE(',', location) - 4)) AS new_location
FROM
    layoffs_present_staging
WHERE
    LOCATE(',', location) != 0;

UPDATE layoffs_present_staging AS p1 
SET 
    p1.location = SUBSTRING(p1.location,
        3,
        (LOCATE(',', p1.location) - 4))
WHERE
    LOCATE(',', location) != 0;

SELECT 
    company,
    location,
    SUBSTRING(location,
        3,
        ((LOCATE(']', location) - 4))) AS new_location
FROM
    layoffs_present_staging
WHERE
    LOCATE('[', location) = 1;

UPDATE layoffs_present_staging AS p1 
SET 
    p1.location = SUBSTRING(p1.location,
        3,
        ((LOCATE(']', p1.location) - 4)))
WHERE
    LOCATE('[', location) = 1;



-- columns
ALTER TABLE layoffs_present_staging
DROP COLUMN row_num;

UPDATE layoffs_present_staging AS p1 
SET 
    p1.percentage_laid_off = NULL
WHERE
    p1.percentage_laid_off = '';


/* 2. COMBINE _present WITH _staging_2 TABLE */

ALTER TABLE layoffs_present_staging
CHANGE COLUMN funds_raised funds_raised_millions int;

DROP TABLE IF EXISTS layoffs_2020_2025;

CREATE TABLE `layoffs_2020_2025` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT DEFAULT NULL,
    `date` DATE DEFAULT NULL,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

INSERT INTO layoffs_2020_2025
SELECT DISTINCT *
	FROM (
	SELECT * 
	FROM layoffs_staging_2
	UNION ALL
	SELECT * 
	FROM layoffs_present_staging
) AS ps;

SELECT 
    *
FROM
    layoffs_2020_2025
ORDER BY company;