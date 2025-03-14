/* ----- DATASET ----- */
/* ----- https://www.kaggle.com/datasets/swaptr/layoffs-2022 ----- */


/* ----- CHECK IMPORTED TABLE ----- */
SELECT 
    *
FROM
    world_layoffs.layoffs
LIMIT 10;


/* ----- 0. CREATE STAGING TABLE ----- */

DROP table IF EXISTS `layoffs_staging`;

CREATE TABLE world_layoffs.layoffs_staging LIKE world_layoffs.layoffs;

INSERT world_layoffs.layoffs_staging
SELECT *
FROM world_layoffs.layoffs;


/* ----- 1. REMOVE DUPLICATES ----- */

DROP table IF EXISTS `layoffs_staging_2`;

-- created via: table > copy to clipboard > create statement
CREATE TABLE `layoffs_staging_2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` TEXT,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` TEXT,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

INSERT INTO layoffs_staging_2 
SELECT *,
ROW_NUMBER()
	OVER(
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
        `date`, stage, country, funds_raised_millions
		)
	AS row_num
FROM layoffs_staging;

SELECT 
    *
FROM
    layoffs_staging_2
WHERE
    row_num > 1;

DELETE FROM layoffs_staging_2 
WHERE
    row_num > 1;


/* ----- 2. STANDARDISE ----- */

/* ----- error with text instead of NULL values  ----- */
/* ----- based in csv to json conversion ----- */
/* ----- conversion for correct import on macos ----- */


UPDATE layoffs_staging_2 
SET 
    industry = NULL
WHERE
    industry LIKE 'NULL';

UPDATE layoffs_staging_2 
SET 
    total_laid_off = NULL
WHERE
    total_laid_off LIKE 'NULL';

UPDATE layoffs_staging_2 
SET 
    percentage_laid_off = NULL
WHERE
    percentage_laid_off LIKE 'NULL';

UPDATE layoffs_staging_2 
SET 
    stage = NULL
WHERE
    stage LIKE 'NULL';

UPDATE layoffs_staging_2 
SET 
    funds_raised_millions = NULL
WHERE
    funds_raised_millions LIKE 'NULL';


/* ----- "clearify" data based on column standard ----- */

-- industry
SELECT DISTINCT
    industry
FROM
    layoffs_staging_2
WHERE
    LOWER(industry) LIKE '%crypto%';

UPDATE layoffs_staging_2 
SET 
    industry = 'Crypto'
WHERE
    industry LIKE 'Crypto%';

-- location
SELECT DISTINCT
    location
FROM
    layoffs_staging_2
ORDER BY 1;

-- country
SELECT DISTINCT
    country
FROM
    layoffs_staging_2
ORDER BY 1;

SELECT DISTINCT
    country
FROM
    layoffs_staging_2
WHERE
    country LIKE 'United S%';

UPDATE layoffs_staging_2 
SET 
    country = 'United States'
WHERE
    country LIKE 'United States%';

-- stage
SELECT DISTINCT
    stage
FROM
    layoffs_staging_2;

SELECT 
    company, stage
FROM
    layoffs_staging_2
WHERE
    stage = 'Unknown';

SELECT 
    l1.company, l1.stage, l2.company, l2.stage
FROM
    layoffs_staging_2 AS l1
        JOIN
    layoffs_staging_2 AS l2 ON l1.company = l2.company
WHERE
    l1.stage = 'Unknown'
        AND l2.stage != 'Unknown';

UPDATE layoffs_staging_2 AS l1
        JOIN
    layoffs_staging_2 AS l2 ON l1.company = l2.company 
SET 
    l1.stage = l2.stage
WHERE
    l1.stage = 'Unknown'
        AND l2.stage != 'Unknown';


/* ----- streamline data types ----- */

-- date
SELECT 
    `date`, STR_TO_DATE(`date`, '%m/%d/%Y') AS new_date
FROM
    layoffs_staging_2;

UPDATE layoffs_staging_2 
SET 
    `date` = NULL
WHERE
    `date` = 'NULL';

UPDATE layoffs_staging_2 
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- correct data types of columns 
-- (only neccessary because of csv to json conversion)
ALTER TABLE layoffs_staging_2
MODIFY COLUMN `date` DATE;

ALTER TABLE layoffs_staging_2
MODIFY COLUMN total_laid_off INT;

ALTER TABLE layoffs_staging_2
MODIFY COLUMN funds_raised_millions INT;


/* ----- 3. NULL AND BLANK VALUES ----- */

/* ----- populate based on db ----- */

-- industry
SELECT 
    *
FROM
    layoffs_staging_2
WHERE
    industry IS NULL OR industry = '';

UPDATE layoffs_staging_2 t1
        JOIN
    layoffs_staging_2 t2 ON t1.company = t2.company 
SET 
    t1.industry = t2.industry
WHERE
    t1.industry IS NULL
        AND t2.industry IS NOT NULL;

-- laid_off
SELECT 
    *
FROM
    layoffs_staging_2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

-- in this example: delete if both values are empty
DELETE FROM layoffs_staging_2 
WHERE
    total_laid_off IS NULL
    AND percentage_laid_off IS NULL;


-- funds_raised
SELECT 
    company, funds_raised_millions
FROM
    layoffs_staging_2
WHERE
    funds_raised_millions IS NULL;

UPDATE layoffs_staging_2 AS l1
        JOIN
    layoffs_staging_2 AS l2 ON l1.company = l2.company 
SET 
    l1.funds_raised_millions = l2.funds_raised_millions
WHERE
    l1.funds_raised_millions IS NULL
        AND l2.funds_raised_millions IS NOT NULL;


/* ----- 4. DELETE 'UNNECCESSARY 'COLUMNS ----- */

ALTER TABLE layoffs_staging_2
DROP COLUMN row_num;

SELECT 
    *
FROM
    layoffs_staging_2