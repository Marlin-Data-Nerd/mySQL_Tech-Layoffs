/* ----- DATASET  ----- */
/* ----- https://www.kaggle.com/datasets/swaptr/layoffs-2022 ----- */
/* ----- Data Cleaning in project_world-layoffs_data-cleaning.sql ----- */

/* ----- EDA ----- */

SELECT 
    *
FROM
    layoffs_staging_2;

SELECT 
    MIN(`date`), MAX(`date`)
FROM
    layoffs_staging_2;
-- returns 2020-03-11 to 2023-03-06


/* ----- MAXIMA ----- */

SELECT 
    COUNT(company) AS closed_down_amount
FROM
    layoffs_staging_2
WHERE
    percentage_laid_off = 1;
-- returns 116

SELECT 
    MAX(total_laid_off)
FROM
    layoffs_staging_2;
-- returns 12000

SELECT 
    company, percentage_laid_off
FROM
    layoffs_staging_2
WHERE
    total_laid_off = 12000;
-- returns Google 0.06 %

SELECT 
    company, SUM(total_laid_off) AS sum_laid_off
FROM
    layoffs_staging_2
GROUP BY company
ORDER BY 2 DESC;
-- returns Amazon: 18150

SELECT 
    industry, SUM(total_laid_off)
FROM
    layoffs_staging_2
GROUP BY industry
ORDER BY 2 DESC;
-- returns Consumer, Retail, Other, Transportation

SELECT 
    country, SUM(total_laid_off)
FROM
    layoffs_staging_2
GROUP BY country
ORDER BY 2 DESC;
-- returns US, India, Netherlands, Sweden


/* ----- AVERAGES AND COMPARISONS ----- */

SELECT 
    AVG(percentage_laid_off)
FROM
    layoffs_staging_2;
-- returns 0.258

SELECT company, industry
FROM 
	(
		SELECT 
		company, 
        industry,
		percentage_laid_off,
		AVG(percentage_laid_off) OVER() as avg_perc
		FROM layoffs_staging_2
	) AS avg_perc_table
WHERE percentage_laid_off > avg_perc;
-- 465 companies with SELECT COUNT(company)

SELECT 
    YEAR(`date`) AS `year`, SUM(total_laid_off) AS total_layoffs
FROM
    layoffs_staging_2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;
-- returns layoffs per year


/* ----- ROLLING SUMS OVER MONTHS ----- */

WITH rolling_total AS
(
SELECT substring(`date`,1,7) AS `month`, SUM(total_laid_off) AS sum_laid_off
FROM layoffs_staging_2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `month`
ORDER BY 1 ASC
)

SELECT `month`, sum_laid_off, SUM(sum_laid_off) OVER(ORDER BY `month`) AS roll_laid_off
FROM rolling_total;


/* ----- BY COMPANY BY YEAR ----- */

SELECT 
    company,
    YEAR(`date`) AS `year`,
    SUM(total_laid_off) AS laid_off
FROM
    layoffs_staging_2
GROUP BY company , YEAR(`date`)
ORDER BY company ASC;


/* ----- BY COMPANY BY YEAR RANKED ----- */

-- company grouped by year and ranked by highest total_laid_off
WITH company_year (company, `year`, laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging_2
GROUP BY company, YEAR(`date`)
),
company_year_rank AS
(
SELECT *, 
DENSE_RANK() OVER (PARTITION BY `year` ORDER BY laid_off DESC) AS rank_layoff
FROM company_year
WHERE `year`IS NOT NULL
) 

-- highest total_laid_off per year
SELECT *
FROM company_year_rank
WHERE rank_layoff = 1;
-- returns Uber, Bytedance, Meta, Google
