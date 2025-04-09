-- Exploratory Data Analysis (EDA)
-- Explore the data and find trends or patterns or anything interesting like outliers
-- See our dataset, First
SELECT * 
FROM world_layoffs.layoffs_staging;

-- Get the maximum total laid off
SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging
WHERE  percentage_laid_off = 1;
-- These are mostly startups it looks like who all went out of business during this time

-- If we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Companies with the biggest single Layoff
SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;
-- now that's just on a single day

-- Companies with the most Total Layoffs
SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

-- By location
SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;

-- By country
SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY country
ORDER BY 2 DESC;

-- By year
SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
Where YEAR(date) IS NOT NULL
GROUP BY YEAR(date)
ORDER BY 1 ASC;

-- By Industry
SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
GROUP BY industry
ORDER BY 2 DESC;

-- By stage
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging
WHERE stage IS NOT NULL
GROUP BY stage
ORDER BY 2 DESC;

-- First 3 Companies with the most Layoffs per year
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

-- Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY dates
ORDER BY dates ASC;

-- Rolling Total of Layoffs Per Month
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE SUBSTRING(date,1,7) IS NOT NULL
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;