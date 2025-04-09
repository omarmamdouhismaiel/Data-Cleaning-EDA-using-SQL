-- Data Cleaning Project
-- At the beginning, create a new schema called "world_layoffs",
-- then import our dataset from CVS file into "layoffs" table
SELECT * 
FROM world_layoffs.layoffs;

-- Create a Staging Dataset: the one we work in and clean the data
-- NOTE: we add 'row_num' column, so we can track duplicates
CREATE TABLE `layoffs_staging` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Copy Data and Add Row Numbers
INSERT INTO layoffs_staging
SELECT *, 
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs;

-- See our staging dataset
SELECT * 
FROM layoffs_staging;
-- -----------------------------------------------------------------------------------------------
-- 1. Remove Duplicates
-- See Duplicates, First
SELECT * 
FROM layoffs_staging
WHERE row_num > 1;

-- Delete Duplicates, Second
DELETE 
FROM layoffs_staging
WHERE row_num > 1;
-- -----------------------------------------------------------------------------------------------
-- 2. Standarize the Data
-- 'company' Trimming Test
SELECT company, TRIM(company)
FROM layoffs_staging;

-- 'company' Column Trimming
UPDATE layoffs_staging
SET company = TRIM(company);

-- See All 'industry' Values
SELECT DISTINCT industry 
FROM layoffs_staging
ORDER BY 1;

-- Crypto Industry written with many values
SELECT * 
FROM layoffs_staging
WHERE industry LIKE 'Crypto%';

-- Standarize it with "Crypto"
UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- See All 'country' Values 
SELECT DISTINCT country 
FROM layoffs_staging
ORDER BY 1;

-- United States country written with {"United States", "United States."}
SELECT * 
FROM layoffs_staging
WHERE country LIKE 'United States%';

-- Standarize it by deleting '.' symbol after "Unites States"
UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Update 'date' column from Text to Date
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
from layoffs_staging;

UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging	
MODIFY COLUMN `date` DATE;

-- See our result dataset
SELECT * 
FROM layoffs_staging;
-- -----------------------------------------------------------------------------------------------
-- 3. Null Values or Blank Values
-- Discover on 'industry' column, First
SELECT DISTINCT industry
FROM layoffs_staging;

-- Our blanked or null 'industry' rows
SELECT * 
FROM layoffs_staging 
WHERE industry IS NULL
OR industry = '';

-- Set blank rows as Null rows
UPDATE layoffs_staging
SET industry = NULL 
WHERE industry = '';

-- See rows with Null which can be imputed with other rows
SELECT st1.industry, st2.industry 
FROM layoffs_staging st1
JOIN layoffs_staging st2
	ON st1.company = st2.company
WHERE (st1.industry IS NULL OR st1.industry = '')
AND st2.industry IS NOT NULL;

-- Impute those rows
UPDATE layoffs_staging st1
JOIN layoffs_staging st2
	ON st1.company = st2.company
SET st1.industry = st2.industry
WHERE st1.industry IS NULL
AND st2.industry IS NOT NULL;

-- See the rest of Null 'industry' rows, It is one and can't be imputed
SELECT * 
FROM layoffs_staging 
WHERE industry IS NULL;
-- -----------------------------------------------------------------------------------------------
-- 4. Remove Any Columns or Rows
-- See rows with Null 'total_laid_off' and 'percentage_laid_off' rows
SELECT * 
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete those, are useless
DELETE
FROM layoffs_staging
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop 'row_num' column, is useless
ALTER TABLE layoffs_staging
DROP COLUMN row_num;

-- See our cleaned data for the second step -> Exploratory Data Analysis (EDA)
SELECT * 
FROM layoffs_staging;