-- Data cleaning

SELECT *
FROM layoffs;

-- 1. Remove Duplicate
-- 2. Standardize the data
-- 3. Null values or Blank values
-- 4. Remove Any Columns(irrelvent)


-- copy all data from layoff to create a new staging table
CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs;

WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER() OVER (PARTITION BY company,location,industry,total_laid_off,
 percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Creating another table of "layoffs_staging2"
 
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (PARTITION BY company,location,industry,total_laid_off,
 percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;


---------------------------------------------------------------------------------------------------
##STEP 2: STANDARIZE THE DATA

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2       # update the company coulumn and remove empty space before comapny name using TRIM
SET company = TRIM(company);

SELECT DISTINCT(industry)     #find that if any industry have same industry but different name like 'Crypto' or 'Crypto currency'
FROM layoffs_staging2
ORDER BY 1;

SELECT *                      #we use to find if there is any industry name which start with crypto
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2        #we update 'Crypto%' to 'Crypto'
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT country,TRIM(TRAILING '.' FROM country)    #TRIM(TRAILING '.' FROM country) is remove '.' ate the end of country name
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2        #now here we have o remove "." from some country name of 'United state'
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE "United States%";

#date is  format now we have to change date formate(year-month-date)
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

#Now we change the date from text to DATE column
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

------------------------------------------------------------------------------------------------------------------
##STEP 3 remove empty and Null value

#now we are checking where are the null values 

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company ='Airbnb';

#now we have to fill the industry who have same company name
SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
WHERE (t1.industry IS NULL OR t1.industry ='')
AND (t2.industry IS NOT NULL AND t2.industry !='');
    
#update the null and empty value 
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE (t1.industry IS NULL OR t1.industry ='')
AND (t2.industry IS NOT NULL AND t2.industry !='');

#now we delete rows where values are  null from total_laid_off and total_laid_off
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

#NOW delete the row_num column which we add at start
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
