SELECT *
FROM layoffs; 

CREATE TABLE layoffs_staging
LIKE layoffs; 

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

SELECT * , 
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`
) as row_num 
FROM layoffs_staging ;

with duplicate_CTE as 
(
SELECT * , 
ROW_NUMBER() OVER(
PARTITION BY company, `location`,  industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num 
FROM layoffs_staging 
)
SELECT *
from duplicate_CTE 
WHERE row_num>1 ;

SELECT *
FROM layoffs_staging
WHERE company='Cazoo';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` float DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

DROP TABLE layoffs_staging2;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` float DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int(11) DEFAULT NULL,  `row_num` INT 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT * , 
ROW_NUMBER() OVER(
PARTITION BY company, `location`,  industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
) as row_num 
FROM layoffs_staging;  

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1 
; 

UPDATE layoffs_staging2
SET company=TRIM(company)
;

SELECT DISTINCT `date`
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET country='United States'
WHERE country LIKE 'Crypto%';

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
modify COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off is NULL
and percentage_laid_off is NULL;

SELECT DISTINCT * 
FROM layoffs_staging2
WHERE industry is null 
or industry='';

SELECT *
FROM layoffs_staging2 t1 
JOIN layoffs_staging2 t2  
  on t1.company=t2.company
  and t1.`location`=t2.`location`
WHERE (t1.industry is NULL or t1.industry='')
AND t2.industry is NOT NULL;

UPDATE layoffs_staging2
SET industry=NULL
WHERE industry='';
  
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
  on t1.company=t2.company
  SET t1.industry=t2.industry
WHERE t1.industry is NULL
AND t2.industry is NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company='Airbnb';

DELETE
FROM layoffs_staging2
WHERE total_laid_off is NULL
AND percentage_laid_off is NULL;

alter TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;