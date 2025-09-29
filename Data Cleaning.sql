-- DATA CLEANING
-- remove duplicates
-- standardrize data
--  null values or blank values
-- remove any column



select * from layoffs;
-- create copy of table
create table layoffs_staging 
like layoffs;
select * from layoffs_staging;
-- take copy from old table to new table
insert into layoffs_staging select * from layoffs;

-- we use ROW_NUMBER AND OVER for detect duplicates
select *, 
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging;
-- create cte and use it
WITH duplicate_cte AS(
select *, 
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging
)

select * 
from duplicate_cte where row_num > 1;

-- use cte that we created for detect duplicate for delete it (didn't work)
WITH duplicate_cte AS(
select *, 
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging
)

DELETE 
FROM duplicate_cte 
where row_num > 1;


-- try to create other table and add column in it that allow us to calculate row_num
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
-- insert copy of data from layoff_staging + row_num
INSERT INTO layoffs_staging2
select *, 
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
from layoffs_staging;

-- now we can delete duplicate 
SET SQL_SAFE_UPDATES = 0;
DELETE 
from layoffs_staging2
where row_num>1;


select * from layoffs_staging2 where row_num>1;

-- Standardizing Data (finding issues in data and fixing it)
-- starting with trim() for example
select distinct (trim(company)) 
from layoffs_staging2;

UPDATE layoffs_staging2
set company=TRIM(company);

-- here we start clean like just write Crypto instead of CryptoCurrency
SELECT *
FROM layoffs_staging2
WHERE Industry Like "Crypto%";

UPDATE layoffs_staging2
Set industry="Crypto"
WHERE industry Like "Crypto%";

-- lets check again to sure there is no duplicate in industry
select distinct industry from layoffs_staging2 order by 1;

-- lets check other columns to check there is no duplicates (we found one in country)
select distinct country from layoffs_staging2 order by 1;

-- TRAILING is trim specific character and in our case . after united states.
SELECT DISTINCT country,TRIM(TRAILING '.' from country) from layoffs_staging2 ORDER BY 1;

update layoffs_staging2
set country=TRIM(TRAILING '.' from country)
where country LIKE "United States%";

-- start work and cleaning date
select date,
str_to_date(`date`,'%m/%d/%Y') as dateText
 from layoffs_staging2;

update layoffs_staging2 set date=str_to_date(`date`,'%m/%d/%Y');

-- change date column type to date instead of text
ALTER TABLE layoffs_staging2
MODIFY `date` DATE;

select * from layoffs_staging2;

-- handle nun and null values
select * from layoffs_staging2
where total_laid_off IS NULL AND percentage_laid_off IS NULL;

select * from layoffs_staging2 where industry IS NULL OR industry='';

-- that how we can get values for similiar items for example airbnb in one table empty and other travel
select t1.industry,t2.industry 
from layoffs_staging2 t1
join layoffs_staging2 t2
on
t1.company=t2.company
where (t1.industry is null or t1.industry='')
AND t2.industry is not null;

update layoffs_staging2
set industry=null
where industry='';
-- that update is remove null from t1 and replace it with value in t2
update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null 
AND t2.industry is not null;

-- here we deleted data is it's totally empty and we can't trust it
DELETE 
From layoffs_staging2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- now we can remover row_num as we no need to it anymore after we used it for delete duplicates
ALTER TABLE layoffs_staging2
DROP row_num;


