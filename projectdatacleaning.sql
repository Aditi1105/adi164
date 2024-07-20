select * from layoffs;
-- 1. remove duplicates
-- 2. standardised the data
-- 3. null values or blank values
-- 4. remove any columns

-- 1. create a duplicate data so that we have the orginal/raw data set unchanged.
create table layoffs_staging
like layoffs;
select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;

-- 2. identify duplicates

select *,
row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off) as row_num
from layoffs_staging;

with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;


select * from layoffs_staging
where company = 'Casper';

-- remove duplicates
with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num > 1;



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


select * from layoffs_staging2
where row_num >1 ;


insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs_staging;


delete  
from layoffs_staging2
where row_num > 1 ;

select * from layoffs_staging2
where row_num > 1 ;

select * from layoffs_staging2
where company =  'Ola';

-- standardizing the Data--fixing issues with data

select distinct(company)
from layoffs_staging2;

select distinct(trim(company))
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select  *
from layoffs_staging2
where industry like 'Crypto%';


select distinct(industry)
from layoffs_staging2
order by 1;

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct(country)
from layoffs_staging2
order by 1;



select *
from  layoffs_staging2
where country like 'United States%';

select distinct(country), trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';


-- change date from text to date format

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');
select *
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

-- delete null values
select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2 as st1
join layoffs_staging2 as st2
     on st1.company=st2.company
     and st1.location=st2.location
where (st1.industry is null or st1.industry = '')
and st2.industry is not null;

update layoffs_staging2 
set industry = null
where industry = '';

update layoffs_staging2 as st1
join layoffs_staging2 as st2
     on st1.company=st2.company
     and st1.location=st2.location
set st1.industry=st2.industry     
where st1.industry is null
and st2.industry is not null;

select *
from layoffs_staging2
where company = 'Airbnb';

select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

