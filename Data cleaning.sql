-- DATA CLEANING 
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022
-- We can download the dataset from this link or I'm attaching the csv file with this sql project

select * from layoffs;
-- We will use following steps to clean the dataset
-- 1. REMOVE DUPLICATES
-- 2. STANDARDIZE THE DATA
-- 3. NULL VALUES OR BLANK VALUES
-- 4. REMOVE ANY COLUMNS OR ROWS

-- 1st thing we need to create a duplicate table like layoffs for the reference or backup
--  Using like we will create a blank table having same column name as layoff
create table layoff_staging
like layoffs;

-- After creating the blank table we need to insert all the same data to the table
select * from layoff_staging;
insert into layoff_staging
select * from layoffs;

-- ****************************************************************************************************************
-- 1. REMOVE DUPLICATES
-- To find the duplicate we need to use row_number function partition by all the column
select * from 
(select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date',stage,country,funds_raised_millions) as row_num
 from layoff_staging) as find_duplicate
 where row_num>1;
 
 -- TO check that there are many duplicates 
 select * from layoff_staging where company='Zymergen';
 
 -- As we cannot delete and update the entries from windows function(row_num)
 -- so we need to create another table having real column row_num
 CREATE TABLE `layoff_staging2` (
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

-- After that we will insert all data including row_num in this blank table layoff_staging2
 insert into layoff_staging2
 select *,
row_number() over(partition by company, 
							   location, 
                               industry, 
                               total_laid_off, 
                               percentage_laid_off, 
                               'date',
                                stage,
								country,
								funds_raised_millions) as row_num
 from layoff_staging;
 
-- Then we will delete all the duplicate data using row_num column
 delete from layoff_staging2
 where row_num>1;
 
 
 -- ****************************************************************************************************************
 -- 2. Standardize Data
 -- Will update the company column as it has some unnecessary space are there
 select distinct company, trim(company) from layoff_staging2;
 update layoff_staging2
 set company=trim(company);

-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
 select distinct industry from layoff_staging2 order by 1;
 select distinct industry from layoff_staging2 where industry like 'Crypto%';
 update layoff_staging2
 set industry = 'Crypto'
 where industry like 'Crypto%';
 
-- Also need to do some updation in country column as it has some duplicates
select distinct country from layoff_staging2 where country like 'United States%';
select distinct country,trim(trailing '.' from country) from layoff_staging2;
update layoff_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';


-- Also need to do some updation in date column as it is in the form of text and also has double sign like '/' and '-'
select date,str_to_date(replace(date,'-','/'),'%m/%d/%Y') from layoff_staging2;
update layoff_staging2
set date = str_to_date(replace(date,'-','/'),'%m/%d/%Y');
select * from layoff_staging2;

-- Convert date comlun from text to date
alter table layoff_staging2
modify column date date;

select * from layoff_staging2
where company= 'Airbnb';


-- ****************************************************************************************************************
-- 3. Deal with null value or blank values
-- There are some company has not mentioned industry
-- So 1st we will replace all blank value by null in industry column
update layoff_staging2
set industry = null
where industry = '';


-- Let us check how many companies are there where the industry has not mentioned
select t1.company,t1.industry,t2.industry from layoff_staging2 t1
join layoff_staging2 t2 
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


-- So we will update the industry using following query
update layoff_staging2 t1
join layoff_staging2 t2 on t1.company = t2.company
set t1.industry = t2.industry
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;


select * from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;


-- ****************************************************************************************************************
-- 4. Will drop all the unnecessery columns and rows
alter table layoff_staging2
drop column row_num;
select date, total_laid_off from layoff_staging2 order by 1;


-- We can delete the rows where the data of total_laid_off and percentage_laid_off is null
-- It beacuse it may happened that these company has not lay off any employee
delete from layoff_staging2
where total_laid_off is null
and percentage_laid_off is null;