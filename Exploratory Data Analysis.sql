-- Portfolio Project EDA
-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!
-- ****************************************************************************************************************
select * from layoff_staging2;

-- EASIER QUERIES

SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;


-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;


-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;
-- these are mostly startups it looks like who all went out of business during this time


-- if we order by funcs_raised_millions we can see how big some of these companies were
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;



-- ****************************************************************************************************************
-- Intermediate
-- Total number of total_laid_off accaurdance to company
select company, sum(total_laid_off), avg(percentage_laid_off) from layoff_staging2
group by company
order by 2 desc;

-- Total number of total_laid_of in reference to industry
select industry, sum(total_laid_off), avg(percentage_laid_off) from layoff_staging2
group by industry
order by 2 desc;

-- Total number of total_laid_of in reference to country
select country, sum(total_laid_off), avg(percentage_laid_off) from layoff_staging2
group by country
order by 2 desc;

-- Total number of total_laid_of in each year
select year(date), sum(total_laid_off) from layoff_staging2
group by 1
order by 1 desc;


select date, sum(total_laid_off) from layoff_staging2
group by 1
order by 1;


-- ****************************************************************************************************************
-- Advanced
-- Find the rolling layoff of all the years month wise
-- Using CTE subquries
with ann as
(select substring(date,1,7) as 'month', sum(total_laid_off) as total_layoff from layoff_staging2
where substring(date,1,7) is not null
group by substring(date,1,7)
order by 1)
select month,total_layoff,sum(total_layoff) over(order by month) as rolling_layoff from ann ;


-- Find the rolling layoff of all the years month wise
-- Using normal subquries
select month, month_wise_layoff,sum(month_wise_layoff) over(order by month) as rolling_layoff from
(select substring(date,1,7) as 'month', sum(total_laid_off) as month_wise_layoff from layoff_staging2
where substring(date,1,7) is not null
group by 1
order by 1) as lilu;


-- Find the rolling layoff of all the years in dialy wise
select date, total_layoff, sum(total_layoff) over(order by date) as rolling_layoff from 
(select date, sum(total_laid_off) as total_layoff from layoff_staging2
where date is not null
group by date
order by 1) as lilu;

-- Find 1st five company which did most layoff in each year
-- Using CTE
with lilu as
(select company,year(date) as years, sum(total_laid_off) as total_layoff from layoff_staging2
group by company, year(date)
),company_year_rank as
(select *,dense_rank() over(partition by years order by total_layoff desc) as ranking from lilu
where years is not null)
select * from company_year_rank
where ranking<=5;


-- Find 1st five company which did most layoff in each year
-- Using normal subquries
select * from 
(select company, years,total_layoff, dense_rank() over(partition by years order by total_layoff desc) as ranking from
(select company, year(date) as years, sum(total_laid_off) as total_layoff from layoff_staging2
group by company, year(date)) as lilu
where years is not null) as lilu2
where ranking<=5;

