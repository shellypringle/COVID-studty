--the first of these is looking at overall data and 
--beginning to understand which data I want to use for visualizations

SELECT
  continent,
  date,
  total_cases,
  new_cases,
  total_deaths,
  population
FROM  
  `covid-project-360318.covid_deaths.covid_deaths`
WHERE continent is not null
ORDER BY 1,2;

  --looking at total cases vs total deaths
SELECT
  location,
  date,
  total_cases,
  total_deaths,
  round(total_deaths/total_cases*100,2) as DeathPercentage
FROM  
  `covid-project-360318.covid_deaths.covid_deaths`
WHERE location ='United States'
ORDER BY 1,2;

  --looking at total cases vs population
SELECT
  location,
  date,
  total_cases,
  population,
  round(total_cases/population*100,2) as percent_of_pop
FROM  
  `covid-project-360318.covid_deaths.covid_deaths`
WHERE location ='United States'
ORDER BY 1,2;

SELECT
    location,
    MAX(total_deaths) as death_count
    FROM  
  `covid-project-360318.covid_deaths.covid_deaths`
WHERE
    continent is not null
GROUP BY
    location
ORDER BY
    death_count desc;

--looking at deaths per new case

SELECT
    SUM(new_cases) as global_cases,
    SUM(new_deaths) as global_deaths,
    SUM(new_deaths)/SUM(new_cases)*100 as percent_death_by_case
FROM
    `covid-project-360318.covid_deaths.covid_deaths`
WHERE continent is not null
GROUP BY
      date
ORDER BY 
      1,2;


--looking at total population vs vaccination
SELECT 
    d.continent,
    d.location,
    d.date,
    d.population,
    v.new_vaccinations,
    SUM(v.new_vaccinations) OVER (Partition by d.location order by d.location, d.date) as rolling_sum_vaccinations
    from `covid-project-360318.covid_deaths.covid_deaths` d
    
    JOIN
      `covid-project-360318.covid_deaths.covid_vaccs` v
        ON
          d.date = v.date
          and d.location = v.location
WHERE d.continent is not null
ORDER BY 2,3;

  --created CTE

WITH pop_vs_vacc 
AS 
  (
  SELECT 
    d.continent,
    d.location,
    d.date,
    d.population,
    v.new_vaccinations,
    SUM(v.new_vaccinations) OVER (Partition by d.location order by d.location, d.date) as rolling_sum_vaccinations
    FROM `covid-project-360318.covid_deaths.covid_deaths` d
    
    JOIN
      `covid-project-360318.covid_deaths.covid_vaccs` v
        ON
          d.date = v.date
          and d.location = v.location
    WHERE d.continent is not null
    )
SELECT *, rolling_sum_vaccinations/population*100 as percent_pop_vacc from pop_vs_vacc
WHERE location = 'United States';

--created a temp table

DROP TABLE IF EXISTS covid_deaths.my_temp_table;
CREATE TABLE covid_deaths.my_temp_table 
  AS 
  (
  SELECT 
    d.continent,
    d.location,
    d.date,
    d.population,
    v.new_vaccinations,
    SUM(v.new_vaccinations) OVER (Partition by d.location order by d.location, d.date) as rolling_sum_vaccinations
    from `covid-project-360318.covid_deaths.covid_deaths` d
    
    JOIN
      `covid-project-360318.covid_deaths.covid_vaccs` v
        ON
          d.date = v.date
          and d.location = v.location
    WHERE d.continent IS NOT NULL
    );
    
SELECT * from `covid-project-360318.covid_deaths.my_temp_table`
ORDER BY 1,2,3


--creating view 

CREATE VIEW covid_deaths.percent_pop_vacc AS
SELECT 
    d.continent,
    d.location,
    d.date,
    d.population,
    v.new_vaccinations,
    SUM(v.new_vaccinations) OVER (Partition by d.location order by d.location, d.date) as rolling_sum_vaccinations
    from `covid-project-360318.covid_deaths.covid_deaths` d
    
    JOIN
      `covid-project-360318.covid_deaths.covid_vaccs` v
        ON
          d.date = v.date
          and d.location = v.location
    WHERE d.continent IS NOT NULL

    --looking at percentage population death by COVID
  WITH percent_death AS (
    SELECT
 SUM(new_deaths/population)*100 as percent_deaths_by_covid, location
 FROM `covid-project-360318.covid_deaths.covid_deaths`
 WHERE location NOT IN('World','Lower middle income') AND continent IS NOT NULL
 GROUP BY location)
 SELECT location, percent_deaths_by_covid, DENSE_RANK() OVER (ORDER BY percent_death.percent_deaths_by_covid DESC) as rank from percent_death
 WHERE percent_deaths_by_covid IS NOT NULL
 ORDER BY rank

 --SQL used for Tableau
SELECT SUM(new_cases) as total_cases, SUM(new_deaths) as total_deaths, SUM(new_deaths)/SUM(new_cases)*100 AS death_percentage
FROM
  `covid-project-360318.covid_deaths.covid_deaths`
WHERE continent IS NOT NULL
ORDER BY 1,2;

SELECT location, SUM(new_deaths) as total_death_count
FROM `covid-project-360318.covid_deaths.covid_deaths`
WHERE continent IS NULL and location NOT IN ('Low income', "High income", 'World', 'European Union', 'International', "Upper middle income", 'Lower middle income')
GROUP BY location
ORDER BY total_death_count DESC;

SELECT date, location, continent, MAX(total_cases) as highest_infection_count, population 
FROM
`covid-project-360318.covid_deaths.covid_deaths`
GROUP BY date, location, continent, population;

SELECT location, continent, coalesce(SUM(new_deaths),0) as total_death_count, date
FROM `covid-project-360318.covid_deaths.covid_deaths`
WHERE continent IN ("North America", 'Asia', "South America", 'Oceana', "Europe", 'Africa')
GROUP BY location, date, continent
ORDER BY date;

SELECT 
  location,
  continent,
  date,
  coalesce(people_fully_vaccinated,0) people_vacc,
FROM covid_deaths.covid_vaccs
WHERE continent IN ('North America', 'Africa', 'Asia', 'South America', 'Oceana', 'Europe')
ORDER BY 1,2,3


