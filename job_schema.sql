
-- Мета курсу Навчитися працювати з SQL на рівні, достатньому для роботи з аналітичними базами даних у реальних проєктах.

-- Навчитися:
-- 	писати складні SELECT-и
-- 	оптимізувати запити
-- 	працювати з аналітичними сценаріями
-- 	дебажити повільні запити
-- 	мислити як data engineer, а не як SQL-solver

---------------------------------------------------------------
-- Початкові налаштування
----------------------------------------------------------------

-- Налаштування середовища для безпеки та передбачуваності, щоб ваш код працював однаково на різних компютерах
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

---------------------------------------------------------------
-- Схема бази даних для job portal
----------------------------------------------------------------

-- 1 companies
-- Навіщо
-- Довідник компаній. Dimension table.
-- Список індустрій для повторення
-- ['IT', 'Finance', 'Healthcare', 'Education', 'Manufacturing', 'Retail', 'Energy']
CREATE TABLE companies (
    company_id      BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    industry        TEXT,
    country         TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Дані
-- 	5 000 to 20 000 компаній
-- 	різні країни
-- 	повторювані індустрії

----------------------------------------------------------------
-- Схема бази даних  вакансій
---------------------------------------------------------------

-- Second jobs
-- Навіщо
-- Центральна таблиця вакансій. Dimension + operational.
-- Список категорій для повторення
-- ['Engineering', 'Marketing', 'Sales', 'Design', 'HR', 'Support']

CREATE TABLE jobs (
    job_id          BIGSERIAL PRIMARY KEY,
    company_id      BIGINT NOT NULL REFERENCES companies(company_id),
    title           TEXT NOT NULL,
    category        TEXT,
    location        TEXT,
    salary_from     INT,
    salary_to       INT,
    posted_at       TIMESTAMPTZ NOT NULL,
    is_active       BOOLEAN NOT NULL,
    last_updated    TIMESTAMPTZ NOT NULL
);

-- Дані
-- 	up to 1 000 000 вакансій
-- 	багато NULL у salary
-- 	часті оновлення last_updated
-- 	частина is_active = false

----------------------------------------------------------------
-- Схема бази даних  взаємодій з вакансіями
---------------------------------------------------------------

-- third job_views (FACT)
-- Навіщо
-- Аналітика + performance + window functions.

CREATE TABLE job_views (
    view_id     BIGSERIAL PRIMARY KEY,
    job_id      BIGINT NOT NULL REFERENCES jobs(job_id),
    user_id     BIGINT,
    viewed_at   TIMESTAMPTZ NOT NULL
);

-- Дані
-- 	up to 50 млн рядків
-- 	skewed distribution (топ вакансії)
-- 	багато повторних переглядів

---------------------------------------------------------------
-- Схема бази даних  заявок на вакансії
---------------------------------------------------------------

-- fourth job_applications (FACT)

-- Навіщо
-- Бізнес-метрики, конверсії.

CREATE TABLE job_applications (
    application_id BIGSERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL REFERENCES jobs(job_id),
    user_id         BIGINT,
    applied_at      TIMESTAMPTZ NOT NULL,
    status          TEXT
);

-- Дані
-- 	1 to 5 млн рядків
-- 	status: applied / rejected / hired
-- 	applied << views

---------------------------------------------------------------
-- Схема бази даних  історії статусів вакансій
---------------------------------------------------------------

-- fifth job_status_history (SCD / історія)
-- Навіщо
-- Incremental loads, window functions, останній стан.

CREATE TABLE job_status_history (
    job_id      BIGINT NOT NULL,
    status      TEXT NOT NULL,
    changed_at  TIMESTAMPTZ NOT NULL
);

-- Дані
-- 	2 to 3 записи на job
-- 	active  paused  closed
-- 	late arriving data

---------------------------------------------------------------