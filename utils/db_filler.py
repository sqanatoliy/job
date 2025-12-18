import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import random
from decouple import config
from datetime import timedelta


# Заповнення таблиці Companies
def populate_companies(db_config, count=20000):
    fake = Faker()
    
    # Список індустрій для повторення
    industries = ['IT', 'Finance', 'Healthcare', 'Education', 'Manufacturing', 'Retail', 'Energy']
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print(f"Генерація {count} компаній...")
        
        # Готуємо дані у пам'яті
        data = []
        for _ in range(count):
            data.append((
                fake.company(),      # name
                random.choice(industries), # industry
                fake.country()       # country
            ))
        
        # SQL запит (без id та created_at, бо вони генеруються автоматично)
        query = "INSERT INTO companies (name, industry, country) VALUES %s"
        
        print("Запис у базу даних...")
        # execute_values набагато швидше за звичайний execute у циклі
        execute_values(cursor, query, data)
        
        conn.commit()
        print(f"Успішно додано {count} компаній!")
        
    except Exception as e:
        print(f"Помилка: {e}")
        if conn: 
            conn.rollback()
    finally:
        if cursor: 
            cursor.close()
        if conn: 
            conn.close()


# Заповнення таблиці Jobs
def populate_jobs(db_config, total_records=1_000_000, chunk_size=10_000):
    fake = Faker()
    categories = ['Engineering', 'Marketing', 'Sales', 'Design', 'HR', 'Support']
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # 1. Отримуємо існуючі company_id
        print("Отримання ID компаній...")
        cursor.execute("SELECT company_id FROM companies")
        company_ids = [row[0] for row in cursor.fetchall()]
        
        if not company_ids:
            print("Помилка: Таблиця companies порожня!")
            return

        print(f"Починаємо генерацію {total_records} вакансій...")

        inserted = 0
        while inserted < total_records:
            data = []
            for _ in range(min(chunk_size, total_records - inserted)):
                # Логіка для дат
                posted_at = fake.date_time_between(start_date='-1y', end_date='now')
                last_updated = fake.date_time_between(start_date=posted_at, end_date='now')
                
                # Логіка для salary (багато NULL)
                has_salary = random.random() > 0.4  # 40% вакансій будуть без зарплати (NULL)
                salary_from = random.randint(500, 5000) if has_salary else None
                salary_to = (salary_from + random.randint(200, 3000)) if salary_from else None

                data.append((
                    random.choice(company_ids),     # company_id
                    fake.job(),                     # title
                    random.choice(categories),      # category
                    fake.city(),                    # location
                    salary_from,                    # salary_from
                    salary_to,                      # salary_to
                    posted_at,                      # posted_at
                    random.choice([True, False]),   # is_active
                    last_updated                    # last_updated
                ))

            # 2. Масове вставлення пачки
            query = """
                INSERT INTO jobs (
                    company_id, title, category, location, 
                    salary_from, salary_to, posted_at, is_active, last_updated
                ) VALUES %s
            """
            execute_values(cursor, query, data)
            conn.commit() # Фіксуємо кожну пачку
            
            inserted += len(data)
            print(f"Прогрес: {inserted}/{total_records} завантажено...")

        print("Готово! Мільйон вакансій додано.")

    except Exception as e:
        print(f"Сталася помилка: {e}")
        if conn: 
            conn.rollback()
    finally:
        if cursor: 
            cursor.close()
        if conn: 
            conn.close()


# Заповнення таблиці Job_Views
def populate_views(db_config, total_records=50_000_000, chunk_size=50_000):
    """Заповнює таблицю job_views з урахуванням популярності вакансій."""
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # 1. Отримуємо ID вакансій
        print("Отримання ID вакансій...")
        cursor.execute("SELECT job_id, posted_at FROM jobs")
        jobs_data = cursor.fetchall() # Обережно, тут 1 млн рядків
        job_ids = [row[0] for row in jobs_data]
        job_dates = {row[0]: row[1] for row in jobs_data}
        
        # Створюємо "популярні" вакансії (перші 10% списку будуть отримувати більше переглядів)
        popular_jobs = job_ids[:int(len(job_ids) * 0.1)]
        other_jobs = job_ids[int(len(job_ids) * 0.1):]

        print(f"Починаємо генерацію {total_records} переглядів...")

        inserted = 0
        while inserted < total_records:
            data = []
            for _ in range(min(chunk_size, total_records - inserted)):
                # Створюємо перекіс: у 80% випадків обираємо з популярних
                if random.random() < 0.8:
                    j_id = random.choice(popular_jobs)
                else:
                    j_id = random.choice(other_jobs)
                
                # Дата перегляду має бути ПІСЛЯ дати публікації
                start_date = job_dates[j_id]
                viewed_at = start_date + timedelta(seconds=random.randint(0, 1000000))
                
                data.append((
                    j_id,
                    random.randint(1, 500000), # user_id
                    viewed_at
                ))

            # Вставка
            query = "INSERT INTO job_views (job_id, user_id, viewed_at) VALUES %s"
            execute_values(cursor, query, data)
            conn.commit()
            
            inserted += len(data)
            if inserted % 500000 == 0:
                print(f"Прогрес: {inserted}/{total_records} ({(inserted/total_records)*100:.1f}%)")

        print("Готово!")

    except Exception as e:
        print(f"Помилка: {e}")
        if conn: 
            conn.rollback()
    finally:
        if cursor: 
            cursor.close()
        if conn: 
            conn.close()


# Заповнення таблиці Job_Applications
def populate_precise_applications(db_config, target_count=5000000, batch_size=20000):
    statuses = ['applied', 'rejected', 'hired']
    weights = [0.3, 0.65, 0.05] # Конверсія у найм 5%
    
    conn = None
    read_cursor = None
    write_cursor = None

    try:
        conn = psycopg2.connect(**db_config)
        
        # 1. Створюємо серверний курсор для ЧИТАННЯ.
        # name='...' робить його серверним (дані не вантажаться в RAM всі одразу).
        # withhold=True дозволяє робити commit() для вставок, не вбиваючи цей курсор.
        read_cursor = conn.cursor(name='views_reader_cursor', withhold=True)
        
        # 2. Створюємо звичайний курсор для ЗАПИСУ.
        write_cursor = conn.cursor()

        print("Аналіз таблиці job_views та підготовка вибірки...")
        print("Використовуємо метод BERNOULLI для максимальної якості розподілу даних.")
        
        # BERNOULLI(10) сканує всю таблицю і бере ~10% рядків. 
        # Це повільніше за SYSTEM, але дає ідеально репрезентативну вибірку.
        read_cursor.execute("""
            SELECT job_id, user_id, viewed_at 
            FROM job_views 
            TABLESAMPLE BERNOULLI(10)
        """)
        
        inserted_total = 0
        
        while True:
            # Читаємо пачку переглядів
            rows = read_cursor.fetchmany(batch_size)
            
            if not rows:
                break # Дані закінчились
            
            # Формуємо заявки на основі переглядів
            applications_data = []
            for job_id, user_id, viewed_at in rows:
                
                # Логіка: заявка завжди пізніше перегляду (від 5 хв до 5 днів)
                time_delay = timedelta(minutes=random.randint(5, 7200)) 
                applied_at = viewed_at + time_delay
                
                # Визначаємо статус
                status = random.choices(statuses, weights=weights)[0]
                
                applications_data.append((
                    job_id,
                    user_id,
                    applied_at,
                    status
                ))
            
            # Записуємо пачку в базу
            insert_query = """
                INSERT INTO job_applications (job_id, user_id, applied_at, status) 
                VALUES %s
            """
            execute_values(write_cursor, insert_query, applications_data)
            
            # Фіксуємо зміни
            conn.commit()
            
            inserted_total += len(applications_data)
            print(f"Прогрес: {inserted_total} якісних заявок додано...")
            
            # (Опціонально) Якщо ми досягли мети, можна вийти раніше
            if inserted_total >= target_count:
                print("Досягнуто цільової кількості записів.")
                break

        print(f"Успішно завершено! Всього додано: {inserted_total}")

    except Exception as e:
        print(f"Критична помилка: {e}")
        if conn: 
            conn.rollback()
    finally:
        # Закриваємо все коректно
        if read_cursor: 
            read_cursor.close()
        if write_cursor: 
            write_cursor.close()
        if conn: 
            conn.close()

# Заповнення таблиці Job_Status_History з точною історією змін
def populate_dense_history(db_config, batch_size=50000):
    try:
        conn = psycopg2.connect(**db_config)
        read_cursor = conn.cursor(name='jobs_reader_dense', withhold=True)
        write_cursor = conn.cursor()

        print("Генерація щільної історії (Density Mode)...")
        read_cursor.execute("SELECT job_id, posted_at, last_updated, is_active FROM jobs")

        inserted_count = 0
        total_records = 0
        
        while True:
            rows = read_cursor.fetchmany(batch_size)
            if not rows:
                break
            
            history_entries = []
            
            for job_id, posted_at, last_updated, is_active in rows:
                # 1. Початок завжди однаковий
                history_entries.append((job_id, 'active', posted_at))
                
                lifespan = (last_updated - posted_at).total_seconds()
                
                # Якщо вакансія живе менше 1 дня, історію не роздуваємо
                if lifespan < 86400:
                    if not is_active:
                         history_entries.append((job_id, 'closed', last_updated))
                    continue

                # 2. ГЕНЕРАЦІЯ ПРОМІЖНИХ СТАНІВ
                # Спробуємо вставити 1 або 2 цикли "паузи" всередину періоду життя
                # Тобто: Active -> [Paused -> Active] -> [Paused -> Active] -> ...
                
                num_pauses = random.choices([0, 1, 2], weights=[0.1, 0.6, 0.3])[0]
                
                current_time = posted_at
                step = lifespan / (num_pauses * 2 + 2) # Розбиваємо час на рівні шматки
                
                for i in range(num_pauses):
                    # Paused
                    current_time += timedelta(seconds=step)
                    history_entries.append((job_id, 'paused', current_time))
                    
                    # Active (відновлення)
                    current_time += timedelta(seconds=step)
                    history_entries.append((job_id, 'active', current_time))

                # 3. ФІНАЛЬНИЙ СТАН
                if not is_active:
                    # Якщо вакансія закрита, останній запис - closed
                    history_entries.append((job_id, 'closed', last_updated))
                else:
                    # Якщо вакансія досі активна, переконуємося, що останній запис 'active'
                    # (Ми це вже зробили в циклі, але якщо циклів було 0 - то перший запис і є останнім)
                    pass

            # Запис
            execute_values(write_cursor, """
                INSERT INTO job_status_history (job_id, status, changed_at) 
                VALUES %s
            """, history_entries)
            
            conn.commit()
            total_records += len(history_entries)
            inserted_count += len(rows)
            
            if inserted_count % 100000 == 0:
                print(f"Оброблено {inserted_count} вакансій. Згенеровано записів історії: {total_records}")

        print(f"Готово! Всього записів в історії: {total_records}")
        print(f"Середня кількість записів на вакансію: {total_records / inserted_count:.2f}")

    except Exception as e:
        print(f"Помилка: {e}")
        if conn: 
            conn.rollback()
    finally:
        if read_cursor: 
            read_cursor.close()
        if write_cursor: 
            write_cursor.close()
        if conn: 
            conn.close()

# Функція для внесення хаосу в базу даних (Chaos Monkey)
def apply_chaos(db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("Запуск Chaos Monkey... Псуємо дані для навчання 🐒")

        # 1. Створюємо пізні апдейти (last_updated < posted_at)
        print("- Створюємо логічні помилки в датах оновлення...")
        cursor.execute("""
            UPDATE jobs 
            SET last_updated = posted_at - interval '1 day' * (random() * 10 + 1)::int
            WHERE job_id IN (SELECT job_id FROM jobs TABLESAMPLE SYSTEM(1));
        """)

        # 2. Додаємо NULL-и в обов'язкові за логікою, але не за схемою поля
        print("- Видаляємо категорії та локації (створюємо NULL)...")
        cursor.execute("""
            UPDATE jobs 
            SET category = NULL, location = NULL
            WHERE job_id IN (SELECT job_id FROM jobs TABLESAMPLE SYSTEM(0.5));
        """)

        # 3. Створюємо дублікати в job_views
        print("- Генеруємо дублікати в переглядах (це займе трохи часу)...")
        cursor.execute("""
            INSERT INTO job_views (job_id, user_id, viewed_at)
            SELECT job_id, user_id, viewed_at 
            FROM job_views 
            TABLESAMPLE SYSTEM(0.2); 
        """)

        # 4. Створюємо NULL-статуси в заявках
        print("- Створюємо порожні статуси в заявках...")
        cursor.execute("""
            UPDATE job_applications 
            SET status = NULL
            WHERE application_id IN (SELECT application_id FROM job_applications TABLESAMPLE SYSTEM(1));
        """)

        conn.commit()
        print("Готово! Тепер твоя база даних сповнена сюрпризів для дебагу. 🔥")

    except Exception as e:
        print(f"Помилка хаосу: {e}")
        if conn: 
            conn.rollback()
    finally:
        if cursor: 
            cursor.close()
        if conn: 
            conn.close()

# Налаштування підключення (заміни на свої дані)
config = {
    "dbname": config("DB_NAME", default="job_db"),
    "user": config("DB_USER", default="job_user"),
    "password": config("DB_NAME", default="job_password"),
    "host": config("DB_HOST", default="localhost"),
    "port": config("DB_PORT", default="5432"),
}

if __name__ == "__main__":
    # populate_companies(config)
    # populate_jobs(config)
    # populate_views(config)
    # populate_precise_applications(config)
    # populate_dense_history(config)
    apply_chaos(config)