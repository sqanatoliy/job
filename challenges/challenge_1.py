"""
SQL Challenge 1 — Health Check даних
Завдання: знайти всі вакансії з логічно некоректними датами.
Очікується:
- виявлення last_updated < posted_at
- підрахунок кількості проблемних записів
- вивід прикладів для дебагу
"""

from typing import Dict, List, Tuple
import logging

from db_connection import get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger("data_health_checks")


def check_invalid_job_dates(conn) -> Dict[str, object]:
    """
    Data Quality Check:
    last_updated must be >= posted_at
    """

    summary_query = """
        SELECT COUNT(*) 
        FROM jobs
        WHERE last_updated < posted_at;
    """

    sample_query = """
        SELECT job_id, posted_at, last_updated
        FROM jobs
        WHERE last_updated < posted_at
        ORDER BY last_updated
        LIMIT 5;
    """

    with conn.cursor() as cursor:
        # 1. Метрика
        cursor.execute(summary_query)
        problem_count = cursor.fetchone()[0]

        # 2. Приклади для дебагу
        cursor.execute(sample_query)
        samples: List[Tuple] = cursor.fetchall()

    return {
        "check_name": "invalid_job_dates",
        "problem_count": problem_count,
        "samples": samples,
    }


def run_health_check_invalid_job_dates():
    """
    Запуск health check та вивід результату
    """
    logger.info("Starting data quality check: invalid_job_dates")

    try:
        with get_db_connection() as conn:
            result = check_invalid_job_dates(conn)

        logger.info("[DATA QUALITY CHECK]")
        logger.info(f"Check name: {result['check_name']}")
        logger.info(f"Problematic rows: {result['problem_count']}")

        if result["problem_count"] > 0:
            logger.info("Sample invalid records:")
            for row in result["samples"]:
                job_id, posted_at, last_updated = row
                logger.info(
                    f"- job_id={job_id}, "
                    f"posted_at={posted_at}, "
                    f"last_updated={last_updated}"
                )
        else:
            logger.info("No issues detected ✅")

    except Exception as e:
        logger.exception(f"[ERROR] Health check failed: {e}")


if __name__ == "__main__":
    run_health_check_invalid_job_dates()