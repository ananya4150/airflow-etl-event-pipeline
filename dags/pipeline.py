from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging


# ------------------------------------------------------------
dag_default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ------------------------------------------------------------
def get_new_messages_and_categorize(ti):

    import logging
    logger = logging.getLogger("airflow.task")

    categorized = {
        "CUST": [],
        "PROD": [],
        "VIEW": []
    }

    invalid_ids = []   

    pg_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="public"
    )

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:

            cursor.execute("""
                SELECT mid, message
                FROM events
                ORDER BY mid
                LIMIT 20
            """)

            rows = cursor.fetchall()

            discarded = 0

            for mid, message in rows:

                # invalid format
                if not message or ":" not in message:
                    discarded += 1
                    invalid_ids.append(mid)
                    logger.warning(f"Invalid format: mid={mid}")
                    continue

                msg_type, _ = message.split(":", 1)
                msg_type = msg_type.strip()

                if msg_type in categorized:
                    categorized[msg_type].append(mid)
                else:
                    discarded += 1
                    invalid_ids.append(mid)
                    logger.warning(f"Invalid type: mid={mid}, type={msg_type}")

            
            if invalid_ids:
                cursor.execute(
                    "DELETE FROM events WHERE mid = ANY(%s)",
                    (invalid_ids,)
                )
                conn.commit()
                logger.info(f"Deleted invalid messages: {invalid_ids}")

    # push to XCom
    ti.xcom_push(
        key="categorized_messages",
        value=categorized
    )

    logger.info(f"Total rows read: {len(rows)}")
    logger.info(f"Discarded: {discarded}")
    logger.info(f"Categorized result: {categorized}")

# ------------------------------------------------------------

# task -2 
def process_customers(ti):
    import logging
    from datetime import date

    logger = logging.getLogger("airflow.task")

    categorized = ti.xcom_pull(
        task_ids="get_new_messages_and_categorize",
        key="categorized_messages"
    )

    customer_ids = categorized.get("CUST", []) if categorized else []

    processed_count = 0
    valid_count = 0

    if not customer_ids:
        logger.info("No customer messages to process.")
        ti.xcom_push(key="customer_processed_count", value=0)
        ti.xcom_push(key="customer_valid_count", value=0)
        return

    pg_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="public"
    )

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT mid, message
                FROM events
                WHERE mid = ANY(%s)
                ORDER BY mid
                """,
                (customer_ids,)
            )

            rows = cursor.fetchall()

            for mid, message in rows:
                processed_count += 1

                if not message or ":" not in message:
                    logger.warning(f"CUST mid={mid} discarded: invalid format")
                    continue

                msg_type, payload = message.split(":", 1)
                msg_type = msg_type.strip()

                if msg_type != "CUST":
                    logger.warning(f"CUST mid={mid} discarded: wrong type {msg_type}")
                    continue

                parts = [p.strip() for p in payload.split(",")]


                username = parts[0] if len(parts) > 0 and parts[0] else None
                email = parts[1] if len(parts) > 1 and parts[1] else None
                join_date = parts[2] if len(parts) > 2 and parts[2] else None

                if not email:
                    logger.warning(f"CUST mid={mid} discarded: missing email")
                    continue

                if not join_date:
                    join_date = date.today()

                try:
                    cursor.execute(
                        """
                        INSERT INTO customers (username, email, joindate)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (username) DO NOTHING
                        """,
                        (username, email, join_date)
                    )
                    valid_count += 1
                    logger.info(f"Inserted customer from mid={mid}: username={username}")

                except Exception as e:
                    logger.error(f"Failed to insert customer mid={mid}: {e}")
                    conn.rollback()
                    continue

            conn.commit()

    ti.xcom_push(key="customer_processed_count", value=processed_count)
    ti.xcom_push(key="customer_valid_count", value=valid_count)

    logger.info(f"Customer messages processed: {processed_count}")
    logger.info(f"Valid customer messages inserted: {valid_count}")

# -- 
# task-3
def process_products(ti):
    import logging

    logger = logging.getLogger("airflow.task")

    categorized = ti.xcom_pull(
        task_ids="get_new_messages_and_categorize",
        key="categorized_messages"
    )

    product_ids = categorized.get("PROD", []) if categorized else []

    processed_count = 0
    valid_count = 0

    if not product_ids:
        logger.info("No product messages to process.")
        ti.xcom_push(key="product_processed_count", value=0)
        ti.xcom_push(key="product_valid_count", value=0)
        return

    pg_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="public"
    )

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT mid, message
                FROM events
                WHERE mid = ANY(%s)
                ORDER BY mid
                """,
                (product_ids,)
            )

            rows = cursor.fetchall()

            for mid, message in rows:
                processed_count += 1

                if not message or ":" not in message:
                    logger.warning(f"PROD mid={mid} discarded: invalid format")
                    continue

                msg_type, payload = message.split(":", 1)
                msg_type = msg_type.strip()

                if msg_type != "PROD":
                    logger.warning(f"PROD mid={mid} discarded: wrong type {msg_type}")
                    continue

          
                product_id = payload[0:15].strip()
                title = payload[15:65].strip()
                stock_raw = payload[65:75].strip()
                price_raw = payload[75:95].strip()

                if not product_id:
                    logger.warning(f"PROD mid={mid} discarded: missing product_id")
                    continue

                if not title:
                    logger.warning(f"PROD mid={mid} discarded: missing title")
                    continue

               
                if not stock_raw:
                    stock = 0
                else:
                    try:
                        stock = int(stock_raw)
                    except ValueError:
                        logger.warning(f"PROD mid={mid} discarded: invalid stock '{stock_raw}'")
                        continue

                try:
                    price = float(price_raw)
                except ValueError:
                    logger.warning(f"PROD mid={mid} discarded: invalid price '{price_raw}'")
                    continue

                try:
                    cursor.execute(
                        """
                        INSERT INTO products (productid, title, stock, price)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (productid) DO NOTHING
                        """,
                        (product_id, title, stock, price)
                    )
                    valid_count += 1
                    logger.info(f"Inserted product from mid={mid}: product_id={product_id}")

                except Exception as e:
                    logger.error(f"Failed to insert product mid={mid}: {e}")
                    conn.rollback()
                    continue

            conn.commit()

    ti.xcom_push(key="product_processed_count", value=processed_count)
    ti.xcom_push(key="product_valid_count", value=valid_count)

    logger.info(f"Product messages processed: {processed_count}")
    logger.info(f"Valid pxroduct messages inserted: {valid_count}")

#--
# task4

def process_reviews(ti):
    import logging
    import json

    logger = logging.getLogger("airflow.task")

    categorized = ti.xcom_pull(
        task_ids="get_new_messages_and_categorize",
        key="categorized_messages"
    )

    review_ids = categorized.get("VIEW", []) if categorized else []

    processed_count = 0
    valid_count = 0

    if not review_ids:
        logger.info("No review messages to process.")
        ti.xcom_push(key="review_processed_count", value=0)
        ti.xcom_push(key="review_valid_count", value=0)
        return

    pg_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="public"
    )

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                SELECT mid, message
                FROM events
                WHERE mid = ANY(%s)
                ORDER BY mid
                """,
                (review_ids,)
            )
            rows = cursor.fetchall()

            for mid, message in rows:
                processed_count += 1

                if not message or ":" not in message:
                    logger.warning(f"VIEW mid={mid} discarded: invalid format")
                    continue

                msg_type, payload = message.split(":", 1)
                msg_type = msg_type.strip()

                if msg_type != "VIEW":
                    logger.warning(f"VIEW mid={mid} discarded: wrong type {msg_type}")
                    continue

                try:
                    review_json = json.loads(payload)
                except Exception as e:
                    logger.warning(f"VIEW mid={mid} discarded: invalid JSON ({e})")
                    continue

                username = review_json.get("username")
                product_id = review_json.get("product")
                review_title = review_json.get("title")
                review_date = review_json.get("datetime")
                review_content = review_json.get("comment")

                if not username or not product_id:
                    logger.warning(f"VIEW mid={mid} discarded: missing username or product")
                    continue

               
                cursor.execute(
                    """
                    SELECT username, email, joindate
                    FROM customers
                    WHERE username = %s
                    """,
                    (username,)
                )
                customer_row = cursor.fetchone()

                if not customer_row:
                    logger.warning(f"VIEW mid={mid} discarded: customer '{username}' not found")
                    continue


                cursor.execute(
                    """
                    SELECT productid, title, stock, price
                    FROM products
                    WHERE productid = %s
                    """,
                    (product_id,)
                )
                product_row = cursor.fetchone()

                if not product_row:
                    logger.warning(f"VIEW mid={mid} discarded: product '{product_id}' not found")
                    continue

                _, email, join_date = customer_row
                _, product_title, stock, price = product_row

                try:
                    cursor.execute(
                        """
                        INSERT INTO reviews
                        (
                            username,
                            productid,
                            email,
                            joindate,
                            producttitle,
                            stock,
                            price,
                            reviewtitle,
                            reviewdate,
                            reviewcontent
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            username,
                            product_id,
                            email,
                            join_date,
                            product_title,
                            stock,
                            price,
                            review_title,
                            review_date,
                            review_content
                        )
                    )
                    valid_count += 1
                    logger.info(
                        f"Inserted review from mid={mid}: username={username}, productid={product_id}"
                    )

                except Exception as e:
                    logger.error(f"Failed to insert review mid={mid}: {e}")
                    conn.rollback()
                    continue

            conn.commit()

    ti.xcom_push(key="review_processed_count", value=processed_count)
    ti.xcom_push(key="review_valid_count", value=valid_count)

    logger.info(f"Review messages processed: {processed_count}")
    logger.info(f"Valid review messages inserted: {valid_count}")
#--
# task-5
def cleanup_events(ti):
    import logging

    logger = logging.getLogger("airflow.task")

    categorized = ti.xcom_pull(
        task_ids="get_new_messages_and_categorize",
        key="categorized_messages"
    ) or {}

    customer_processed = ti.xcom_pull(
        task_ids="process_customers",
        key="customer_processed_count"
    ) or 0
    customer_valid = ti.xcom_pull(
        task_ids="process_customers",
        key="customer_valid_count"
    ) or 0

    product_processed = ti.xcom_pull(
        task_ids="process_products",
        key="product_processed_count"
    ) or 0
    product_valid = ti.xcom_pull(
        task_ids="process_products",
        key="product_valid_count"
    ) or 0

    review_processed = ti.xcom_pull(
        task_ids="process_reviews",
        key="review_processed_count"
    ) or 0
    review_valid = ti.xcom_pull(
        task_ids="process_reviews",
        key="review_valid_count"
    ) or 0

    all_ids = []
    all_ids.extend(categorized.get("CUST", []))
    all_ids.extend(categorized.get("PROD", []))
    all_ids.extend(categorized.get("VIEW", []))

    # remove duplicates while preserving order
    seen = set()
    processed_ids = []
    for mid in all_ids:
        if mid not in seen:
            seen.add(mid)
            processed_ids.append(mid)

    if not processed_ids:
        logger.info("No processed event ids to delete.")
        return

    pg_hook = PostgresHook(
        postgres_conn_id="postgres_conn",
        schema="public"
    )

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM events WHERE mid = ANY(%s)",
                (processed_ids,)
            )
            deleted_rows = cursor.rowcount
            conn.commit()

    customer_invalid = customer_processed - customer_valid
    product_invalid = product_processed - product_valid
    review_invalid = review_processed - review_valid


    logger.info(f"Processed event ids: {processed_ids}")
    logger.info(f"Deleted rows from events: {deleted_rows}")
    logger.info(
        f"Customers -> processed: {customer_processed}, valid: {customer_valid}, invalid: {customer_invalid}"
    )
    logger.info(
        f"Products  -> processed: {product_processed}, valid: {product_valid}, invalid: {product_invalid}"
    )
    logger.info(
        f"Reviews   -> processed: {review_processed}, valid: {review_valid}, invalid: {review_invalid}"
    )
#--
with DAG(
    dag_id="pipeline_dag",
    default_args=dag_default_args,
    description="Pipeline Tasks 1 and 2",
    schedule=timedelta(minutes=10),
    catchup=False,
    start_date=datetime(2026, 3, 1),
    tags=["pipeline"],
) as dag:

    task1 = PythonOperator(
        task_id="get_new_messages_and_categorize",
        python_callable=get_new_messages_and_categorize,
    )

    task2 = PythonOperator(
        task_id="process_customers",
        python_callable=process_customers,
    )

    task3 = PythonOperator(
    task_id="process_products",
    python_callable=process_products,
)

    task4 = PythonOperator(
    task_id="process_reviews",
    python_callable=process_reviews,
)

task5 = PythonOperator(
    task_id="cleanup_events",
    python_callable=cleanup_events,
)

task1 >> [task2, task3, task4] >> task5