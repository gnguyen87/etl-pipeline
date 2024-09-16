from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_education_data(resource, file_path):
    pg_hook = PostgresHook(postgres_conn_id='postgres', database = 'raw' )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(f"truncate table raw_nn.{resource};")
    with open(file_path, "r") as f:
        cursor.copy_expert(f"COPY raw_nn.{resource} (v) from STDIN", f)
    pg_conn.commit()
    
