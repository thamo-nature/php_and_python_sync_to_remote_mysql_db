import asyncio
import aiomysql
import logging
import time

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters
local_db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'smartivf',
    'charset': 'utf8mb4',
}

remote_db_config = {
    'host': '',
    'user': '',
    'password': '',
    'db': '',
    'charset': 'utf8mb4',
}

batch_size = 1000

async def create_table_if_not_exists(remote_conn, table_name, create_table_sql):
    async with remote_conn.cursor() as cursor:
        await cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = await cursor.fetchone()
        if not result:
            logging.info(f"Creating table {table_name} on remote database")
            await cursor.execute(create_table_sql)
        await remote_conn.commit()

async def insert_rows_in_batches(remote_conn, table_name, rows, column_names, placeholders, update_list):
    async with remote_conn.cursor() as cursor:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            values = [tuple(row[col] for col in column_names) for row in batch]
            await cursor.executemany(
                f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {update_list}",
                values
            )
        await remote_conn.commit()

async def sync_table(table_name, local_db_config, remote_db_config):
    local_conn = await aiomysql.connect(**local_db_config)
    remote_conn = await aiomysql.connect(**remote_db_config)

    try:
        async with local_conn.cursor(aiomysql.DictCursor) as local_cursor:
            # Describe table to get column details
            await local_cursor.execute(f"DESCRIBE {table_name}")
            columns = await local_cursor.fetchall()

            column_names = [col['Field'] for col in columns]
            placeholders = ', '.join(['%s'] * len(column_names))
            update_list = ', '.join([f"{col} = VALUES({col})" for col in column_names if col != 'time_stamp'])

            # Check for last sync time
            async with remote_conn.cursor(aiomysql.DictCursor) as remote_cursor:
                await remote_cursor.execute("SELECT last_sync FROM sync_status WHERE table_name = %s", (table_name,))
                last_sync = await remote_cursor.fetchone()

                if not last_sync:
                    # Full sync
                    logging.info(f"Performing full sync for table: {table_name}")
                    await local_cursor.execute(f"SELECT * FROM {table_name}")
                    rows = await local_cursor.fetchall()

                    # Insert or update rows in remote database in batches
                    await insert_rows_in_batches(remote_conn, table_name, rows, column_names, placeholders, update_list)

                    # Update sync status
                    await local_cursor.execute(f"SELECT MAX(time_stamp) as max_ts FROM {table_name}")
                    max_timestamp = (await local_cursor.fetchone())['max_ts']
                    await remote_cursor.execute(
                        "INSERT INTO sync_status (table_name, last_sync) VALUES (%s, %s) ON DUPLICATE KEY UPDATE last_sync = %s",
                        (table_name, max_timestamp, max_timestamp)
                    )
                    await remote_conn.commit()

                    logging.info(f"Full sync completed for table: {table_name}")

                else:
                    # Incremental sync
                    logging.info(f"Performing incremental sync for table: {table_name}")
                    last_sync_time = last_sync['last_sync']

                    await local_cursor.execute(
                        f"SELECT * FROM {table_name} WHERE time_stamp > %s",
                        (last_sync_time,)
                    )
                    rows = await local_cursor.fetchall()

                    # Insert or update rows in remote database in batches
                    await insert_rows_in_batches(remote_conn, table_name, rows, column_names, placeholders, update_list)

                    # Update sync status
                    await local_cursor.execute(
                        f"SELECT MAX(time_stamp) as max_ts FROM {table_name} WHERE time_stamp > %s",
                        (last_sync_time,)
                    )
                    max_timestamp = (await local_cursor.fetchone())['max_ts']
                    await remote_cursor.execute(
                        "UPDATE sync_status SET last_sync = %s WHERE table_name = %s",
                        (max_timestamp, table_name)
                    )
                    await remote_conn.commit()

                    logging.info(f"Incremental sync completed for table: {table_name}")

    finally:
        local_conn.close()
        remote_conn.close()

async def main():
    local_conn = await aiomysql.connect(**local_db_config)
    remote_conn = await aiomysql.connect(**remote_db_config)

    try:
        async with local_conn.cursor(aiomysql.DictCursor) as cursor:
            # Create sync_status table if not exists
            async with remote_conn.cursor() as remote_cursor:
                await remote_cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sync_status (
                        table_name VARCHAR(255) PRIMARY KEY,
                        last_sync TIMESTAMP NULL
                    )
                    """
                )
                await remote_conn.commit()

            # Get list of tables
            await cursor.execute('SHOW TABLES')
            tables = [table['Tables_in_smartivf'] for table in await cursor.fetchall()]

        # First, create all tables on the remote database
        for table in tables:
            async with local_conn.cursor(aiomysql.DictCursor) as cursor:
                await cursor.execute(f"SHOW CREATE TABLE {table}")
                create_table_sql = (await cursor.fetchone())['Create Table']
                await create_table_if_not_exists(remote_conn, table, create_table_sql)

        # Then, sync the tables in parallel, each with its own connection
        tasks = [sync_table(table, local_db_config, remote_db_config) for table in tables]
        await asyncio.gather(*tasks)

    finally:
        local_conn.close()
        remote_conn.close()

if __name__ == "__main__":
    start_time = time.time()
    print("Starting the main process...")
    
    # Main process
    asyncio.run(main())
    
    # End the timer
    end_time = time.time()
    
    total_seconds = end_time - start_time
    total_minutes = total_seconds / 60

    print(f"--- {total_seconds:.2f} Total Execution Seconds ---")
    print(f"--- {total_minutes:.2f} Total Execution Minutes ---")
