import logging
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error as MySQLError
import requests

def get_database_connection():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host='host',
            port='port',
            user='user',
            password="password"
        )
        logging.info('Database connected successfully.')
        return connection
    except mysql.connector.Error as error:
        logging.error("Error connecting to MySQL database: %s", error)
        raise

def truncate(siteid):
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        truncate_queries = [
            f"DELETE FROM pre_stage.site_archive_post_v2 WHERE siteid = {siteid};",
            f"DELETE FROM stage.site_archive_post_v2 WHERE siteid = {siteid};",
            f"DELETE FROM prod.link_updates WHERE siteid = {siteid};"
        ]
        for query in truncate_queries:
            cursor.execute(query)
        connection.commit()
        logging.info("Data truncated for siteid: %s", siteid)
    except Exception as e:
        logging.error("Error during truncation: %s", e)
        raise

def insertion_into_pre_stage(cursor, connection, site_id, base_url):
    start_date = datetime.today().date()
    end_date = start_date - timedelta(days=1)

    cms_api_url = f"{base_url}?fromdate={end_date}&todate={start_date}"
    data_list = []

    try:
        response = requests.get(cms_api_url)
        if response.status_code == 200:
            daily_data = response.json()
            logging.info("Data fetched successfully from API. Total records: %d", len(daily_data))
        else:
            logging.error("Failed to fetch articles: %s, %s", response.status_code, response.text)
            return

        for article in daily_data:
            postid = article.get("id", None)
            title = article.get('title', None)
            publisheddate = article.get('publishdate', None)
            updatedate = article.get('updatedate', None)
            url = article.get('url', None)
            status = "published"
            categories = article.get('tagTargetGroup', None)
            tags = article.get('tagEvergreen', None)

            data_list.append((postid, title, publisheddate, url, categories, tags, status, updatedate, site_id))

        logging.info("Processed %d records for insertion into pre_stage.", len(data_list))

        insert_query = """
            INSERT IGNORE INTO pre_stage.site_archive_post_v2 (ID, Title, Date, Link, Categories, Tags, Status, Modified, siteid)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, data_list)
        connection.commit()
        logging.info("Data insertion into pre_stage completed.")

    except Exception as e:
        logging.error("Error during data insertion into pre_stage: %s", e)
        raise

def insert_into_prod(siteid):
    start_date = datetime.today().strftime('%Y-%m-%d')
    connection = get_database_connection()
    cursor = connection.cursor()

    queries = {
        "insert_into_historic": f"""
            INSERT INTO prod.site_archive_post_historic (ID, Title, Date, Link, categories, Tags, userneeds, Status, Modified, siteid, inserted_at)
            SELECT d.ID, d.Title, d.Date, d.Link, d.Categories, d.Tags, d.userneeds, d.Status, CAST(d.Modified AS date), d.siteid, '{start_date}'
            FROM pre_stage.site_archive_post_v2 s
            LEFT JOIN prod.site_archive_post d
                ON s.siteid = d.siteid AND s.ID = d.ID 
            WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid} 
            AND (s.Link != d.Link OR s.Date != d.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """,

        "insert_link_updates_query": f"""
            INSERT INTO prod.link_updates (postid, siteid, date, old_link, new_link)
            SELECT s.ID AS postid, s.siteid, s.date, d.Link AS old_link, s.Link AS new_link
            FROM pre_stage.site_archive_post_v2 s
            LEFT JOIN prod.site_archive_post_historic d
                ON s.siteid = d.siteid AND s.ID = d.ID AND s.Link != d.Link
            WHERE s.siteid = {siteid} AND d.Link IS NOT NULL AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """,

        "delete_old_records": f"""
            WITH IDS AS (
                SELECT d.ID
                FROM prod.site_archive_post d
                LEFT JOIN pre_stage.site_archive_post_v2 s
                    ON s.siteid = d.siteid AND s.ID = d.ID
                WHERE d.ID IS NOT NULL AND d.Date IS NOT NULL AND s.siteid = {siteid}
                AND (d.Link != s.Link OR d.Date != s.Date) AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            )
            DELETE FROM prod.site_archive_post d WHERE d.ID IN (SELECT ID FROM IDS) AND d.siteid = {siteid};
        """,

        "insert_query": f"""
            INSERT INTO prod.site_archive_post (ID, Title, Date, Link, categories, Tags, Status, Modified, siteid)
            SELECT s.ID, s.Title, s.Date, s.Link, s.Categories, s.Tags, s.Status, CAST(s.Modified AS date), s.siteid 
            FROM pre_stage.site_archive_post_v2 s
            LEFT JOIN prod.site_archive_post d ON s.siteid = d.siteid AND s.ID = d.ID 
            WHERE d.ID IS NULL AND d.Date IS NULL AND s.siteid = {siteid} AND s.date = DATE_SUB(CURDATE(), INTERVAL 1 DAY);
        """,

        "update_events": f"""
            UPDATE prod.events e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.event_name = lu.old_link
                AND lu.postid = e.postid
            SET e.event_name = lu.new_link
            WHERE e.siteid = lu.siteid AND e.event_name = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """,

        "update_pages": f"""
            UPDATE prod.pages e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.URL = lu.old_link
                AND lu.postid = e.postid
            SET e.URL = lu.new_link
            WHERE e.siteid = lu.siteid AND e.URL = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """,

        "update_traffic_channel": f"""
            UPDATE prod.traffic_channels e
            JOIN prod.link_updates lu
                ON e.siteid = lu.siteid
                AND e.firsturl = lu.old_link
                AND lu.postid = e.postid
            SET e.firsturl = lu.new_link
            WHERE e.siteid = lu.siteid AND e.firsturl = lu.old_link AND lu.postid = e.postid AND e.siteid = {siteid};
        """
    }

    try:
        for query_name, query in queries.items():
            cursor.execute(query)
            logging.info("Executed query: %s", query_name)
        connection.commit()
        logging.info("Data insertion into prod completed successfully.")
    except MySQLError as e:
        logging.error("Error inserting data into prod MySQL database: %s", e)
        connection.rollback()
        raise

def start_execution():
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        site_id = 15
        base_url = "https://api_url"

        logging.info("Starting truncation for siteid: %s", site_id)
        truncate(site_id)

        logging.info("Starting insertion into pre_stage for siteid: %s", site_id)
        insertion_into_pre_stage(cursor, connection, site_id, base_url)

        logging.info("Starting insertion into prod for siteid: %s", site_id)
        insert_into_prod(site_id)

        logging.info("ETL process completed successfully.")
    except Exception as e:
        logging.error("Error during ETL process: %s", e)
        raise

def dev_etl():
    start_execution()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    dev_etl()
