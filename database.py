import mysql.connector

def create_database():
    
    database = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = ""
    )
    cursor = database.cursor()
    cursor.execute("DROP DATABASE IF EXISTS me_database")
    cursor.execute("CREATE DATABASE IF NOT EXISTS me_database") 
    cursor.execute("USE me_database")
    
    create_tables(cursor)
    
    cursor.close()

def create_tables(cursor):
    
    cursor.execute('''
                    CREATE TABLE IF NOT EXISTS request (
                       req_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                       method VARCHAR(100), 
                       uri VARCHAR(100), 
                       url VARCHAR(100), 
                       size INT,
                       querystring VARCHAR(100), 
                       headers_accept VARCHAR(100), 
                       headers_host VARCHAR(100), 
                       headers_user_agent VARCHAR(100),
                       consumer_id_uuid VARCHAR(100),
                       service_id VARCHAR(100)
                    );
                    ''')
    
    cursor.execute('''
                    CREATE TABLE IF NOT EXISTS response (
                       req_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                       status SMALLINT,
                       size SMALLINT,
                       headers_content_length SMALLINT,
                       headers_via VARCHAR(30),
                       headers_connection VARCHAR(10),
                       headers_access_control_allow_credentials VARCHAR(10),
                       headers_content_type VARCHAR(20),
                       headers_server VARCHAR(10),
                       headers_access_control_allow_origin VARCHAR(10)
                    );
                    ''')
    
    cursor.execute('''
                    CREATE TABLE IF NOT EXISTS route (
                       created_at BIGINT,
                       host VARCHAR(30),
                       id VARCHAR(100),
                       methods VARCHAR(100),
                       paths VARCHAR(20),
                       preserve_host BOOLEAN,
                       protocols VARCHAR(30),
                       regex_priority INT,
                       service_id VARCHAR(100),
                       strip_path BOOLEAN,
                       updated_at INT 
                    );
                    ''')
    
    cursor.execute('''
                    CREATE TABLE IF NOT EXISTS service (
                       connect_timeout INT,
                       created_at BIGINT,
                       host VARCHAR(30),
                       id VARCHAR(100),
                       name VARCHAR(30),
                       path VARCHAR(10),
                       port INT,
                       protocol VARCHAR(10),
                       read_timeout INT,
                       retries SMALLINT,
                       updated_at INT,
                       write_timeout INT
                    );
                    ''')
    
    cursor.execute('''
                    CREATE TABLE IF NOT EXISTS latencies (
                       service_id VARCHAR(50),
                       proxy INT,
                       kong INT,
                       request INT
                    );
                    ''')
    
    cursor.execute('''
                    CREATE TABLE IF NOT EXISTS users_info (
                       consumer_id_uuid VARCHAR(100),
                       client_ip VARCHAR(30),
                       started_at INT
                    );
                    ''')
    