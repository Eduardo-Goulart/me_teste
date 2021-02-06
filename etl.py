import pandas as pd
import json
import mysql.connector
import database

def process_request(auth_data, request_data, service_id):
    
    processed_row = {}
    processed_row['method'] = request_data['method'] 
    processed_row['uri'] = request_data['uri']
    processed_row['url'] = request_data['url']
    processed_row['size'] = request_data['size']
    processed_row['querystring'] = request_data['querystring'] 
    processed_row['headers_accept'] = request_data['headers']['accept']
    processed_row['headers_host'] = request_data['headers']['host']
    processed_row['headers_user_agent'] = request_data['headers']['user-agent']
    processed_row['consumer_id_uuid'] = auth_data['uuid']
    processed_row['service_id'] = service_id
    
    return processed_row

def process_response(data):
    
    processed_row={}
    processed_row['status'] = data['status']
    processed_row['size'] = data['size']
    processed_row['headers_content_length'] = data['headers']['Content-Length']
    processed_row['headers_via'] = data['headers']['via']
    processed_row['headers_connection'] = data['headers']['Connection']
    processed_row['headers_access_control_allow_credentials'] = data['headers']['access-control-allow-credentials']
    processed_row['headers_content_type'] = data['headers']['Content-Type']
    processed_row['headers_server'] = data['headers']['server']
    processed_row['headers_access_control_allow_origin'] = data['headers']['access-control-allow-origin']
    
    return processed_row

def process_route(data):
    
    processed_row = {}
    processed_row['created_at'] = data['created_at']
    processed_row['hosts'] = data['hosts']
    processed_row['id'] = data['id']
    processed_row['methods'] = data['methods']
    processed_row['paths'] = data['paths']
    processed_row['preserve_host'] = data['preserve_host']
    processed_row['protocols'] = data['protocols']
    processed_row['regex_priority'] = data['regex_priority']
    processed_row['service_id'] = data['service']['id']
    processed_row['strip_path'] = data['strip_path']
    processed_row['updated_at'] = data['updated_at']
    
    return processed_row

def process_service(data):
    
    processed_row = {}
    processed_row['connect_timeout'] = data['connect_timeout']
    processed_row['created_at'] = data['created_at']
    processed_row['host'] = data['host']
    processed_row['id'] = data['id']
    processed_row['name'] = data['name']
    processed_row['path'] = data['path']
    processed_row['port'] = data['port']
    processed_row['protocol'] = data['protocol']
    processed_row['read_timeout'] = data['read_timeout']
    processed_row['retries'] = data['retries']
    processed_row['updated_at'] = data['updated_at']
    processed_row['write_timeout'] = data['write_timeout']
    
    return processed_row

def process_latencies(service_id, data):
    
    processed_row = {}
    
    processed_row['service_id'] = service_id
    processed_row['proxy'] = data['proxy']
    processed_row['kong'] = data['kong']
    processed_row['request'] = data['request']
    
    return processed_row
    
def process_users_info(auth_data, client_ip_data, started_at_data):
    
    processed_row = {}
    processed_row['consumer_id_uuid'] = auth_data['uuid']
    processed_row['client_ip'] = client_ip_data
    processed_row['started_at'] = started_at_data
    
    return processed_row

def extract(lines):
    
    processed_request = list()
    processed_response = list()
    processed_route = list()
    processed_service = list()
    processed_latencies = list()
    processed_users_info = list()
    
    for line in lines:
        data = json.loads(line)
        
        processed_request.append(process_request(data['authenticated_entity']['consumer_id'], data['request'], data['service']['id']))
        processed_response.append(process_response(data['response']))
        processed_route.append(process_route(data['route']))
        processed_service.append(process_service(data['service']))
        processed_latencies.append(process_latencies(data['service']['id'], data['latencies']))
        processed_users_info.append(process_users_info(data['authenticated_entity']['consumer_id'], data['client_ip'], data['started_at']))
        
    
    df_request = pd.DataFrame(processed_request)
    df_response = pd.DataFrame(processed_response)
    df_route = pd.DataFrame(processed_route)
    df_service = pd.DataFrame(processed_service)
    df_latencies = pd.DataFrame(processed_latencies)
    df_users_info = pd.DataFrame(processed_users_info)
    
    return {'request': df_request,
                'response': df_response,
                'route':  df_route,
                'service':  df_service,
                'latencies':  df_latencies,
                'users_info': df_users_info
           }

def transform(extracted_data):
    
    extracted_data['request']['querystring'] = extracted_data['request']['querystring'].astype('str')
    extracted_data['route']['methods'] = extracted_data['route']['methods'].astype('str')
    extracted_data['route']['protocols'] = extracted_data['route']['protocols'].astype('str')
    extracted_data['route']['paths'] = extracted_data['route']['paths'].astype('str')
    extracted_data['service'] = extracted_data['service'].drop_duplicates(subset=['id'])
    
    return extracted_data

def load(transformed_data, cursor):
    
    print('Load tables started')
    
    for _, row in transformed_data['request'].iterrows():
        cursor.execute('''
            INSERT INTO request 
                (method, uri, url, size, querystring, headers_accept, headers_host, headers_user_agent, consumer_id_uuid, service_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (row['method'], row['uri'], row['url'], row['size'], row['querystring'], row['headers_accept'], row['headers_host'], 
             row['headers_user_agent'], row['consumer_id_uuid'], row['service_id']))
    
    print('Request table loaded!')
    
    for _,row in transformed_data['response'].iterrows():
        cursor.execute('''
            INSERT INTO response
                (status, size, headers_content_length, headers_via, headers_connection, headers_access_control_allow_credentials, headers_content_type, 
                 headers_server, headers_access_control_allow_origin)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''', 
            (row['status'], row['size'], row['headers_content_length'], row['headers_via'], row['headers_connection'], 
             row['headers_access_control_allow_credentials'], row['headers_content_type'], row['headers_server'], row['headers_access_control_allow_origin']))
        
    print('Response table loaded!')
        
    for _,row in transformed_data['route'].iterrows():
        cursor.execute('''
            INSERT INTO route 
                (created_at, host, id, methods, paths, preserve_host, protocols, regex_priority, service_id, strip_path, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', 
            (row['created_at'], row['hosts'], row['id'], row['methods'], row['paths'], row['preserve_host'], row['protocols'], row['regex_priority'], 
             row['service_id'], row['strip_path'], row['updated_at']))
    
    print('Route table loaded!')
    
    for _,row in transformed_data['service'].iterrows():
        cursor.execute('''
            INSERT INTO service 
                (connect_timeout, created_at, host, id, name, path, port, protocol, read_timeout, retries, updated_at, write_timeout)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', 
            (row['connect_timeout'], row['created_at'], row['host'], row['id'], row['name'], row['path'], row['port'], row['protocol'], row['read_timeout'], 
             row['retries'], row['updated_at'], row['write_timeout']))

    print('Service table loaded!')
        
    for _,row in transformed_data['latencies'].iterrows():
        cursor.execute('''
            INSERT INTO latencies 
                (service_id, proxy, kong, request)
            VALUES (%s,%s,%s,%s)''', 
            (row['service_id'], row['proxy'], row['kong'], row['request']))
        
    print('Latencies table loaded!')    
        
    for _,row in transformed_data['users_info'].iterrows():
        cursor.execute('''
            INSERT INTO users_info 
                (consumer_id_uuid, client_ip, started_at)
            VALUES (%s,%s,%s)''', 
            (row['consumer_id_uuid'], row['client_ip'], row['started_at']))

    print('Users_info table loaded!')    

def etl(cursor):
    
    f = open('logs.txt', 'r')
    lines = f.readlines()
    
    print('Extraction started')
    
    extracted_data = extract(lines)
    transformed_data = transform(extracted_data)
    load(transformed_data, cursor)
    
def create_reports(cursor):
    
    print('Creating reports')
    
    cursor.execute("SELECT consumer_id_uuid, count(req_id) FROM request GROUP BY consumer_id_uuid")
    data = cursor.fetchall()
    
    df_req_consumer = pd.DataFrame.from_dict(data).rename(columns={0: 'consumer_id_uuid', 1: 'total'}).to_csv('req_consumer.csv', index=False)
    
    cursor.execute("SELECT service_id, service.name, count(req_id) FROM request INNER JOIN service ON service.id = request.service_id GROUP BY service.name, service_id")
    data = cursor.fetchall()
    
    df_req_services = pd.DataFrame.from_dict(data).rename(columns={0: 'service_id', 1: 'service_name', 2: 'total'}).to_csv('req_services.csv', index=False)
    
    cursor.execute("SELECT service_id, service.name, AVG(proxy), AVG(kong), AVG(request) FROM latencies INNER JOIN service ON service.id = latencies.service_id GROUP BY service_id, service.name")
    data = cursor.fetchall()
    
    df_avgs = pd.DataFrame.from_dict(data).rename(columns={0: "service_id", 1: "service_name", 2: "proxy_avg", 3: "kong_avg", 4: "request_avg"}).to_csv('latencies_avgs.csv', index=False)
    
def main():
    db = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = "",
    )
    
    cursor = db.cursor()
    
    database.create_database()
    
    cursor.execute("USE me_database")
    
    etl(cursor)
    
    create_reports(cursor)
    
    cursor.close()
    
main()