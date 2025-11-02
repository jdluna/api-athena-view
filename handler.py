import os
import time
import json
import boto3

athena = boto3.client('athena')

def query_athena_view(event, context):
    # Defensive coding for environment variables
    try:
        athena_view = os.environ['ATHENA_VIEW']
        athena_db = os.environ['ATHENA_DB']
        s3_output = os.environ['S3_OUTPUT']
    except KeyError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Environment variable {str(e)} not set"})
        }

    query = f"SELECT * FROM {athena_view}"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': athena_db},
        ResultConfiguration={'OutputLocation': s3_output}
    )
    query_execution_id = response['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(1)

    if state != 'SUCCEEDED':
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Query failed'})
        }

    results = athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=1000)  # Increase or make configurable if needed
    columns = [c['Name'] for c in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = [
        dict(zip(columns, [field.get('VarCharValue', '') for field in row['Data']]))
        for row in results['ResultSet']['Rows'][1:]
    ]

    return {
        'statusCode': 200,
        'body': rows,  # <<-- Pass as Python list, will be encoded by lote_aggregation
        'headers': {"Content-Type": "application/json"}
    }

def lote_aggregation(event, context):
    athena_response = query_athena_view(event, context)

    if athena_response['statusCode'] != 200:
        return athena_response

    results = athena_response["body"]

    if isinstance(results, str):
        results = json.loads(results)

    aggregation = {}
    for record in results:
        lote = record['codigo_lote']
        area = float(record['area_terreno_m2'])
        if lote not in aggregation:
            aggregation[lote] = {'count_predios': 0, 'total_area_m2': 0.0}
        aggregation[lote]['count_predios'] += 1
        aggregation[lote]['total_area_m2'] += area

    output = [
        {'codigo_lote': k, 'count_predios': v['count_predios'], 'total_area_m2': v['total_area_m2']}
        for k, v in aggregation.items()
    ]

    return {
        "statusCode": 200,
        "body": json.dumps(output),
        "headers": {"Content-Type": "application/json"}
    }
