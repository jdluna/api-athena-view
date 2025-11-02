import os
import time
import json
import boto3

athena = boto3.client('athena')

def query_athena_view(event, context):
    query = f"SELECT * FROM {os.environ['ATHENA_VIEW']}"
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': os.environ['ATHENA_DB']},
        ResultConfiguration={'OutputLocation': os.environ['S3_OUTPUT']}
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

    results = athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=10)
    columns = [c['Name'] for c in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = [dict(zip(columns, [field.get('VarCharValue', '') for field in row['Data']]))
            for row in results['ResultSet']['Rows'][1:]]

    return {
        'statusCode': 200,
        'body': json.dumps(rows)
    }

def lote_aggregation(event, context):
    athena_response = query_athena_view(event, context)

    # Step 1: Parse the response body
    results_str = athena_response["body"]
    results = json.loads(results_str)

    # Step 2: Aggregate by codigo_lote
    aggregation = {}
    for record in results:
        lote = record['codigo_lote']
        area = float(record['area_terreno_m2'])
        if lote not in aggregation:
            aggregation[lote] = {'count_predios': 0, 'total_area_m2': 0.0}
        aggregation[lote]['count_predios'] += 1
        aggregation[lote]['total_area_m2'] += area

    # Step 3: Prepare the output list
    output = [
        {'codigo_lote': k, 'count_predios': v['count_predios'], 'total_area_m2': v['total_area_m2']}
        for k, v in aggregation.items()
    ]

    return {
        "statusCode": 200,
        "body": json.dumps(output),
        "headers": {"Content-Type": "application/json"}
    }