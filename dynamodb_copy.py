import sys
import boto3

def copy_and_append_database(boto3_client, from_table: str, to_table: str, max_items: int = 0):
    '''
    DynamoDB copy and append from one table to another.
    '''

    if boto3_client is None or from_table is None or to_table is None:
        raise ValueError("A value is none. This should not happen.")

    from_db_info = boto3_client.describe_table(TableName=from_table)
    print(from_db_info['Table']['TableName'])
    from_attr_info1 = {obj['AttributeName']: obj['AttributeType'] for obj in from_db_info['Table']['AttributeDefinitions']}
    from_attr_info2 = {obj['AttributeName']: obj['KeyType'] for obj in from_db_info['Table']['KeySchema']}
    print(from_attr_info1)
    print(from_attr_info2)

    to_db_info = boto3_client.describe_table(TableName=to_table)
    print(to_db_info['Table']['TableName'])
    to_attr_info1 = {obj['AttributeName']: obj['AttributeType'] for obj in to_db_info['Table']['AttributeDefinitions']}
    to_attr_info2 = {obj['AttributeName']: obj['KeyType'] for obj in to_db_info['Table']['KeySchema']}
    print(to_attr_info1)
    print(to_attr_info2)

    if len(to_attr_info1) != len(from_attr_info1) or len(to_attr_info2) != len(from_attr_info2):
        raise TypeError('Databases attribute counts are not compatible.')

    for (k,v) in from_attr_info1.items():
        if to_attr_info1[k] != v:
            raise TypeError('Databases attribute types are not compatible.')

    for (k,v) in from_attr_info2.items():
        if to_attr_info2[k] != v:
            raise TypeError('Databases key types are not compatible.')

    counter = 1 if max_items < 1 else max_items
    key_index = None
    while counter > 0:
        if key_index is not None:
            from_items = boto3_client.scan(
                    TableName = from_table,
                    ExclusiveStartKey = key_index
                ) if max_items < 1 else boto3_client.scan(
                    TableName = from_table,
                    Limit = counter,
                    ExclusiveStartKey = key_index
                )
        else:
            from_items = boto3_client.scan(
                    TableName = from_table
                ) if max_items < 1 else boto3_client.scan(
                    TableName = from_table,
                    Limit = counter
                )
        for item in from_items['Items']:
            print(boto3_client.put_item(
                TableName = to_table,
                Item = item
            ))

        if 'LastEvaluatedKey' not in from_items:
            counter = 0
        else:
            if max_items >= 1:
                counter -= from_items['Count']
            key_index = from_items['LastEvaluatedKey']

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print(f'Usage: {sys.argv[0]} <source_table_name> <destination_table_name> [profile_name]')
        sys.exit(1)

    FROM_TABLE = sys.argv[1]
    TO_TABLE = sys.argv[2]

    if FROM_TABLE == TO_TABLE or (not FROM_TABLE) or (not TO_TABLE):
        print('<source_table_name> and <destination_table_name> must be different.')
        sys.exit(1)

    PROFILE = sys.argv[3] if len(sys.argv) > 3 else 'default'

    SESSION = boto3.Session(profile_name=PROFILE)

    AWS_CLIENT = SESSION.client('dynamodb')

    copy_and_append_database(AWS_CLIENT, FROM_TABLE, TO_TABLE, 0)
