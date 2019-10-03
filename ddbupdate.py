import boto3
from boto3.dynamodb.conditions import Key, Attr
import os.path

mimetype = {
    '.jpg': 'Image JPEG',
    '.png': 'Image PNG',
    '.gif': 'Image GIF'
}

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Files-08-2019')

def updateItem(item):
    print(item['uuid'], ' ', item['filename'], ' ', mimetype[os.path.splitext(item['filename'])[1]])

    r = table.update_item(
        Key={
            'uuid': item['uuid'],
            'subKey': item['subKey']
        },
        UpdateExpression="set gestalt = :p",
        ExpressionAttributeValues={
            ':p': mimetype[os.path.splitext(item['filename'])[1]]
        },
        ReturnValues="UPDATED_NEW"
    )


response = table.scan(FilterExpression=Attr('gestalt').eq("Image {mimetype[file_ext].upper()}"))
for item in response['Items']:
    updateItem(item)

while 'LastEvaluatedKey' in response:
    response = table.scan(
        FilterExpression=Attr('gestalt').eq("Image {mimetype[file_ext].upper()}"),
        ExclusiveStartKey=response['LastEvaluatedKey']
    )
    for item in response['Items']:
        updateItem(item)