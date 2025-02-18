import json
import uuid  # Para gerar IDs únicos
import boto3  # Para interagir com o DynamoDB
from botocore.exceptions import BotoCoreError, ClientError

# Configuração do DynamoDB
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = 'users_info'  # Nome da tabela no DynamoDB
table = dynamodb.Table(TABLE_NAME)

# Variáveis para armazenar os paths
USERS_PATH = '/users'
HEALTH_PATH = '/health'

def lambda_handler(event, context):
    try:
        http_method = event.get('httpMethod', '')
        path = event.get('path', '')

        # Health check - Verifica se a API está rodando
        if http_method == 'GET' and path == HEALTH_PATH:
            return success_response({'message': 'API is up and running'})

        # Criar usuário (POST /users)
        elif http_method == 'POST' and path == USERS_PATH:
            return create_user(event)

        # Buscar usuário por ID (GET /users/{user_id})
        elif http_method == 'GET' and path.startswith(USERS_PATH):
            return get_user(event)

        # Atualizar usuário (PUT /users/{user_id})
        elif http_method == 'PUT' and path.startswith(USERS_PATH):
            return update_user(event)

        # Deletar usuário (DELETE /users/{user_id})
        elif http_method == 'DELETE' and path.startswith(USERS_PATH):
            return delete_user(event)

        else:
            return error_response(405, 'Method Not Allowed')

    except json.JSONDecodeError:
        return error_response(400, 'Invalid JSON format')
    except Exception as e:
        return error_response(500, f'Internal Server Error: {str(e)}')

# Função para criar um novo usuário
def create_user(event):
    try:
        body = json.loads(event['body'])
        if 'name' not in body or 'email' not in body:
            return error_response(400, 'Missing required fields: name, email')

        user_id = str(uuid.uuid4())  # Gerando um ID único
        user_data = {
            'user_id': user_id,
            'name': body['name'],
            'email': body['email']
        }

        table.put_item(Item=user_data)

        return success_response(user_data, 201)

    except (BotoCoreError, ClientError) as e:
        return error_response(500, f'DynamoDB error: {str(e)}')

# Função para buscar um usuário por ID
def get_user(event):
    try:
        path_params = event.get('pathParameters', {})
        user_id = path_params.get('user_id')

        if not user_id:
            return list_all_users()  # Se não tem ID, retorna todos os usuários

        response = table.get_item(Key={'user_id': user_id})
        user = response.get('Item')

        if not user:
            return error_response(404, 'User not found')

        return success_response(user)

    except (BotoCoreError, ClientError) as e:
        return error_response(500, f'DynamoDB error: {str(e)}')

# Função para listar todos os usuários
def list_all_users():
    try:
        response = table.scan()
        users = response.get('Items', [])
        return success_response(users)
    except (BotoCoreError, ClientError) as e:
        return error_response(500, f'DynamoDB error: {str(e)}')

# Função para atualizar um usuário
def update_user(event):
    try:
        path_params = event.get('pathParameters', {})
        user_id = path_params.get('user_id')

        if not user_id:
            return error_response(400, 'Missing user ID')

        body = json.loads(event['body'])
        if 'name' not in body or 'email' not in body:
            return error_response(400, 'Missing required fields: name, email')

        # Atualizar o usuário no DynamoDB
        table.update_item(
            Key={'user_id': user_id},
            UpdateExpression='SET #name = :name, email = :email',
            ExpressionAttributeNames={'#name': 'name'},
            ExpressionAttributeValues={':name': body['name'], ':email': body['email']}
        )

        return success_response({'message': 'User updated successfully'})

    except (BotoCoreError, ClientError) as e:
        return error_response(500, f'DynamoDB error: {str(e)}')

# Função para deletar um usuário
def delete_user(event):
    try:
        path_params = event.get('pathParameters', {})
        user_id = path_params.get('user_id')

        if not user_id:
            return error_response(400, 'Missing user ID')

        # Deletar do DynamoDB
        table.delete_item(Key={'user_id': user_id})

        return success_response({'message': f'User {user_id} deleted successfully'}, 204)

    except (BotoCoreError, ClientError) as e:
        return error_response(500, f'DynamoDB error: {str(e)}')

# Função para retornar resposta de sucesso
def success_response(data, status_code=200):
    return {
        'statusCode': status_code,
        'body': json.dumps(data)
    }

# Função para retornar erro
def error_response(status_code, message):
    return {
        'statusCode': status_code,
        'body': json.dumps({'error': message})
    }
