import papermill as pm
import boto3
from datetime import datetime
import json
import os
import pytz
from dotenv import load_dotenv

load_dotenv('/home/ec2-user/automacoes/.env')

BUCKET_RAW = os.getenv("BUCKET_RAW")
BUCKET_TRUSTED = os.getenv("BUCKET_TRUSTED")
PREFIXO = 'iot'
ARQUIVO_CONTROLE = 'arquivos_processados.json'

try:
    print(BUCKET_RAW, BUCKET_TRUSTED)
    s3 = boto3.client('s3')

    # Carrega ou cria o arquivo de controle
    if os.path.exists(ARQUIVO_CONTROLE):
        with open(ARQUIVO_CONTROLE, 'r') as f:
            arquivos_processados = set(json.load(f))
    else:
        arquivos_processados = set()
        with open(ARQUIVO_CONTROLE, 'w') as f:
            json.dump([], f)

    hoje = datetime.now(pytz.timezone("America/Sao_paulo")).date()
    novos_arquivos = []

    response = s3.list_object_versions(Bucket=BUCKET_RAW, Prefix=PREFIXO)

    latest_objects = [
        v for v in response.get('Versions', [])
        if v.get('IsLatest', False)
    ]

    for item in latest_objects:
        chave = item['Key']
        data_modificacao = item['LastModified'].astimezone(
            pytz.timezone("America/Sao_paulo")
        ).date()

        if data_modificacao == hoje and chave not in arquivos_processados:
            novos_arquivos.append(chave)

    for arquivo in novos_arquivos:
        try:
            pm.execute_notebook(
                input_path='/home/ec2-user/tratativas/tratamento-e-envio-edu.ipynb',
                output_path='saida.ipynb',
                parameters={
                    'archive_name': arquivo,
                    'raw_bucket': BUCKET_RAW,
                    'output_bucket': BUCKET_TRUSTED
                }
            )
            print(f"Notebook rodou com sucesso para {arquivo}!")
            arquivos_processados.add(arquivo)

        except Exception as e:
            print(f"Erro ao executar o notebook para {arquivo}: {e}")

    # ✅ Grava a lista atualizada no JSON
    with open(ARQUIVO_CONTROLE, 'w') as f:
        json.dump(list(arquivos_processados), f)

except Exception as e:
    print(e)
