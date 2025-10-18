import papermill as pm
import boto3
from datetime import datetime, timezone
import json
import os
import pytz

BUCKET_RAW = os.environ.get("BUCKET_RAW")
BUCKET_TRUSTED = os.environ.get("BUCKET_TRUSTED")
PREFIXO = 'iot'
ARQUIVO_CONTROLE = 'arquivos_processados.json'
try:
    s3 = boto3.client('s3')

    if os.path.exists(ARQUIVO_CONTROLE):
        with open(ARQUIVO_CONTROLE, 'r') as f:
            arquivos_processados = set(json.load(f))
    else:
        arquivos_processados = set()
        # cria o arquivo vazio (lista) no disco
        with open(ARQUIVO_CONTROLE, 'w') as f:
            json.dump(list(arquivos_processados), f)

    hoje = datetime.now(pytz.timezone("America/Sao_paulo")).date()
    novos_arquivos = []

    response = s3.list_objects_v2(Bucket=BUCKET_RAW, Prefix=PREFIXO)
    for item in response.get('Contents', []):
        chave = item['Key']
        data_modificacao = item['LastModified'].astimezone(pytz.timezone("America/Sao_paulo")).date()
        
        if data_modificacao == hoje and chave not in arquivos_processados:
            novos_arquivos.append(chave)
    for arquivo in novos_arquivos:
        try:
            pm.execute_notebook(
                input_path='~/home/ec2-user/tratativas/tratamento-e-envio-edu.ipynb',
                output_path='saida.ipynb',
                parameters={
                    'archive_name': arquivo, 
                    'raw_bucket': BUCKET_RAW, 
                    'output_bucket': BUCKET_TRUSTED
                }
            )
            print("Notebook rodou com sucesso!")
        except Exception as e:
            print("Erro ao executar o notebook:", e)

        arquivos_processados.add(arquivo)
except Exception as e:
    print(e)