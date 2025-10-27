import boto3
import json
import os
from datetime import datetime, timezone
import papermill as pm
from dotenv import load_dotenv


load_dotenv('/home/ec2-user/automacoes/.env')

# ⚙️ Configurações
BUCKET_TRUSTED = os.getenv("BUCKET_TRUSTED")
BUCKET_CLIENT = os.getenv("BUCKET_CLIENT")
PREFIXO = ""  # exemplo: "arquivos/" ou "" se estiver na raiz
ARQUIVOS_ESPECIFICOS = [
    "tabela_atendimentos_tratada.csv",
    "tabela_sensores_tratada.csv",
    "upa.csv",
    "paciente.csv"
]
ARQUIVO_STATUS = "ultima_verificacao.json"

s3 = boto3.client("s3")

def obter_data_modificacao_s3(key):
    """Obtém a data de última modificação de um objeto S3."""
    response = s3.head_object(Bucket=BUCKET_TRUSTED, Key=key)
    return response["LastModified"]

def carregar_status_anterior():
    """Carrega o histórico de modificações da execução anterior."""
    if os.path.exists(ARQUIVO_STATUS):
        with open(ARQUIVO_STATUS, "r") as f:
            return json.load(f)
    return {}

def salvar_status_atual(status):
    """Salva o histórico de modificações atual."""
    with open(ARQUIVO_STATUS, "w") as f:
        json.dump(status, f, indent=4, ensure_ascii=False)

def verificar_atualizacoes():
    status_anterior = carregar_status_anterior()
    status_atual = {}
    atualizados = []

    for arquivo in ARQUIVOS_ESPECIFICOS:
        key = f"{PREFIXO}{arquivo}" if PREFIXO else arquivo
        try:
            ultima_modificacao = obter_data_modificacao_s3(key)
            ultima_modificacao_iso = ultima_modificacao.astimezone(timezone.utc).isoformat()

            status_atual[arquivo] = ultima_modificacao_iso

            # 🔍 Comparação com execução anterior
            if arquivo not in status_anterior:
                atualizados.append((arquivo, "novo"))
            elif ultima_modificacao_iso != status_anterior[arquivo]:
                atualizados.append((arquivo, "atualizado"))

        except s3.exceptions.ClientError as e:
            print(f"⚠️ Erro ao buscar {arquivo}: {e}")
            continue

    # Salva o estado atual
    salvar_status_atual(status_atual)

    # 📊 Resultado
    if not atualizados:
        print("✅ Nenhum arquivo foi atualizado desde a última verificação.")
    else:
        try:
            pm.execute_notebook(
                input_path='/home/ec2-user/tratativas-bases/registros/tratativa-tabelao-sensores-atendimento.ipynb',
                output_path='saida3.ipynb',
                parameters={
                    'trusted_bucket': BUCKET_TRUSTED,
                    'output_bucket': BUCKET_CLIENT
                }
            )
        except Exception as e:
            print(f"Erro ao executar o notebook para {arquivo}: {e}")

if __name__ == "__main__":
    verificar_atualizacoes()
