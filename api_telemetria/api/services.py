import csv 
import os
import uuid
from decimal import Decimal
from datetime import datetime

from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.db import transaction, connection

from api_telemetria.models import MedicaoVeiculoTemp, Veiculo, Medicao


def executar_procedure_pos_importacao(arquivoid):
    """
    Responsável por chamar uma procedure no banco de dados
    após a importação dos dados.

    Essa procedure pode fazer:
    - Processamentos adicionais
    - Movimentação de dados
    - Cálculos ou validações finais
    """
    with connection.cursor() as cursor:
        cursor.callproc("processa_arquivo", [arquivoid])


def processar_csv_medicoes(arquivo):
    """
    Função principal de importação do CSV.

    Etapas:
    1. Salva o arquivo
    2. Lê e valida os dados
    3. Prepara os registros
    4. Insere no banco
    5. Executa pós-processamento
    """

    # ID único → serve para rastrear essa importação inteira
    arquivoid = str(uuid.uuid4())

    # Define onde o arquivo será salvo fisicamente
    pasta_destino = os.path.join(settings.MEDIA_ROOT, "importacoes_medicao")
    os.makedirs(pasta_destino, exist_ok=True)  # garante que a pasta exista

    # Evita sobrescrever arquivos com o mesmo nome
    nome_salvo = f"{arquivoid}_{arquivo.name}"

    # Sistema de armazenamento do Django
    fs = FileSystemStorage(location=pasta_destino)

    # Salva o arquivo no disco
    nome_arquivo_salvo = fs.save(nome_salvo, arquivo)

    # Caminho completo (usado para leitura depois)
    caminho_completo = os.path.join(pasta_destino, nome_arquivo_salvo)

    # Variáveis de controle
    total_linhas_arquivo = 0   # quantas linhas o CSV tem
    erros = []                 # armazena erros por linha
    linhas_para_inserir = []  # dados válidos preparados para o banco

    # Cache → evita bater no banco a cada linha (ganho grande de performance)
    veiculos_cache = {v.id: v for v in Veiculo.objects.all()}
    medicoes_cache = {m.id: m for m in Medicao.objects.all()}

    # Abre o CSV
    with open(caminho_completo, mode="r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=';')

        # Estrutura mínima obrigatória do arquivo
        campos_esperados = {"veiculoid", "medicaoid", "data", "valor"}

        # Validação: precisa ter cabeçalho
        if not reader.fieldnames:
            raise Exception("O CSV não possui cabeçalho.")

        # Validação: precisa ter todas as colunas necessárias
        if not campos_esperados.issubset(set(reader.fieldnames)):
            raise Exception(
                f"Cabeçalho inválido. Esperado: {list(campos_esperados)}. Recebido: {reader.fieldnames}"
            )

        # Loop principal → processa linha por linha
        for numero_linha, row in enumerate(reader, start=2):
            total_linhas_arquivo += 1

            try:
                # Conversão de tipos (string → tipos corretos)
                id_veiculo = int(row["veiculoid"])
                id_medicao = int(row["medicaoid"])

                # Busca no cache (muito mais rápido que banco)
                veiculo = veiculos_cache.get(id_veiculo)
                if not veiculo:
                    raise Exception(f"Veículo {id_veiculo} não encontrado.")

                medicao = medicoes_cache.get(id_medicao)
                if not medicao:
                    raise Exception(f"Medição {id_medicao} não encontrada.")

                # Converte string para datetime
                data_convertida = datetime.strptime(
                    row["data"].strip(),
                    "%Y-%m-%d %H:%M:%S"
                )

                # Decimal evita erro de precisão (ex: valores financeiros)
                valor_convertido = Decimal(row["valor"].strip())

                # Cria objeto (ainda NÃO salva no banco)
                linhas_para_inserir.append(
                    MedicaoVeiculoTemp(
                        veiculoid=veiculo,
                        medicaoid=medicao,
                        data=data_convertida,
                        valor=valor_convertido,
                        arquivoid=arquivoid
                    )
                )

            except Exception as e:
                # Se der erro, guarda info e continua o processamento
                erros.append({
                    "linha": numero_linha,
                    "erro": str(e)
                })

    # Quantas linhas passaram na validação
    total_linhas_validas = len(linhas_para_inserir)

    # Transação → garante consistência no banco
    with transaction.atomic():

        # Inserção em lote → muito mais eficiente que salvar 1 por 1
        if linhas_para_inserir:
            MedicaoVeiculoTemp.objects.bulk_create(
                linhas_para_inserir,
                batch_size=1000
            )

        # Confere quantas realmente foram salvas
        total_linhas_importadas = MedicaoVeiculoTemp.objects.filter(
            arquivoid=arquivoid
        ).count()

        # Validação final de integridade
        quantidades_conferem = total_linhas_validas == total_linhas_importadas

        if quantidades_conferem:
            # Se tudo certo → roda processamento no banco
            executar_procedure_pos_importacao(arquivoid)
        else:
            # Se algo deu errado → desfaz importação
            MedicaoVeiculoTemp.objects.filter(
                arquivoid=arquivoid
            ).delete()

    # Retorno final → resumo da operação
    return {
        "arquivoid": arquivoid,  # ID da importação
        "arquivo_salvo": nome_arquivo_salvo,
        "caminho": caminho_completo,
        "total_linhas_arquivo": total_linhas_arquivo,
        "total_linhas_importadas": total_linhas_importadas,
        "quantidades_conferem": total_linhas_arquivo == total_linhas_importadas,
        "erros": erros  # lista de erros encontrados
    }