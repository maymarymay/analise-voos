#Lê os 12 CSVs mensais do volume `voebem.bronze.arquivos/vra/` e materializa `voebem.bronze.vra`.
# nada de tipagem, nada de filtro, colunas de auditoria, idempotente

from pyspark.sql import functions as F

CAMINHO = "/Volumes/voebem/bronze/arquivos/vra/*.csv"
TABELA = "voebem.bronze.vra"

#`sep=";"`-  qual separador 
# `skipRows=1` a 1ª linha é `Atualizado em: <data>` é skippada
# `header=true` a 2ª linha é o cabeçalho de verdade 
# `inferSchema` **desligado** (default), bronze não tipa

bruto = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)          
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")  
    .load(CAMINHO)
)

print("colunas lidas do arquivo:")
for c in bruto.columns:
    print(f"  {c!r}")

# COMMAND ----------

#`ICAO Empresa Aérea` é um nome de coluna válido em CSV e **inválido** em Delta já que espaço está na lista de caracteres proibidos (` ,;{}()\n\t=`).
# bronze preserva o valor e a granularidade. não a grafia do cabeçalho

RENOMEAR = {
    "ICAO Empresa Aérea": "icao_empresa",
    "Número Voo": "numero_voo",
    "Código Autorização (DI)": "codigo_di",
    "Código Tipo Linha": "codigo_tipo_linha",
    "ICAO Aeródromo Origem": "icao_origem",
    "ICAO Aeródromo Destino": "icao_destino",
    "Partida Prevista": "partida_prevista",
    "Partida Real": "partida_real",
    "Chegada Prevista": "chegada_prevista",
    "Chegada Real": "chegada_real",
    "Situação Voo": "situacao_voo",
    "Código Justificativa": "codigo_justificativa",
}

faltando = [c for c in RENOMEAR if c not in bruto.columns]
assert not faltando, f"Coluna esperada nao encontrada no CSV: {faltando}"

renomeado = bruto.select(
    *[F.col(f"`{origem}`").cast("string").alias(novo) for origem, novo in RENOMEAR.items()]
)

# COMMAND ----------

#  adicionar `_arquivo_origem` que diz de qual CSV a linha veio, `_metadata` é uma coluna oculta que o Spark expõe em qualquer leitura de arquivo); e `_ingerido_em`.

bronze = renomeado.withColumn(
    "_arquivo_origem", F.col("_metadata.file_name")
).withColumn(
    "_ingerido_em", F.current_timestamp()
)

# para escrita idempotente `mode("overwrite")` sobre o conjunto inteiro de arquivos.
#o volume tem os 12 arquivos do mês fechado, e a ANAC republica o mês inteiro quando corrige algo. A entrada
# define o estado final, logo o destino pode ser derivado inteiro dela.
# `overwrite` no Delta é atômic, ou a versão nova aparece inteira, ou a antiga continua valendo.
# e o histórico não se perde, cada `overwrite` gera uma versão nova no log do Delta, e a anterior continua acessível por time travel.

(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA)
)

print(f"{TABELA}: {spark.table(TABELA).count():,} linhas")

spark.sql(f"""
    COMMENT ON TABLE {TABELA} IS
    'Bronze - VRA (Voo Regular Ativo) da ANAC, 12 meses (ago/2025 a jul/2026).
     Dado bruto: todas as colunas string, nenhuma linha descartada.
     Carga full refresh idempotente a partir de /Volumes/voebem/bronze/arquivos/vra/.'
""")


display(
    spark.sql(f"""
        SELECT _arquivo_origem, COUNT(*) AS linhas, MAX(_ingerido_em) AS ingerido_em
        FROM {TABELA}
        GROUP BY _arquivo_origem
        ORDER BY _arquivo_origem
    """)
)
