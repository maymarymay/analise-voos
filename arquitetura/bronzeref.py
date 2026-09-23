# Um arquivo, uma tabela. Empresas aéreas chegam em DOIS cadastros da ANAC, e no
# bronze elas continuam em duas tabelas, bronze preserva o dado como chegou.

from pyspark.sql import functions as F

# Caminho dos arquivos de referência
REF = "/Volumes/voebem/bronze/arquivos/referencias"

# Caractere que NAO existe no arquivo -> desliga o quoting do leitor de CSV
SEM_ASPAS = chr(0)



# `encoding = "ISO-8859-1"`. Este CSV não é UTF-8. Lido como UTF-8, "Plácido de
# Castro" vira "Pl?cido de Castro" — e aí o nome do aeroporto chega quebrado no
# produto final, que é o que o cliente vai ler.
# `quote = SEM_ASPAS`** (o caractere NUL, `chr(0)`). O arquivo não usa aspas para
# Mdelimitar campo mas usa o caractere `"` como símbolo de segundo nas coordenadas:
# `09°52'06"S`. Com o `quote='"'` padrão, o Spark abre uma aspa ali e sai
# Mengolindo linhas até achar a próxima. Desligar o quoting (apontando para um
# aractere que não existe no arquivo) é o que mantém uma linha = um registro.

# Faz a leitura do arquivo de aeródromos
aerodromos = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)
    .option("encoding", "ISO-8859-1")   # latin-1, nao UTF-8
    .option("quote", SEM_ASPAS)         # desliga o quoting: aspas aqui sao "segundos"
    .load(f"{REF}/AerodromosPublicos.csv")
)

# Seleciona as colunas e muda os nomes
aerodromos = aerodromos.select(
    F.col("`Código OACI`").alias("icao"),
    F.col("CIAD").alias("ciad"),
    F.col("Nome").alias("nome"),
    F.col("`Município`").alias("municipio"),
    F.col("UF").alias("uf"),
    F.col("`Município Servido`").alias("municipio_servido"),
    F.col("`UF Servido`").alias("uf_servido"),
    F.col("Latitude").alias("latitude"),
    F.col("Longitude").alias("longitude"),
    F.col("Altitude").alias("altitude"),
    F.col("`Situação`").alias("situacao"),
).withColumn("_ingerido_em", F.current_timestamp())

# Salva os dados como uma tabela Delta
aerodromos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("voebem.bronze.aerodromos")

# Mostra a quantidade de linhas e alguns aeroportos para conferência
print(f"bronze.aerodromos: {spark.table('voebem.bronze.aerodromos').count():,} linhas")
display(spark.sql("SELECT icao, nome, municipio, uf FROM voebem.bronze.aerodromos WHERE icao IN ('SBRB','SBGR','SBSP','SBFZ')"))

# Função para ler os arquivos de empresas aéreas
def ler_empresas(arquivo: str):
    """Le um cadastro de empresas. Sem uniao, sem enriquecimento: uma tabela por arquivo."""
    return (
        spark.read.format("csv")
        .option("sep", ";")
        .option("header", "true")
        .option("skipRows", 1)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .load(f"{REF}/{arquivo}")
        .select(
            F.col("ICAO").alias("icao"),
            F.col("Estrangeira").alias("sigla_iata"),
            F.col("Razao").alias("razao_social"),
            F.col("Servico").alias("servico"),
            F.col("Cidade").alias("cidade"),
            F.col("UF").alias("uf"),
            F.col("Ativa").alias("situacao"),
        )
        # Guarda qual foi o arquivo usado
        .withColumn("_arquivo_origem", F.lit(arquivo))
        # Guarda quando os dados foram carregados
        .withColumn("_ingerido_em", F.current_timestamp())
    )


# Lista os dois arquivos e as tabelas onde eles serão salvos
for arquivo, tabela in [
    ("pda_empresas_aereas_nacionais.csv",    "voebem.bronze.empresas_nacionais"),
    ("pda_empresas_aereas_estrangeiros.csv", "voebem.bronze.empresas_estrangeiras"),
]:
    ler_empresas(arquivo).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela)
    print(f"{tabela}: {spark.table(tabela).count():,} linhas")


# Faz uma verificação das duas tabelas de empresas
display(spark.sql("""
    SELECT 'empresas_nacionais' AS tabela, COUNT(*) AS linhas,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS com_icao
    FROM voebem.bronze.empresas_nacionais
    UNION ALL
    SELECT 'empresas_estrangeiras', COUNT(*),
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END)
    FROM voebem.bronze.empresas_estrangeiras
"""))


# Mostra algumas empresas nacionais
display(spark.sql("""
    SELECT icao, razao_social, servico, uf, situacao
    FROM voebem.bronze.empresas_nacionais
    WHERE icao IN ('GLO','TAM','AZU','PAM')
    ORDER BY icao
"""))

# Mostra algumas empresas estrangeiras
display(spark.sql("""
    SELECT icao, razao_social, servico, situacao
    FROM voebem.bronze.empresas_estrangeiras
    WHERE icao IN ('AAL','TAP','AVA','ARG')
    ORDER BY icao
"""))


# Códigos usados para descrever os tipos de operação
CODIGOS = [
    ("codigo_di", "0", "Etapa Regular"),
    ("codigo_di", "2", "Etapa Extra"),
    ("codigo_di", "3", "Etapa de Retorno"),
    ("codigo_di", "4", "Inclusão de Etapa"),
    ("codigo_di", "6", "Etapa Não Remunerada Sem Transporte de Objetos"),
    ("codigo_di", "7", "Etapa de Voo de Fretamento"),
    ("codigo_di", "9", "Etapa de Voo Charter"),
    ("codigo_di", "D", "Etapa de Voo Duplicada"),
    ("codigo_di", "E", "Etapa Não Remunerada Com Transporte de Objetos"),
    ("codigo_tipo_linha", "N", "Doméstica Mista"),
    ("codigo_tipo_linha", "C", "Doméstica Cargueira"),
    ("codigo_tipo_linha", "I", "Internacional Mista"),
    ("codigo_tipo_linha", "G", "Internacional Cargueira"),
]

# Cria um DataFrame com os códigos
codigos = spark.createDataFrame(CODIGOS, "dominio string, codigo string, descricao string")

# Salva os códigos na tabela bronze
codigos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("voebem.bronze.codigos_operacao")

# Mostra a quantidade de linhas da tabela
print(f"bronze.codigos_operacao: {spark.table('voebem.bronze.codigos_operacao').count()} linhas")

# Mostra os códigos cadastrados
display(spark.table("voebem.bronze.codigos_operacao"))


# Mostra todas as tabelas existentes na camada bronze
display(spark.sql("SHOW TABLES IN voebem.bronze"))


# Mostra o histórico de versões da tabela VRA
display(spark.sql("""
    SELECT version, timestamp, operation,
           operationMetrics.numOutputRows AS linhas_escritas
    FROM (DESCRIBE HISTORY voebem.bronze.vra)
    ORDER BY version
"""))

# Compara a primeira versão da tabela com a versão atual
display(spark.sql("""
    SELECT 'versao 0 (1a carga)'   AS versao,
           COUNT(*)                AS linhas,
           MIN(_ingerido_em)       AS ingerido_em
    FROM voebem.bronze.vra VERSION AS OF 0
    UNION ALL
    SELECT 'versao atual', COUNT(*), MIN(_ingerido_em)
    FROM voebem.bronze.vra
"""))


# Adiciona comentários explicando o objetivo de cada tabela
for tabela, comentario in [
    ("voebem.bronze.aerodromos",
     "Bronze - cadastro de aerodromos publicos da ANAC, como chegou. Chave: codigo ICAO (OACI). "
     "Cobre apenas aerodromos brasileiros - aeroportos estrangeiros do VRA nao estao aqui."),
    ("voebem.bronze.empresas_nacionais",
     "Bronze - cadastro de empresas aereas NACIONAIS da ANAC, como chegou. Chave: codigo ICAO. "
     "Nao unir com empresas_estrangeiras nesta camada: a uniao e feita na silver."),
    ("voebem.bronze.empresas_estrangeiras",
     "Bronze - cadastro de empresas aereas ESTRANGEIRAS autorizadas a operar no Brasil, como chegou. "
     "Chave: codigo ICAO. Cadastro separado do nacional na origem, mantido separado no bronze."),
    ("voebem.bronze.codigos_operacao",
     "Bronze - seed table curada a partir da pagina de descricao de variaveis da ANAC. "
     "Traduz codigo_di e codigo_tipo_linha para descricao em portugues."),
]:
    # Aplica o comentário na tabela
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")

# Informa que os comentários foram aplicados
print("comentarios aplicados")
