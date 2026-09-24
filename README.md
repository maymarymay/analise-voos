# VoeBem Analytics — Voos, atrasos e pontualidade (ANAC)

Projeto de engenharia de dados no Databricks que transforma os dados públicos de Voo
Regular Ativo (VRA) da ANAC em uma tabela analítica única (OBT) e em um agente de BI em
linguagem natural (Genie Space) capaz de responder perguntas de negócio sobre atrasos,
pontualidade e cancelamentos de voos no Brasil.

## Sobre o projeto

O projeto cobre a cadeia completa de um produto de dados: download automatizado dos
arquivos brutos da ANAC, arquitetura Medalhão (bronze → silver → gold) no Databricks,
contrato de qualidade de dados aplicado em um pipeline declarativo, uma tabela gold
desenhada para consumo por LLM e um Genie Space cujas respostas são validadas contra um
gabarito de SQL escrito à mão.

A janela de dados coberta vai de **agosto de 2025 a julho de 2026** (12 meses).

## Arquitetura

Arquitetura Medalhão em três camadas, tudo dentro do catálogo `voebem` no Unity Catalog:

- **Bronze** (`voebem.bronze.*`) — dado bruto, como chegou da ANAC. Sem tipagem, sem
  filtro, com colunas de auditoria (`_arquivo_origem`, `_ingerido_em`). Carga full
  refresh idempotente.
- **Silver** (`voebem.silver.*`) — tipagem, unificação de cadastros e um contrato de
  qualidade de dados: um pipeline declarativo (Lakeflow) marca cada voo com o resultado
  de expectations de integridade e coerência temporal, sem nunca descartar linha. Os
  registros reprovados vão para uma tabela de **quarentena** — um espelho diagnóstico,
  não um filtro.
- **Gold** (`voebem.gold.*`) — camada de negócio, com `obt_voos`: uma *One Big Table*
  desnormalizada, uma linha por etapa de voo, com nomes (não códigos) para companhia e
  aeroportos, métricas de atraso e pontualidade já calculadas, e comentário de negócio em
  cada coluna — desenhada para ser lida por um LLM sem exigir JOIN.

## Estrutura do repositório

```
.
├── baixardados.py            # baixa os CSVs da ANAC e gera fontes.md
├── fontes.md                 # proveniência de cada arquivo baixado (URL, hash, data)
├── arquitetura/
│   ├── bronzevra.py          # bronze.vra — 12 CSVs mensais de VRA
│   ├── bronzeref.py          # bronze.aerodromos, empresas (nacionais/estrangeiras), códigos
│   ├── prata.py              # silver — tipagem, colunas derivadas de data/hora
│   └── ouro.py                # gold — comentários de negócio, tags, lineage
├── pipeline/                  # contrato de qualidade de dados (Lakeflow declarativo)
│   ├── 1_vramarcado.sql       # marca cada voo com flags de integridade referencial
│   ├── 2_vraauditado.sql      # expectations do contrato (warn, nunca descarta linha)
│   └── 3_vraquarentena.sql    # espelho diagnóstico dos registros reprovados
├── sql/
│   ├── preparar-ambiente.sql  # cria catálogo, schemas e volume no Unity Catalog
│   ├── qualidade-metricas.sql # lê as métricas do contrato no event log do pipeline
│   ├── ouro/                  # DDL das tabelas gold (obt_voos, dim_aeroporto, fato_voo)
│   └── gabarito/              # SQL de referência (P1–P5b) usado no teste de aceitação
├── genie/
│   ├── instrucoes.md          # camada semântica do Genie Space (text_instructions)
│   ├── montar_genie_space.py  # gera genie_space.json a partir do gabarito + instruções
│   ├── genie_space.json       # definição do Genie Space (perguntas de exemplo, instruções)
│   └── perguntar_genie.py     # envia uma pergunta ao Genie Space via Databricks CLI
├── teste-aceitacao.md         # Genie Agent × gabarito SQL, antes/depois das instruções
└── dados_projeto/             # dados baixados (12 CSVs de VRA + 3 arquivos de referência)
```

## Fonte de dados

Todos os dados vêm do portal de dados abertos da **ANAC** (Agência Nacional de Aviação
Civil), em `sistemas.anac.gov.br/dadosabertos` — o repositório de `www.gov.br` está
descontinuado e não é usado.

| conjunto | conteúdo | arquivos |
|---|---|---|
| `vra` | Voo Regular Ativo, um CSV por mês | 12 (ago/2025–jul/2026) |
| `referencias` | Aeródromos públicos, empresas aéreas nacionais e estrangeiras | 3 |

O script `baixardados.py` baixa tudo, calcula hash e gera `fontes.md` com a proveniência
completa de cada arquivo. Ele também documenta as armadilhas do portal (numeração de mês
diferente entre pasta e nome de arquivo, encoding, cabeçalho fora da primeira linha, e um
cadastro de empresas nacionais que está corrompido na raiz da pasta de origem).

## Como rodar

1. **Baixar os dados:**
   ```
   python baixardados.py
   ```
2. **Preparar o ambiente no Databricks** (catálogo, schemas e volume no Unity Catalog):
   ```
   sql/preparar-ambiente.sql
   ```
3. **Subir os CSVs baixados** para `/Volumes/voebem/bronze/arquivos/` e rodar, em ordem:
   - `arquitetura/bronzevra.py` e `arquitetura/bronzeref.py` (camada bronze)
   - `arquitetura/prata.py` + os notebooks de `pipeline/` (camada silver e contrato de
     qualidade)
   - `arquitetura/ouro.py` e os scripts de `sql/ouro/` (camada gold)
4. **Consultar a qualidade dos dados** com `sql/qualidade-metricas.sql`.
5. **Perguntar ao Genie Space:**
   ```
   export DATABRICKS_CONFIG_PROFILE=<seu_perfil>
   export GENIE_SPACE_ID=<id_do_seu_space>
   python genie/perguntar_genie.py "Qual companhia entrega a melhor pontualidade?"
   ```

## Genie Space

O Genie Space é um agente de BI em linguagem natural, configurado sobre
`voebem.gold.obt_voos`, com uma camada semântica (`genie/instrucoes.md`) que define, em
português, as regras de negócio do domínio: o que é atraso, pontualidade, cancelamento,
como calcular percentuais sem inflar o resultado, e como tratar perguntas com mais de uma
dimensão. `genie/montar_genie_space.py` gera a definição do space a partir dessas
instruções e do gabarito de SQL, para que os exemplos do Genie e o gabarito nunca
divirjam.

## Teste de aceitação

`teste-aceitacao.md` documenta a validação do Genie Space contra oito perguntas de
negócio, comparando o SQL gerado pelo agente com o gabarito escrito à mão
(`sql/gabarito/`). Na primeira rodada houve 1 falha e 3 divergências; depois de ajustes
nas instruções da camada semântica (sem alterar nenhuma linha de SQL da OBT), todas as
perguntas passaram a bater com o gabarito.

## Tecnologias utilizadas

- Databricks (Unity Catalog, Delta Lake, Lakeflow Declarative Pipelines, Genie Space)
- PySpark e Spark SQL
- Databricks CLI / Python

## Disclaimer

Projeto feito com auxílio de IA.
