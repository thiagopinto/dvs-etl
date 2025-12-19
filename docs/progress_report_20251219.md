# Relatório de Progresso ETL DVS - 19/12/2025

Este documento detalha as melhorias estruturais implementadas para garantir a integridade e performance do pipeline de dados.

## 1. Implementação de Carga Incremental (UPSERT)
A estratégia de carga foi alterada de `replace/append` simples para um fluxo de **UPSERT (Insert or Update)** robusto.

*   **Chave Primária Composta:** Definida como `(ID_AGRAVO, NU_NOTIFIC, NU_ANO)` para garantir a identificação única de cada caso, permitindo que atualizações na fonte reflitam corretamente no banco de dados sem gerar duplicatas.
*   **Estratégia de Staging:** Para otimizar a performance em massa, os dados são carregados primeiro em uma tabela temporária (`staging_...`) e depois fundidos na tabela final via SQL nativo (`INSERT ... ON CONFLICT`). Isso reduz drásticamente o tempo de lock nas tabelas de produção.

## 2. Qualidade e Integridade de Dados
*   **Deduplicação Automática:** O pipeline agora detecta e remove registros duplicados dentro do próprio arquivo de entrada antes do upload, mantendo a última ocorrência (presumida como a mais atualizada).
*   **Sincronização de Schema:** Adicionada uma camada de proteção que introspecta a tabela destino no PostgreSQL e filtra o DataFrame para conter apenas as colunas fisicamente presentes no banco, evitando erros de execução.
*   **Tipagem Estrita:** Correção de tipos de dados para colunas críticas como `SEM_PRI` e `SEM_NOT`, garantindo compatibilidade com o tipo `BIGINT` do banco de dados.

## 3. Otimização de Escopo
*   **Whitelist de Colunas:** Implementado filtro nas classes `DengueSource` e `ChikungunyaSource` para carregar apenas as colunas essenciais à análise epidemiológica, reduzindo o consumo de storage e processamento.

---
**Status Atual:** Pipeline validado com sucesso para Dengue e Chikungunya, suportando cargas incrementais seguras e performáticas.
