# Roteiro de Implementação: Leaderboard V2 (Atualizado)

Este documento é o plano de ação oficial e atualizado para a implementação do sistema de pontuação e automação do Leaderboard.

---

## Fase 1: Fundação e Estrutura do Banco de Dados (CONCLUÍDO)

*   **Objetivo:** Preparar o banco de dados para suportar a nova lógica de negócios.
*   **Status:** Concluído.
*   **Ações Realizadas:**
    1.  Criação do arquivo `create_schema.sql` com todas as tabelas e relações.
    2.  Adição das colunas `is_thread` e `is_thread_checked` na tabela `tweets`.
    3.  Documentação de todas as regras de pontuação como comentários dentro do `create_schema.sql`.

---

## Fase 2: População de Dados Essenciais (PARCIALMENTE CONCLUÍDO)

*   **Objetivo:** Carregar os dados iniciais e históricos no banco de dados.
*   **Status:** Em andamento.
*   **Ações Realizadas:**
    1.  Criação de scripts para popular as tabelas `tweets` e `tweet_entities` a partir de arquivos CSV.
*   **Ações Pendentes:**
    1.  **Criar o script `populate_metrics_history.py`:** Ler o arquivo `tweet_metrics_history_rows.csv` e inserir os dados de snapshots na tabela `tweet_metrics_history`.

---

## Fase 3: Enriquecimento de Dados (CONCLUÍDO)

*   **Objetivo:** Processar os dados brutos para adicionar inteligência (identificar threads).
*   **Status:** Concluído.
*   **Ações Realizadas:**
    1.  Criação do script `thread_identifier.py` que utiliza uma API externa para verificar quais tweets são threads e atualiza o campo `is_thread` no banco de dados.

---

## Fase 4: Lógica Central de Pontuação (PENDENTE)

*   **Objetivo:** Implementar a lógica de cálculo de pontos diretamente no banco de dados e corrigir dados históricos.
*   **Status:** Pendente.
*   **Ações Pendentes:**
    1.  **Adicionar Funções SQL ao Schema:** Inserir o código-fonte das funções `calculate_leaderboard`, `update_leaderboard_ranks`, e `update_leaderboard_history_ranks` ao final do arquivo `create_schema.sql`. A função principal implementará as regras de pontuação de tweets (6/10/12 pts + bônus de views).
    2.  **Corrigir Timestamps de Engajamentos:**
        *   **Ação Imediata:** Criar um script de correção **isolado** para atualizar os `created_at` na tabela `ambassador_engagements`, fazendo-os corresponder ao `createdat` do tweet original para todos os registros existentes.
        *   **Ação Futura:** Modificar o script `cross_engagement_tracker.py` para que, nas próximas execuções, ele já insira o timestamp correto.

---

## Fase 5: Flexibilidade Administrativa (PENDENTE)

*   **Objetivo:** Permitir que administradores concedam pontos por contribuições de alto impacto.
*   **Status:** Pendente.
*   **Ações Pendentes:**
    1.  **Refatorar `manual_contributions_manager.py`:** Alterar o script para que o administrador possa inserir um valor de pontos variável para cada contribuição, em vez de usar um menu com valores fixos.

---

## Fase 6: Automação Completa via GitHub Actions (PENDENTE)

*   **Objetivo:** Orquestrar a execução de todos os scripts de coleta e processamento de forma periódica e automática.
*   **Status:** Pendente.
*   **Ações Pendentes:**
    1.  **Criar o arquivo de workflow `.github/workflows/update_data.yml`**.
    2.  **Configurar o Workflow:** O arquivo `YAML` deverá definir a sequência e a frequência de execução para todos os scripts relevantes, garantindo a atualização consistente dos dados.
        *   `realtime_activity_updater.py` (Atividade Telegram)
        *   `metrics_snapshot.py` (Snapshots de Métricas de Tweets)
        *   `cross_engagement_tracker.py` (Engajamento Cruzado)
        *   `thread_identifier.py` (Identificação de Threads)
        *   `generate_leaderboard.py` (Cálculo Final do Leaderboard)
    3.  **Configurar Secrets:** O usuário deverá configurar todas as chaves de API e banco de dados (`SUPABASE_URL`, `SUPABASE_KEY`, `TWITTER_API_KEY`, `TELEGRAM_API_ID`, etc.) como "Secrets" no repositório do GitHub.

**Resultado Final Esperado:** Um sistema 100% automatizado que mantém os dados atualizados e o leaderboard sempre correto, sem necessidade de intervenção manual. 