# Documentação Completa do Banco de Dados - YELLOW Engagement Tracker

Este documento serve como a referência central e oficial para a estrutura do banco de dados do projeto. Ele detalha o propósito, o esquema e as relações de cada tabela, bem como a lógica das funções de banco de dados (RPCs) que alimentam o leaderboard.

---

## Parte 1: Estrutura das Tabelas (Schema)

### Tabela 1: `authors`

> **Propósito:** Tabela central que armazena os perfis dos embaixadores, servindo como a principal fonte de verdade sobre os participantes e ligando suas identidades do Telegram e do Twitter.

**Schema:**

| Coluna                 | Tipo de Dado  | Chave / Restrição                        |
| ---------------------- | ------------- | ---------------------------------------- |
| `telegram_id`          | `BIGINT`      | **Chave Primária**                       |
| `telegram_name`        | `TEXT`        |                                          |
| `twitter_id`           | `TEXT`        | **Única**, Não Nula                      |
| `twitter_username`     | `TEXT`        |                                          |
| `twitter_name`         | `TEXT`        |                                          |
| `twitter_description`  | `TEXT`        |                                          |
| `twitter_followers`    | `INT`         |                                          |
| `twitter_following`    | `INT`         |                                          |
| `twitter_statusescount`| `INT`         |                                          |
| `twitter_mediacount`   | `INT`         |                                          |
| `twitter_createdat`    | `TIMESTAMPTZ` |                                          |
| `twitter_isblueverified`| `BOOLEAN`     |                                          |
| `twitter_profilepicture`| `TEXT`        |                                          |
| `sync_timestamp`       | `TIMESTAMPTZ` | Padrão: `now()`                          |

---

### Tabela 2: `tweets`

> **Propósito:** Registra cada tweet postado por um embaixador. É uma tabela fundamental para o cálculo de pontos, considerando o tipo de conteúdo e o bônus por visualizações.

**Schema:**

| Coluna             | Tipo de Dado  | Chave / Restrição                               |
| ------------------ | ------------- | ----------------------------------------------- |
| `tweet_id`         | `TEXT`        | **Chave Primária**                              |
| `author_id`        | `TEXT`        | **Chave Estrangeira** (ref: `authors.twitter_id`) |
| `twitter_url`      | `TEXT`        |                                                 |
| `text`             | `TEXT`        |                                                 |
| `createdat`        | `TIMESTAMPTZ` |                                                 |
| `views`            | `INT`         |                                                 |
| `likes`            | `INT`         |                                                 |
| `retweets`         | `INT`         |                                                 |
| `replies`          | `INT`         |                                                 |
| `quotes`           | `INT`         |                                                 |
| `bookmarks`        | `INT`         |                                                 |
| `content_type`     | `TEXT`        |                                                 |
| `media_url`        | `TEXT`        |                                                 |
| `is_thread`        | `BOOLEAN`     |                                                 |
| `is_thread_checked`| `BOOLEAN`     | Padrão: `FALSE`                                 |

**Descrição das Colunas:**
*   `content_type`: Armazena o tipo de mídia. Os valores utilizados são `''` (ou `NULL` para texto), `'photo'` (para imagem) e `'video'`.
*   `is_thread`: Se `TRUE`, o tweet é pontuado como uma imagem (10 pts), independentemente do `content_type`.
*   `views`: Se for maior ou igual a 1000, a pontuação base do tweet é multiplicada por 2.
*   `is_thread_checked`: Campo de controle para o script `thread_identifier.py` não reprocessar tweets.

---

### Tabela 3: `tweet_entities`

> **Propósito:** Armazena as "entidades" (menções a outros usuários, hashtags, URLs) contidas em cada tweet, permitindo análises futuras.

**Schema:**

| Coluna            | Tipo de Dado | Chave / Restrição                             |
| ----------------- | ------------ | --------------------------------------------- |
| `id`              | `BIGINT`     | **Chave Primária** (Auto-incremento)          |
| `tweet_id`        | `TEXT`       | **Chave Estrangeira** (ref: `tweets.tweet_id`)  |
| `entity_type`     | `TEXT`       | Não Nula                                      |
| `text_in_tweet`   | `TEXT`       |                                               |
| `mentioned_user_id`| `TEXT`       |                                               |
| `expanded_url`    | `TEXT`       |                                               |

---

### Tabela 4: `tweet_metrics_history`

> **Propósito:** Cria "snapshots" periódicos das métricas de um tweet, permitindo a análise da evolução do seu engajamento ao longo do tempo.

**Schema:**

| Coluna       | Tipo de Dado  | Chave / Restrição                             |
| ------------ | ------------- | --------------------------------------------- |
| `id`         | `BIGINT`      | **Chave Primária** (Auto-incremento)          |
| `tweet_id`   | `TEXT`        | **Chave Estrangeira** (ref: `tweets.tweet_id`)  |
| `snapshot_at`| `TIMESTAMPTZ` | Padrão: `now()`                               |
| `views`      | `INT`         |                                               |
| `likes`      | `INT`         |                                               |
| `retweets`   | `INT`         |                                               |
| `replies`    | `INT`         |                                               |
| `quotes`     | `INT`         |                                               |
| `bookmarks`  | `INT`         |                                               |

---

### Tabela 5: `user_activity`

> **Propósito:** Rastreia a pontuação de atividade dos embaixadores no grupo do Telegram, calculada com base na frequência e distribuição de mensagens ao longo do dia.

**Schema:**

| Coluna                 | Tipo de Dado       | Chave / Restrição                               |
| ---------------------- | ------------------ | ----------------------------------------------- |
| `id`                   | `BIGINT`           | **Chave Primária** (Auto-incremento)            |
| `user_id`              | `BIGINT`           | **Chave Estrangeira** (ref: `authors.telegram_id`)|
| `activity_date`        | `DATE`             | Não Nula                                        |
| `total_day_score`      | `DOUBLE PRECISION` |                                                 |
| `intervals_details_json`| `JSONB`            |                                                 |

---

### Tabela 6: `ambassador_engagements`

> **Propósito:** Registra as interações valiosas (respostas e retweets) entre os embaixadores, formando a base da pontuação de engajamento.

**Schema:**

| Coluna              | Tipo de Dado  | Chave / Restrição                               |
| ------------------- | ------------- | ----------------------------------------------- |
| `id`                | `BIGINT`      | **Chave Primária** (Auto-incremento)            |
| `tweet_id`          | `TEXT`        | **Chave Estrangeira** (ref: `tweets.tweet_id`)  |
| `tweet_author_id`   | `TEXT`        | **Chave Estrangeira** (ref: `authors.twitter_id`)|
| `interacting_user_id`| `TEXT`        | **Chave Estrangeira** (ref: `authors.twitter_id`)|
| `action_type`       | `TEXT`        | Não Nula                                        |
| `points_awarded`    | `INT`         |                                                 |
| `created_at`        | `TIMESTAMPTZ` | Padrão: `now()`                                 |

**Descrição das Colunas:**
*   `action_type`: Define a pontuação. Os valores são `'reply'` (2 pts) ou `'retweet_or_quote'` (2 pts).

---

### Tabela 7: `manual_contributions`

> **Propósito:** Permite que administradores adicionem pontos manualmente por contribuições especiais e de alto impacto que não podem ser rastreadas automaticamente.

**Schema:**

| Coluna            | Tipo de Dado  | Chave / Restrição                               |
| ----------------- | ------------- | ----------------------------------------------- |
| `id`              | `BIGINT`      | **Chave Primária** (Auto-incremento)            |
| `user_id`         | `BIGINT`      | **Chave Estrangeira** (ref: `authors.telegram_id`)|
| `contribution_type`| `TEXT`        | Não Nula                                        |
| `points_awarded`  | `INT`         |                                                 |
| `description`     | `TEXT`        |                                                 |
| `recorded_by`     | `TEXT`        |                                                 |
| `created_at`      | `TIMESTAMPTZ` | Padrão: `now()`                                 |

**Descrição das Colunas:**
*   `contribution_type`: Categoriza a contribuição. Ex: `partner_introduction`, `product_feedback`, `recruitment_ambassador`.

---

### Tabela 8: `leaderboard`

> **Propósito:** Armazena o placar em tempo real (ou o mais recente). É a tabela principal para consulta do ranking atual, consolidando todas as fontes de pontuação.

**Schema:**

| Coluna                         | Tipo de Dado       | Chave / Restrição                               |
| ------------------------------ | ------------------ | ----------------------------------------------- |
| `user_id`                      | `BIGINT`           | **Chave Primária** / **Estrangeira** (ref: `authors.telegram_id`) |
| `last_updated`                 | `TIMESTAMPTZ`      |                                                 |
| `rank`                         | `INT`              |                                                 |
| `total_score_from_tweets`      | `DOUBLE PRECISION` |                                                 |
| `total_score_from_engagements` | `DOUBLE PRECISION` |                                                 |
| `total_score_from_telegram`    | `DOUBLE PRECISION` |                                                 |
| `total_score_from_contributions`| `DOUBLE PRECISION` |                                                 |
| `grand_total_score`            | `DOUBLE PRECISION` |                                                 |
| ... (e todas as colunas `count_*`) | `INT`              |                                                 |


---

### Tabela 9: `leaderboard_history`

> **Propósito:** Salva um histórico do placar a cada vez que ele é gerado, criando um registro "fotográfico" da evolução dos embaixadores ao longo do tempo.

**Schema:** Idêntico à tabela `leaderboard`, com a adição de:
| Coluna             | Tipo de Dado  | Chave / Restrição                    |
| ------------------ | ------------- | ------------------------------------ |
| `id`               | `BIGINT`      | **Chave Primária** (Auto-incremento) |
| `snapshot_timestamp` | `TIMESTAMPTZ` |                                      |

---

## Parte 2: Funções de Banco de Dados (RPCs)

### Função 1: `calculate_leaderboard()`

> **Propósito:** É a função principal que executa a lógica de cálculo. Ela lê os dados de todas as tabelas de origem (`tweets`, `ambassador_engagements`, etc.), aplica as regras de pontuação e retorna uma tabela virtual com a pontuação completa e detalhada para cada embaixador.

### Função 2: `update_leaderboard_ranks()`

> **Propósito:** Uma função de utilidade que percorre a tabela `leaderboard` e preenche a coluna `rank` com a classificação correta (1º, 2º, 3º lugar) com base no `grand_total_score`.

### Função 3: `update_leaderboard_history_ranks()`

> **Propósito:** Similar à anterior, mas opera na tabela `leaderboard_history`. Ela calcula o ranking para um `snapshot_timestamp` específico, garantindo que os ranks históricos sejam salvos corretamente. 