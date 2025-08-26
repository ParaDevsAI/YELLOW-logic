-- =============================================
--      SCRIPT DE TESTE - LEADERBOARD RETROATIVO
-- =============================================
--
-- Objetivo:
-- Este script calcula a evolução diária da pontuação para UM ÚNICO usuário,
-- permitindo validar a lógica de cálculo do histórico do leaderboard.
--
-- Como usar:
-- 1. Substitua o valor 'PUT_YOUR_TELEGRAM_ID_HERE' pelo telegram_id do usuário que você quer testar.
-- 2. Copie e cole todo o script no SQL Editor do seu projeto Supabase.
-- 3. Clique em "RUN".
--
-- O que esperar:
-- Uma tabela mostrando a data, o id do usuário, e a pontuação total acumulada dele em cada dia,
-- desde sua primeira atividade até hoje.
--
-- ESTE SCRIPT APENAS LÊ DADOS (SELECT), NÃO MODIFICA NADA.

WITH
-- =========================================================================================
-- ETAPA 1: DEFINIR O USUÁRIO E O PERÍODO DE ANÁLISE
-- =========================================================================================
target_user AS (
    -- >>>>> COLOQUE O TELEGRAM ID DO USUÁRIO DE TESTE AQUI <<<<<
    SELECT 123456789 AS user_id -- SUBSTITUA 123456789 PELO ID REAL
),
date_range AS (
    -- Encontra a data da primeira e última atividade para gerar a série de dias
    SELECT
        (SELECT user_id FROM target_user) AS user_id,
        MIN(activity_date)::date AS start_date,
        MAX(activity_date)::date AS end_date
    FROM (
        SELECT createdat AS activity_date FROM tweets WHERE author_id IN (SELECT twitter_id FROM authors WHERE telegram_id = (SELECT user_id FROM target_user))
        UNION ALL
        SELECT created_at AS activity_date FROM ambassador_engagements WHERE interacting_user_id IN (SELECT twitter_id FROM authors WHERE telegram_id = (SELECT user_id FROM target_user))
        UNION ALL
        SELECT activity_date AS activity_date FROM user_activity WHERE user_id = (SELECT user_id FROM target_user)
        UNION ALL
        SELECT created_at AS activity_date FROM manual_contributions WHERE user_id = (SELECT user_id FROM target_user)
    ) AS all_activities
),
date_series AS (
    -- Gera uma linha para cada dia no intervalo de atividade do usuário
    SELECT
        generate_series(
            (SELECT start_date FROM date_range),
            (SELECT end_date FROM date_range),
            '1 day'::interval
        )::date AS "date",
        (SELECT user_id FROM date_range) AS user_id
),

-- =========================================================================================
-- ETAPA 2: CALCULAR OS PONTOS GANHOS EM CADA DIA (NÃO ACUMULADO)
-- =========================================================================================
daily_points AS (
    SELECT
        d.date,
        d.user_id,
        a.twitter_id,
        a.telegram_name,
        a.twitter_username,
        -- Pontos de Tweets no dia
        COALESCE(SUM(
            CASE
                WHEN t.is_thread THEN 10
                WHEN t.content_type = 'image' THEN 10
                WHEN t.content_type = 'video' THEN 12
                WHEN t.content_type = 'text_only' THEN 6
                ELSE 0
            END *
            CASE WHEN t.views >= 1000 THEN 2 ELSE 1 END
        ), 0) AS daily_tweet_score,
        -- Pontos de Engajamentos feitos no dia
        COALESCE(SUM(e.points_awarded), 0) AS daily_engagement_score,
        -- Pontos de Atividade no Telegram no dia
        COALESCE(SUM(ua.total_day_score), 0) AS daily_telegram_score,
        -- Pontos de Contribuições Manuais no dia
        COALESCE(SUM(mc.points_awarded), 0) AS daily_manual_score
    FROM date_series d
    JOIN authors a ON a.telegram_id = d.user_id
    -- Join com Tweets
    LEFT JOIN tweets t ON t.author_id = a.twitter_id AND t.createdat::date = d.date
    -- Join com Engajamentos
    LEFT JOIN ambassador_engagements e ON e.interacting_user_id = a.twitter_id AND e.created_at::date = d.date
    -- Join com Atividade do Telegram
    LEFT JOIN user_activity ua ON ua.user_id = a.telegram_id AND ua.activity_date = d.date
    -- Join com Contribuições Manuais
    LEFT JOIN manual_contributions mc ON mc.user_id = a.telegram_id AND mc.created_at::date = d.date
    GROUP BY d.date, d.user_id, a.twitter_id, a.telegram_name, a.twitter_username
)

-- =========================================================================================
-- ETAPA 3: CALCULAR OS SCORES ACUMULADOS USANDO WINDOW FUNCTIONS
-- =========================================================================================
SELECT
    p.date AS snapshot_timestamp,
    p.user_id,
    p.telegram_name,
    p.twitter_username,
    -- Scores Acumulados
    SUM(p.daily_tweet_score) OVER (PARTITION BY p.user_id ORDER BY p.date) AS total_score_from_tweets,
    SUM(p.daily_engagement_score) OVER (PARTITION BY p.user_id ORDER BY p.date) AS total_score_from_engagements,
    SUM(p.daily_telegram_score) OVER (PARTITION BY p.user_id ORDER BY p.date) AS total_score_from_telegram,
    SUM(p.daily_manual_score) OVER (PARTITION BY p.user_id ORDER BY p.date) AS total_score_from_contributions,
    -- Score Total Acumulado
    (
        SUM(p.daily_tweet_score) OVER (PARTITION BY p.user_id ORDER BY p.date) +
        SUM(p.daily_engagement_score) OVER (PARTITION BY p.user_id ORDER BY p.date) +
        SUM(p.daily_telegram_score) OVER (PARTITION BY p.user_id ORDER BY p.date) +
        SUM(p.daily_manual_score) OVER (PARTITION BY p.user_id ORDER BY p.date)
    ) AS grand_total_score
FROM daily_points p
ORDER BY p.date; 