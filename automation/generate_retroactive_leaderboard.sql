-- =============================================
--      SCRIPT DE GERAÇÃO - LEADERBOARD RETROATIVO (VERSÃO CORRIGIDA)
-- =============================================
--
-- Objetivo:
-- Este script limpa a tabela leaderboard_history e a repopula com um
-- histórico completo, diário e CORRETO da pontuação e classificação de TODOS os usuários.
--
-- Lógica da Correção:
-- A versão anterior multiplicava os dados ao juntar várias tabelas de uma vez.
-- Esta versão corrige isso calculando os totais de cada fonte (tweets, engajamentos, etc.)
-- em passos separados (CTEs) antes de juntá-los no final. Isso garante que cada
-- ponto e cada item seja contado apenas uma vez.
--
-- AVISO:
-- Este script executa um TRUNCATE, que apaga TODOS os dados da tabela
-- leaderboard_history antes de inserir os novos.

-- Limpa a tabela para a nova população.
TRUNCATE TABLE leaderboard_history;

-- Inicia a inserção na tabela de histórico
INSERT INTO leaderboard_history (
    snapshot_timestamp,
    user_id,
    rank,
    telegram_name,
    twitter_username,
    count_tweets_text_only,
    count_tweets_image,
    count_tweets_thread,
    count_tweets_video,
    total_score_from_tweets,
    count_retweets_made,
    count_comments_made,
    total_score_from_engagements,
    total_score_from_telegram,
    count_partner_introduction,
    count_hosting_ama,
    count_recruitment_ambassador,
    count_product_feedback,
    count_recruitment_investor,
    total_score_from_contributions,
    grand_total_score
)
WITH
-- =========================================================================================
-- ETAPA 1: Gerar uma linha do tempo completa e uma lista de todos os usuários para cada dia.
-- =========================================================================================
date_range AS (
    SELECT
        MIN(activity_date)::date AS start_date,
        (now() AT TIME ZONE 'utc')::date AS end_date
    FROM (
        SELECT createdat AS activity_date FROM tweets
        UNION ALL SELECT created_at AS activity_date FROM ambassador_engagements
        UNION ALL SELECT activity_date AS activity_date FROM user_activity
        UNION ALL SELECT created_at AS activity_date FROM manual_contributions
    ) AS all_activities
),
date_series AS (
    SELECT generate_series(start_date, end_date, '1 day'::interval)::date AS "date"
    FROM date_range
),
all_users_all_dates AS (
    SELECT d.date, a.telegram_id, a.twitter_id, a.telegram_name, a.twitter_username
    FROM date_series d CROSS JOIN authors a
),

-- =========================================================================================
-- ETAPA 2: Calcular métricas DIÁRIAS de cada fonte de forma ISOLADA.
-- =========================================================================================
daily_tweets AS (
    SELECT
        createdat::date AS "date",
        author_id,
        SUM(CASE WHEN (content_type IS NULL OR content_type = '') AND COALESCE(is_thread, false) = false THEN 1 ELSE 0 END) AS tweets_text_only,
        SUM(CASE WHEN content_type = 'photo' AND COALESCE(is_thread, false) = false THEN 1 ELSE 0 END) AS tweets_image,
        SUM(CASE WHEN is_thread = true THEN 1 ELSE 0 END) AS tweets_thread,
        SUM(CASE WHEN content_type = 'video' THEN 1 ELSE 0 END) AS tweets_video,
        SUM(
            CASE
                WHEN is_thread THEN 10
                WHEN content_type = 'photo' THEN 10
                WHEN content_type = 'video' THEN 12
                WHEN (content_type IS NULL OR content_type = '') THEN 6
                ELSE 0
            END * 
            CASE WHEN views >= 1000 THEN 2 ELSE 1 END
        ) AS score_from_tweets
    FROM tweets
    GROUP BY createdat::date, author_id
),
daily_engagements AS (
    SELECT
        created_at::date AS "date",
        interacting_user_id,
        SUM(CASE WHEN action_type = 'retweet_or_quote' THEN 1 ELSE 0 END) AS retweets_made,
        SUM(CASE WHEN action_type = 'reply' THEN 1 ELSE 0 END) AS comments_made,
        SUM(points_awarded) AS score_from_engagements
    FROM ambassador_engagements
    GROUP BY created_at::date, interacting_user_id
),
daily_telegram AS (
    SELECT
        activity_date AS "date",
        user_id,
        total_day_score AS score_from_telegram
    FROM user_activity
    GROUP BY activity_date, user_id, total_day_score
),
daily_contributions AS (
    SELECT
        created_at::date as "date",
        user_id,
        SUM(CASE WHEN contribution_type = 'partner_introduction' THEN 1 ELSE 0 END) AS partner_introduction,
        SUM(CASE WHEN contribution_type = 'hosting_ama' THEN 1 ELSE 0 END) AS hosting_ama,
        SUM(CASE WHEN contribution_type = 'recruitment_ambassador' THEN 1 ELSE 0 END) AS recruitment_ambassador,
        SUM(CASE WHEN contribution_type = 'product_feedback' THEN 1 ELSE 0 END) AS product_feedback,
        SUM(CASE WHEN contribution_type = 'recruitment_investor' THEN 1 ELSE 0 END) AS recruitment_investor,
        SUM(points_awarded) AS score_from_contributions
    FROM manual_contributions
    GROUP BY created_at::date, user_id
),

-- =========================================================================================
-- ETAPA 3: Juntar as métricas diárias já agregadas.
-- =========================================================================================
daily_combined_metrics AS (
    SELECT
        d.date,
        d.telegram_id,
        d.telegram_name,
        d.twitter_username,
        COALESCE(dt.tweets_text_only, 0) AS daily_tweets_text_only,
        COALESCE(dt.tweets_image, 0) AS daily_tweets_image,
        COALESCE(dt.tweets_thread, 0) AS daily_tweets_thread,
        COALESCE(dt.tweets_video, 0) AS daily_tweets_video,
        COALESCE(dt.score_from_tweets, 0) AS daily_score_from_tweets,
        COALESCE(de.retweets_made, 0) AS daily_retweets_made,
        COALESCE(de.comments_made, 0) AS daily_comments_made,
        COALESCE(de.score_from_engagements, 0) AS daily_score_from_engagements,
        COALESCE(dgram.score_from_telegram, 0) AS daily_score_from_telegram,
        COALESCE(dc.partner_introduction, 0) AS daily_partner_introduction,
        COALESCE(dc.hosting_ama, 0) AS daily_hosting_ama,
        COALESCE(dc.recruitment_ambassador, 0) AS daily_recruitment_ambassador,
        COALESCE(dc.product_feedback, 0) AS daily_product_feedback,
        COALESCE(dc.recruitment_investor, 0) AS daily_recruitment_investor,
        COALESCE(dc.score_from_contributions, 0) AS daily_score_from_contributions
    FROM all_users_all_dates d
    LEFT JOIN daily_tweets dt ON d.twitter_id = dt.author_id AND d.date = dt.date
    LEFT JOIN daily_engagements de ON d.twitter_id = de.interacting_user_id AND d.date = de.date
    LEFT JOIN daily_telegram dgram ON d.telegram_id = dgram.user_id AND d.date = dgram.date
    LEFT JOIN daily_contributions dc ON d.telegram_id = dc.user_id AND d.date = dc.date
),

-- =========================================================================================
-- ETAPA 4: Calcular os totais ACUMULADOS usando Window Functions.
-- =========================================================================================
cumulative_metrics AS (
    SELECT
        date,
        telegram_id,
        telegram_name,
        twitter_username,
        SUM(daily_tweets_text_only) OVER w AS count_tweets_text_only,
        SUM(daily_tweets_image) OVER w AS count_tweets_image,
        SUM(daily_tweets_thread) OVER w AS count_tweets_thread,
        SUM(daily_tweets_video) OVER w AS count_tweets_video,
        SUM(daily_score_from_tweets) OVER w AS total_score_from_tweets,
        SUM(daily_retweets_made) OVER w AS count_retweets_made,
        SUM(daily_comments_made) OVER w AS count_comments_made,
        SUM(daily_score_from_engagements) OVER w AS total_score_from_engagements,
        SUM(daily_score_from_telegram) OVER w AS total_score_from_telegram,
        SUM(daily_partner_introduction) OVER w AS count_partner_introduction,
        SUM(daily_hosting_ama) OVER w AS count_hosting_ama,
        SUM(daily_recruitment_ambassador) OVER w AS count_recruitment_ambassador,
        SUM(daily_product_feedback) OVER w AS count_product_feedback,
        SUM(daily_recruitment_investor) OVER w AS count_recruitment_investor,
        SUM(daily_score_from_contributions) OVER w AS total_score_from_contributions,
        (
            SUM(daily_score_from_tweets) OVER w +
            SUM(daily_score_from_engagements) OVER w +
            SUM(daily_score_from_telegram) OVER w +
            SUM(daily_score_from_contributions) OVER w
        ) AS grand_total_score
    FROM daily_combined_metrics
    WINDOW w AS (PARTITION BY telegram_id ORDER BY date)
)

-- =========================================================================================
-- ETAPA 5: Calcular o RANK diário e selecionar colunas finais para inserção.
-- =========================================================================================
SELECT
    c.date,
    c.telegram_id,
    RANK() OVER (PARTITION BY c.date ORDER BY c.grand_total_score DESC, c.telegram_name ASC) as rank,
    c.telegram_name,
    c.twitter_username,
    c.count_tweets_text_only,
    c.count_tweets_image,
    c.count_tweets_thread,
    c.count_tweets_video,
    c.total_score_from_tweets,
    c.count_retweets_made,
    c.count_comments_made,
    c.total_score_from_engagements,
    c.total_score_from_telegram,
    c.count_partner_introduction,
    c.count_hosting_ama,
    c.count_recruitment_ambassador,
    c.count_product_feedback,
    c.count_recruitment_investor,
    c.total_score_from_contributions,
    c.grand_total_score
FROM cumulative_metrics c
ORDER BY c.date, rank; 