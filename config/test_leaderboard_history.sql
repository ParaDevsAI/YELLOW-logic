-- =============================================
--      FUNÇÃO DE TESTE PARA LEADERBOARD HISTÓRICO
-- =============================================
-- Esta função é temporária e serve apenas para simular o cálculo
-- do leaderboard em uma data específica do passado.
-- Ela NÃO será usada em produção e pode ser removida após o teste.

CREATE OR REPLACE FUNCTION calculate_leaderboard_for_date(target_date TIMESTAMPTZ)
RETURNS TABLE(
    telegram_id BIGINT,
    telegram_name TEXT,
    twitter_username TEXT,
    count_tweets_text_only BIGINT,
    count_tweets_image BIGINT,
    count_tweets_thread BIGINT,
    count_tweets_video BIGINT,
    total_score_from_tweets NUMERIC,
    count_retweets_made BIGINT,
    count_comments_made BIGINT,
    total_score_from_engagements NUMERIC,
    total_score_from_telegram NUMERIC,
    count_recruitment_ambassador BIGINT,
    count_recruitment_investor BIGINT,
    count_hosting_ama BIGINT,
    count_partner_introduction BIGINT,
    count_product_feedback BIGINT,
    total_score_from_contributions NUMERIC,
    grand_total_score NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH
    -- Step 1: Calculate tweet scores for each author up to the target date
    tweet_scores AS (
        SELECT
            t.author_id,
            SUM(
                CASE
                    WHEN t.is_thread THEN 10
                    WHEN t.content_type = 'image' THEN 10
                    WHEN t.content_type = 'video' THEN 12
                    WHEN t.content_type = 'text_only' THEN 6
                    ELSE 0
                END *
                CASE
                    WHEN t.views >= 1000 THEN 2
                    ELSE 1
                END
            ) AS score,
            COUNT(CASE WHEN t.content_type = 'text_only' AND COALESCE(t.is_thread, false) = false THEN 1 END) AS text_only_count,
            COUNT(CASE WHEN t.content_type = 'image' AND COALESCE(t.is_thread, false) = false THEN 1 END) AS image_count,
            COUNT(CASE WHEN t.is_thread = true THEN 1 END) AS thread_count,
            COUNT(CASE WHEN t.content_type = 'video' AND COALESCE(t.is_thread, false) = false THEN 1 END) AS video_count
        FROM tweets t
        WHERE t.createdat <= target_date
        GROUP BY t.author_id
    ),
    -- Step 2: Calculate engagement scores for each author up to the target date
    engagement_scores AS (
        SELECT
            ae.interacting_user_id AS author_id,
            SUM(ae.points_awarded) AS score,
            COUNT(CASE WHEN ae.action_type = 'retweet_or_quote' THEN 1 END) AS retweets_count,
            COUNT(CASE WHEN ae.action_type IN ('reply', 'comment') THEN 1 END) AS comments_count
        FROM ambassador_engagements ae
        WHERE ae.created_at <= target_date
        GROUP BY ae.interacting_user_id
    ),
    -- Step 3: Calculate Telegram activity scores up to the target date
    telegram_scores AS (
        SELECT
            ua.user_id AS telegram_id,
            SUM(ua.total_day_score) AS score
        FROM user_activity ua
        WHERE ua.activity_date <= target_date::date
        GROUP BY ua.user_id
    ),
    -- Step 4: Calculate manual contribution scores and counts up to the target date
    contribution_scores AS (
        SELECT
            mc.user_id AS telegram_id,
            SUM(mc.points_awarded) AS score,
            COUNT(CASE WHEN mc.contribution_type = 'recruitment_ambassador' THEN 1 END) AS recruitment_ambassador_count,
            COUNT(CASE WHEN mc.contribution_type = 'recruitment_investor' THEN 1 END) AS recruitment_investor_count,
            COUNT(CASE WHEN mc.contribution_type = 'hosting_ama' THEN 1 END) AS hosting_ama_count,
            COUNT(CASE WHEN mc.contribution_type = 'partner_introduction' THEN 1 END) AS partner_introduction_count,
            COUNT(CASE WHEN mc.contribution_type = 'product_feedback' THEN 1 END) AS product_feedback_count
        FROM manual_contributions mc
        WHERE mc.created_at <= target_date
        GROUP BY mc.user_id
    )
    -- Final Step: Join all scores and calculate grand total
    SELECT
        a.telegram_id,
        a.telegram_name,
        a.twitter_username,
        COALESCE(ts.text_only_count, 0),
        COALESCE(ts.image_count, 0),
        COALESCE(ts.thread_count, 0),
        COALESCE(ts.video_count, 0),
        COALESCE(ts.score, 0)::NUMERIC,
        COALESCE(es.retweets_count, 0),
        COALESCE(es.comments_count, 0),
        COALESCE(es.score, 0)::NUMERIC,
        COALESCE(tgs.score, 0)::NUMERIC,
        COALESCE(cs.recruitment_ambassador_count, 0),
        COALESCE(cs.recruitment_investor_count, 0),
        COALESCE(cs.hosting_ama_count, 0),
        COALESCE(cs.partner_introduction_count, 0),
        COALESCE(cs.product_feedback_count, 0),
        COALESCE(cs.score, 0)::NUMERIC,
        (
            COALESCE(ts.score, 0) +
            COALESCE(es.score, 0) +
            COALESCE(tgs.score, 0) +
            COALESCE(cs.score, 0)
        )::NUMERIC AS grand_total_score
    FROM
        authors a
    LEFT JOIN tweet_scores ts ON a.twitter_id = ts.author_id
    LEFT JOIN engagement_scores es ON a.twitter_id = es.author_id
    LEFT JOIN telegram_scores tgs ON a.telegram_id = tgs.telegram_id
    LEFT JOIN contribution_scores cs ON a.telegram_id = cs.telegram_id;
END;
$$ LANGUAGE plpgsql;

SELECT 'Função calculate_leaderboard_for_date(target_date) criada com sucesso para o teste.'; 