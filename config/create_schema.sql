-- =============================================
--      YELLOW AMBASSADOR ENGAGEMENT TRACKER
--          DATABASE SCHEMA SCRIPT
-- =============================================
--
-- Instruções:
-- 1. Navegue até o "SQL Editor" no seu projeto Supabase.
-- 2. Clique em "+ New query".
-- 3. Cole todo o conteúdo deste script na janela de consulta.
-- 4. Clique em "RUN".
--
-- Este script irá criar todas as tabelas, chaves e relações necessárias.

-- Apagar tabelas existentes (opcional, para um reset completo)
-- CUIDADO: Use somente se tiver certeza que quer apagar TUDO.
/*
DROP TABLE IF EXISTS leaderboard_history CASCADE;
DROP TABLE IF EXISTS leaderboard CASCADE;
DROP TABLE IF EXISTS manual_contributions CASCADE;
DROP TABLE IF EXISTS ambassador_engagements CASCADE;
DROP TABLE IF EXISTS user_activity CASCADE;
DROP TABLE IF EXISTS tweet_metrics_history CASCADE;
DROP TABLE IF EXISTS tweet_entities CASCADE;
DROP TABLE IF EXISTS tweets CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
*/

-- Tabela 1: authors
-- Armazena os dados dos embaixadores, servindo como a tabela central do sistema.
CREATE TABLE IF NOT EXISTS authors (
    telegram_id BIGINT PRIMARY KEY,
    telegram_name TEXT,
    twitter_id TEXT UNIQUE NOT NULL,
    twitter_username TEXT,
    twitter_name TEXT,
    twitter_description TEXT,
    twitter_followers INT,
    twitter_following INT,
    twitter_statusescount INT,
    twitter_mediacount INT,
    twitter_createdat TIMESTAMPTZ,
    twitter_isblueverified BOOLEAN,
    twitter_profilepicture TEXT,
    sync_timestamp TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'utc')
);
COMMENT ON TABLE authors IS 'Tabela central que armazena os perfis dos embaixadores, linkando Telegram e Twitter.';

-- Tabela 2: tweets
-- Armazena os dados de cada tweet postado por um embaixador.
CREATE TABLE IF NOT EXISTS tweets (
    tweet_id TEXT PRIMARY KEY,
    author_id TEXT NOT NULL REFERENCES authors(twitter_id) ON DELETE CASCADE,
    twitter_url TEXT,
    text TEXT,
    createdat TIMESTAMPTZ,
    views INT,
    likes INT,
    retweets INT,
    replies INT,
    quotes INT,
    bookmarks INT,
    content_type TEXT,
    media_url TEXT,
    is_thread BOOLEAN,
    is_thread_checked BOOLEAN DEFAULT FALSE
);
COMMENT ON TABLE tweets IS 'Registra cada tweet postado por um embaixador. A pontuação é baseada no tipo de conteúdo e um bônus por visualizações.';
COMMENT ON COLUMN tweets.content_type IS 'Tipo de conteúdo para pontuação base: text_only (6 pts), image (10 pts), video (12 pts).';
COMMENT ON COLUMN tweets.is_thread IS 'Se TRUE, conta como tipo "image" para pontuação (10 pts). Verificado via API.';
COMMENT ON COLUMN tweets.views IS 'Se >= 1000, a pontuação base do tweet é multiplicada por 2.';
COMMENT ON COLUMN tweets.is_thread_checked IS 'Indica se o script thread_identifier.py já processou este tweet.';
CREATE INDEX IF NOT EXISTS idx_tweets_author_id ON tweets(author_id);

-- Tabela 3: tweet_entities
-- Detalha as entidades (menções, hashtags, links) dentro de um tweet.
CREATE TABLE IF NOT EXISTS tweet_entities (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    tweet_id TEXT NOT NULL REFERENCES tweets(tweet_id) ON DELETE CASCADE,
    entity_type TEXT NOT NULL, -- 'user_mention', 'hashtag', 'url'
    text_in_tweet TEXT,
    mentioned_user_id TEXT,
    expanded_url TEXT,
    UNIQUE(tweet_id, entity_type, text_in_tweet)
);
COMMENT ON TABLE tweet_entities IS 'Armazena as entidades (menções, hashtags, URLs) de cada tweet.';
CREATE INDEX IF NOT EXISTS idx_tweet_entities_tweet_id ON tweet_entities(tweet_id);

-- Tabela 4: tweet_metrics_history
-- Cria um "snapshot" periódico das métricas de um tweet para análise de evolução.
CREATE TABLE IF NOT EXISTS tweet_metrics_history (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    tweet_id TEXT NOT NULL REFERENCES tweets(tweet_id) ON DELETE CASCADE,
    snapshot_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'utc'),
    views INT,
    likes INT,
    retweets INT,
    replies INT,
    quotes INT,
    bookmarks INT
);
COMMENT ON TABLE tweet_metrics_history IS 'Snapshots periódicos das métricas de um tweet para análise histórica.';
CREATE INDEX IF NOT EXISTS idx_tweet_metrics_history_tweet_id ON tweet_metrics_history(tweet_id);

-- Tabela 5: user_activity
-- Rastreia a pontuação de atividade dos embaixadores no grupo do Telegram.
CREATE TABLE IF NOT EXISTS user_activity (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    user_id BIGINT NOT NULL REFERENCES authors(telegram_id) ON DELETE CASCADE,
    activity_date DATE NOT NULL,
    total_day_score DOUBLE PRECISION,
    intervals_details_json JSONB,
    UNIQUE(user_id, activity_date)
);
COMMENT ON TABLE user_activity IS 'Pontuação da atividade de mensagens de um embaixador no Telegram, por dia.';
CREATE INDEX IF NOT EXISTS idx_user_activity_user_id ON user_activity(user_id);

-- Tabela 6: ambassador_engagements
-- Registra interações (respostas, retweets) entre embaixadores.
CREATE TABLE IF NOT EXISTS ambassador_engagements (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    tweet_id TEXT NOT NULL REFERENCES tweets(tweet_id) ON DELETE CASCADE,
    tweet_author_id TEXT NOT NULL REFERENCES authors(twitter_id) ON DELETE NO ACTION,
    interacting_user_id TEXT NOT NULL REFERENCES authors(twitter_id) ON DELETE NO ACTION,
    action_type TEXT NOT NULL, -- 'reply' (2 pts), 'retweet_or_quote' (2 pts)
    points_awarded INT,
    created_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'utc'),
    UNIQUE(tweet_id, interacting_user_id, action_type)
);
COMMENT ON TABLE ambassador_engagements IS 'Registra interações entre embaixadores. Retweet (2 pts) e Comentário (2 pts).';
CREATE INDEX IF NOT EXISTS idx_ambassador_engagements_tweet_id ON ambassador_engagements(tweet_id);

-- Tabela 7: manual_contributions
-- Permite que administradores adicionem pontos manualmente por contribuições especiais.
CREATE TABLE IF NOT EXISTS manual_contributions (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    user_id BIGINT NOT NULL REFERENCES authors(telegram_id) ON DELETE CASCADE,
    contribution_type TEXT NOT NULL,
    points_awarded INT,
    description TEXT,
    recorded_by TEXT, -- Pode ser o telegram_id ou nome do admin
    created_at TIMESTAMPTZ DEFAULT (now() AT TIME ZONE 'utc')
);
COMMENT ON TABLE manual_contributions IS 'Registros de pontos manuais baseados em impacto. Ex: Introdução a parceiros (100+ pts), Feedback de produto (10+ pts), Recrutamento de embaixador (20-50 pts).';
CREATE INDEX IF NOT EXISTS idx_manual_contributions_user_id ON manual_contributions(user_id);

-- Tabela 8: leaderboard
-- Armazena o placar em tempo real com a pontuação geral.
CREATE TABLE IF NOT EXISTS leaderboard (
    user_id BIGINT PRIMARY KEY REFERENCES authors(telegram_id) ON DELETE CASCADE,
    last_updated TIMESTAMPTZ,
    rank INT,
    telegram_name TEXT,
    twitter_username TEXT,
    count_tweets_text_only INT,
    count_tweets_image INT,
    count_tweets_thread INT,
    count_tweets_video INT,
    total_score_from_tweets DOUBLE PRECISION,
    count_retweets_made INT,
    count_comments_made INT,
    total_score_from_engagements DOUBLE PRECISION,
    total_score_from_telegram DOUBLE PRECISION,
    count_partner_introduction INT,
    count_hosting_ama INT,
    count_recruitment_ambassador INT,
    count_product_feedback INT,
    count_recruitment_investor INT,
    total_score_from_contributions DOUBLE PRECISION,
    grand_total_score DOUBLE PRECISION
);
COMMENT ON TABLE leaderboard IS 'Placar em tempo real com a pontuação consolidada de cada embaixador.';

-- Tabela 9: leaderboard_history
-- Salva um histórico do placar a cada vez que ele é gerado.
CREATE TABLE IF NOT EXISTS leaderboard_history (
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    snapshot_timestamp TIMESTAMPTZ,
    user_id BIGINT NOT NULL REFERENCES authors(telegram_id) ON DELETE CASCADE,
    rank INT,
    telegram_name TEXT,
    twitter_username TEXT,
    count_tweets_text_only INT,
    count_tweets_image INT,
    count_tweets_thread INT,
    count_tweets_video INT,
    total_score_from_tweets DOUBLE PRECISION,
    count_retweets_made INT,
    count_comments_made INT,
    total_score_from_engagements DOUBLE PRECISION,
    total_score_from_telegram DOUBLE PRECISION,
    count_partner_introduction INT,
    count_hosting_ama INT,
    count_recruitment_ambassador INT,
    count_product_feedback INT,
    count_recruitment_investor INT,
    total_score_from_contributions DOUBLE PRECISION,
    grand_total_score DOUBLE PRECISION
);
COMMENT ON TABLE leaderboard_history IS 'Snapshots históricos do placar para análise de performance ao longo do tempo.';
CREATE INDEX IF NOT EXISTS idx_leaderboard_history_snapshot_timestamp ON leaderboard_history(snapshot_timestamp);
CREATE INDEX IF NOT EXISTS idx_leaderboard_history_user_id ON leaderboard_history(user_id);

-- =============================================
--      DATABASE FUNCTIONS (RPCs)
-- =============================================

-- Main function to calculate all scores and generate the leaderboard data.
CREATE OR REPLACE FUNCTION calculate_leaderboard()
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
    -- Step 1: Calculate tweet scores for each author
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
        GROUP BY t.author_id
    ),
    -- Step 2: Calculate engagement scores for each author
    engagement_scores AS (
        SELECT
            ae.interacting_user_id AS author_id,
            SUM(ae.points_awarded) AS score,
            COUNT(CASE WHEN ae.action_type = 'retweet_or_quote' THEN 1 END) AS retweets_count,
            COUNT(CASE WHEN ae.action_type = 'reply' THEN 1 END) AS comments_count
        FROM ambassador_engagements ae
        GROUP BY ae.interacting_user_id
    ),
    -- Step 3: Calculate Telegram activity scores
    telegram_scores AS (
        SELECT
            ua.user_id AS telegram_id,
            SUM(ua.total_day_score) AS score
        FROM user_activity ua
        GROUP BY ua.user_id
    ),
    -- Step 4: Calculate manual contribution scores and counts
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
        COALESCE(ts.score, 0),
        COALESCE(es.retweets_count, 0),
        COALESCE(es.comments_count, 0),
        COALESCE(es.score, 0),
        COALESCE(tgs.score, 0),
        COALESCE(cs.recruitment_ambassador_count, 0),
        COALESCE(cs.recruitment_investor_count, 0),
        COALESCE(cs.hosting_ama_count, 0),
        COALESCE(cs.partner_introduction_count, 0),
        COALESCE(cs.product_feedback_count, 0),
        COALESCE(cs.score, 0),
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


-- Function to update ranks in the main leaderboard table
CREATE OR REPLACE FUNCTION update_leaderboard_ranks()
RETURNS void AS $$
BEGIN
    WITH ranked_users AS (
        SELECT
            user_id,
            RANK() OVER (ORDER BY grand_total_score DESC, telegram_name ASC) as new_rank
        FROM leaderboard
    )
    UPDATE leaderboard l
    SET rank = ru.new_rank
    FROM ranked_users ru
    WHERE l.user_id = ru.user_id;
END;
$$ LANGUAGE plpgsql;


-- Function to update ranks in the history table for a specific snapshot
CREATE OR REPLACE FUNCTION update_leaderboard_history_ranks(snapshot_ts timestamptz)
RETURNS void AS $$
BEGIN
    WITH ranked_history AS (
        SELECT
            id,
            RANK() OVER (ORDER BY grand_total_score DESC, telegram_name ASC) as new_rank
        FROM leaderboard_history
        WHERE snapshot_timestamp = snapshot_ts
    )
    UPDATE leaderboard_history lh
    SET rank = rh.new_rank
    FROM ranked_history rh
    WHERE lh.id = rh.id;
END;
$$ LANGUAGE plpgsql;

-- Fim do script
SELECT 'Schema creation script executed successfully.'; 