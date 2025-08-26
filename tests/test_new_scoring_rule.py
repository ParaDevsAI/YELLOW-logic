import asyncio
import os
import logging
from dotenv import load_dotenv
from collections import defaultdict

# Importando o cliente Supabase centralizado
from bot.author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def get_test_authors(limit=3):
    """Busca uma lista de IDs de autores para o teste."""
    supabase = await get_supabase_client()
    if not supabase:
        return []
    try:
        # Garante que o usuário super ativo esteja na nossa lista de testes
        power_user_id = '799992467332407296'
        
        response = await asyncio.to_thread(
            supabase.table('authors')
            .select('twitter_id')
            .neq('twitter_id', power_user_id) # Pega outros que não sejam ele
            .limit(limit - 1)
            .execute
        )
        
        if response.data:
            other_authors = [item['twitter_id'] for item in response.data]
            return [power_user_id] + other_authors
        return [power_user_id] # Retorna pelo menos ele se não encontrar outros
    except Exception as e:
        logger.error(f"Erro ao buscar autores para teste: {e}")
        return []

async def test_scoring_rules():
    """
    Executa um teste 'dry run' da nova regra de pontuação para um grupo de autores.
    Não modifica nenhum dado no banco.
    """
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Não foi possível conectar ao Supabase. Abortando.")
        return

    test_author_ids = await get_test_authors()
    if not test_author_ids:
        logger.error("Nenhum autor encontrado para o teste.")
        return
        
    logger.info(f"--- Iniciando teste de pontuação para os autores: {test_author_ids} ---")

    # 1. Buscar todas as interações para os autores de teste
    try:
        response = await asyncio.to_thread(
            supabase.table('ambassador_engagements')
            .select('interacting_user_id, tweet_id, points_awarded, action_type')
            .in_('interacting_user_id', test_author_ids)
            .execute
        )
        engagements = response.data if response.data else []
    except Exception as e:
        logger.error(f"Erro ao buscar engajamentos para o teste: {e}")
        return

    if not engagements:
        logger.warning("Nenhuma interação encontrada para os autores selecionados.")
        return

    # 2. Calcular pontuações
    old_scores = defaultdict(int)
    # Estrutura para calcular a nova pontuação: {author_id: {tweet_id: set_of_actions}}
    new_scores_preparation = defaultdict(lambda: defaultdict(set))

    for eng in engagements:
        author_id = eng['interacting_user_id']
        tweet_id = eng['tweet_id']
        points = eng['points_awarded']
        action = eng['action_type']

        # Calcular pontuação antiga (simples soma)
        old_scores[author_id] += points

        # Preparar para o cálculo da nova pontuação
        # A ação aqui pode ser 'reply' ou 'retweet_or_quote'
        new_scores_preparation[author_id][tweet_id].add(action)

    # Calcular a nova pontuação final
    new_scores = defaultdict(int)
    for author_id, tweets in new_scores_preparation.items():
        author_total_score = 0
        for tweet_id, actions in tweets.items():
            # A nova regra: 2 pontos por tweet engajado, não importa como.
            # Se houver qualquer ação (o set não estará vazio), some 2 pontos.
            if actions:
                author_total_score += 2
        new_scores[author_id] = author_total_score

    # 3. Exibir o relatório de comparação
    print("\n" + "="*80)
    print("                Resultados do Teste de Regra de Pontuação (Dry Run)")
    print("  Este script NÃO MODIFICOU nenhum dado. É apenas uma simulação.")
    print("="*80)
    
    for author_id in test_author_ids:
        old_score = old_scores.get(author_id, 0)
        new_score = new_scores.get(author_id, 0)
        reduction = old_score - new_score
        reduction_percent = (reduction / old_score * 100) if old_score > 0 else 0

        print(f"\n--- Autor: {author_id} ---")
        print(f"  - Pontuação com a REGRA ANTIGA: {old_score}")
        print(f"  - Pontuação com a NOVA REGRA (Máx 2 pts/tweet): {new_score}")
        print(f"  - Redução de Pontos: {reduction} ({reduction_percent:.2f}%)")
        
    print("\n" + "="*80)


async def main():
    load_dotenv()
    await test_scoring_rules()

if __name__ == "__main__":
    asyncio.run(main()) 