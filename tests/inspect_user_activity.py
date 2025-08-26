import asyncio
import os
import logging
from dotenv import load_dotenv

# Importando o cliente Supabase centralizado
from bot.author_manager import get_supabase_client

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- ID do Usuário a ser Investigado ---
# Este é o ID que aparece com frequência no arquivo CSV fornecido.
USER_ID_TO_INSPECT = '799992467332407296'

async def investigate_user():
    """
    Executa uma análise detalhada da atividade de um usuário específico.
    """
    logger.info(f"--- Iniciando investigação para o usuário: {USER_ID_TO_INSPECT} ---")
    
    supabase = await get_supabase_client()
    if not supabase:
        logger.critical("Não foi possível conectar ao Supabase. Abortando.")
        return

    # 1. Verificar se o usuário é um embaixador registrado
    try:
        response = await asyncio.to_thread(
            supabase.table('authors')
            .select('id')
            .eq('twitter_id', USER_ID_TO_INSPECT)
            .execute
        )
        is_ambassador = True if response.data else False
    except Exception as e:
        logger.error(f"Erro ao verificar se o usuário é um embaixador: {e}")
        is_ambassador = "Erro na verificação"

    # 2. Buscar todas as interações do usuário na tabela de engajamentos com paginação
    all_engagements = []
    page = 0
    page_size = 1000  # Limite padrão do Supabase
    while True:
        try:
            start_index = page * page_size
            end_index = start_index + page_size - 1
            
            response = await asyncio.to_thread(
                supabase.table('ambassador_engagements')
                .select('action_type, points_awarded, tweet_id, created_at')
                .eq('interacting_user_id', USER_ID_TO_INSPECT)
                .order('created_at', desc=True) # Ordenar para consistência
                .range(start_index, end_index)
                .execute
            )
            
            if response.data:
                all_engagements.extend(response.data)
                # Se o número de resultados for menor que o tamanho da página, esta é a última página.
                if len(response.data) < page_size:
                    break
                page += 1
                logger.info(f"Coletando dados... {len(all_engagements)} registros encontrados até agora.")
            else:
                break # Sai do loop se não houver mais dados
        except Exception as e:
            logger.error(f"Erro ao buscar engajamentos do usuário (página {page}): {e}")
            break

    # 3. Analisar e agregar os dados
    total_interactions = len(all_engagements)
    total_points = 0
    action_counts = {}
    
    # Análise detalhada por tweet
    tweets_analysis = {} # Key: tweet_id, Value: {actions: set, points: int}

    for eng in all_engagements:
        total_points += eng.get('points_awarded', 0)
        action = eng.get('action_type')
        tweet_id = eng.get('tweet_id')

        if action:
            normalized_action = 'reply/comment' if action in ['reply', 'comment'] else action
            action_counts[normalized_action] = action_counts.get(normalized_action, 0) + 1
        
        if tweet_id:
            if tweet_id not in tweets_analysis:
                tweets_analysis[tweet_id] = {'actions': set(), 'points': 0}
            tweets_analysis[tweet_id]['actions'].add(action)
            tweets_analysis[tweet_id]['points'] += eng.get('points_awarded', 0)

    # Filtrar tweets com 2 interações
    tweets_with_2_interactions = {
        tid: data for tid, data in tweets_analysis.items() if len(data['actions']) > 1
    }

    # Verificar duplicatas exatas
    # Criamos uma representação de string para cada linha para fácil comparação
    seen_records = set()
    duplicates = []
    for eng in all_engagements:
        record_tuple = (eng.get('tweet_id'), eng.get('action_type'), eng.get('interacting_user_id'))
        if record_tuple in seen_records:
            duplicates.append(record_tuple)
        else:
            seen_records.add(record_tuple)

    # 4. Exibir os resultados
    print("\n" + "="*60)
    print(f"  Resultados da Investigação DETALHADA para o Usuário ID: {USER_ID_TO_INSPECT}")
    print("="*60)
    print(f"  -> É um embaixador registrado? {'Sim' if is_ambassador else 'Não'}")
    print(f"\n  --- Análise Geral (dados completos) ---")
    print(f"  -> Total de Interações Registradas: {total_interactions}")
    print(f"  -> Total de Pontos Acumulados: {total_points}")
    print(f"  -> Total de Tweets Únicos Interagidos: {len(tweets_analysis)}")

    print("\n  --- Detalhamento das Ações ---")
    if not action_counts:
        print("    - Nenhuma ação encontrada.")
    for action, count in action_counts.items():
        print(f"    - Tipo '{action}': {count} vezes")

    print("\n  --- Verificação de Duplicatas Exatas ---")
    if not duplicates:
        print("  -> Nenhuma duplicata exata (mesmo tweet, mesma ação) encontrada. O sistema está consistente.")
    else:
        print(f"  -> ATENÇÃO: Encontradas {len(duplicates)} duplicatas exatas! Exemplo: {duplicates[0]}")

    print("\n  --- Análise de Múltiplas Ações no Mesmo Tweet ---")
    print(f"  -> O usuário realizou mais de uma ação (e.g., reply + retweet) em {len(tweets_with_2_interactions)} tweets diferentes.")
    if tweets_with_2_interactions:
        print("  -> Exemplo de 5 tweets onde isso ocorreu:")
        for i, (tid, data) in enumerate(list(tweets_with_2_interactions.items())[:5]):
            print(f"    - Tweet ID: {tid} | Ações: {data['actions']} | Pontos: {data['points']}")
            
    print("\n  --- Lista de Todos os Tweets Interagidos ---")
    if not tweets_analysis:
        print("    - Nenhum tweet encontrado.")
    else:
        print(f"  (Exibindo os primeiros 10 de {len(tweets_analysis)} tweets únicos)")
        for i, tid in enumerate(list(tweets_analysis.keys())[:10]):
            print(f"    - {tid}")

    print("="*60 + "\n")

async def main():
    load_dotenv()
    await investigate_user()

if __name__ == "__main__":
    asyncio.run(main()) 