import asyncio
import logging
from pathlib import Path
import sys

# --- Configuração de Caminho e Logging ---
# Garante que o script possa encontrar o 'core.database_client'
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))
from core.database_client import get_db_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def get_user_score_details(user_name: str):
    """
    Busca e exibe um detalhamento completo da pontuação de um usuário específico
    a partir do snapshot mais recente do leaderboard.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Iniciando a análise detalhada para o usuário: '{user_name}'")
    
    try:
        db_client = get_db_client()

        # 1. Obter o user_id (telegram_id) a partir do nome
        author_response = await asyncio.to_thread(
            db_client.table('authors').select('telegram_id').eq('telegram_name', user_name).limit(1).execute
        )
        if not author_response.data:
            logger.error(f"Usuário '{user_name}' não encontrado na tabela 'authors'.")
            return
        user_id = author_response.data[0]['telegram_id']
        logger.info(f"ID do usuário '{user_name}' é: {user_id}")

        # 2. Encontrar a data do snapshot mais recente
        latest_snapshot_response = await asyncio.to_thread(
            db_client.table('leaderboard_history').select('snapshot_timestamp').order('snapshot_timestamp', desc=True).limit(1).execute
        )
        if not latest_snapshot_response.data:
            logger.warning("Nenhum snapshot encontrado na tabela 'leaderboard_history'.")
            return
        latest_snapshot_date = latest_snapshot_response.data[0]['snapshot_timestamp']
        
        # 3. Buscar o registro completo do usuário para essa data
        logger.info(f"Buscando detalhes do leaderboard para o user_id {user_id} no snapshot de {latest_snapshot_date}...")
        details_response = await asyncio.to_thread(
            db_client.table('leaderboard_history').select('*').eq('user_id', user_id).eq('snapshot_timestamp', latest_snapshot_date).single().execute
        )

        if not details_response.data:
            logger.error(f"Nenhum registro de leaderboard encontrado para o usuário '{user_name}' no último snapshot.")
            return

        details = details_response.data
        
        # 4. Exibir o relatório detalhado
        print("\\n" + "="*80)
        print(f"          ANÁLISE DE PONTUAÇÃO DETALHADA PARA: {user_name.upper()}")
        print("="*80)

        # Seção de Tweets
        print("\\n--- Pontuação de Tweets ---")
        print(f"  Tweets (só texto): {details.get('count_tweets_text_only', 0)}")
        print(f"  Tweets (com imagem): {details.get('count_tweets_image', 0)}")
        print(f"  Tweets (com vídeo): {details.get('count_tweets_video', 0)}")
        print(f"  Threads de Tweets: {details.get('count_tweets_thread', 0)}")
        print(f"  >> PONTUAÇÃO TOTAL DE TWEETS: {details.get('total_score_from_tweets', 0.0):.2f}")

        # Seção de Engajamentos
        print("\\n--- Pontuação de Engajamentos (feitos por ele) ---")
        print(f"  Retweets/Quotes feitos: {details.get('count_retweets_made', 0)}")
        print(f"  Comentários feitos: {details.get('count_comments_made', 0)}")
        print(f"  >> PONTUAÇÃO TOTAL DE ENGAJAMENTOS: {details.get('total_score_from_engagements', 0.0):.2f}")
        
        # Seção de Telegram
        print("\\n--- Pontuação do Telegram ---")
        print(f"  >> PONTUAÇÃO TOTAL DO TELEGRAM: {details.get('total_score_from_telegram', 0.0):.2f}")
        
        # Seção de Contribuições Manuais
        print("\\n--- Pontuação de Contribuições Manuais ---")
        print(f"  Introdução de Parceiro: {details.get('count_partner_introduction', 0)}")
        print(f"  Organização de AMA: {details.get('count_hosting_ama', 0)}")
        print(f"  Recrutamento de Embaixador: {details.get('count_recruitment_ambassador', 0)}")
        print(f"  Feedback de Produto: {details.get('count_product_feedback', 0)}")
        print(f"  Recrutamento de Investidor: {details.get('count_recruitment_investor', 0)}")
        print(f"  >> PONTUAÇÃO TOTAL DE CONTRIBUIÇÕES: {details.get('total_score_from_contributions', 0.0):.2f}")
        
        print("-" * 80)
        
        # Verificação final
        calculated_sum = (
            details.get('total_score_from_tweets', 0) +
            details.get('total_score_from_engagements', 0) +
            details.get('total_score_from_telegram', 0) +
            details.get('total_score_from_contributions', 0)
        )
        grand_total = details.get('grand_total_score', 0)
        
        print(f"\\nSOMA CALCULADA DAS PARTES: {calculated_sum:.2f}")
        print(f"PONTUAÇÃO TOTAL REGISTRADA: {grand_total:.2f}")
        print("-" * 80)
        
        # Conclusão
        print("\\n--- CONCLUSÃO DA ANÁLISE ---")
        if abs(calculated_sum - grand_total) > 0.01:
            print("  [!!!] ALERTA DE INCONSISTÊNCIA: A soma das partes NÃO BATE com o total registrado!")
            print(f"        A diferença é de: {(grand_total - calculated_sum):.2f} pontos.")
            print("        Isso indica um erro grave na query que calcula a soma final no script SQL.")
        else:
            print("  [OK] SOMA CONSISTENTE: A soma das partes bate com o total registrado.")
        
        if max(details.get(k, 0) for k in details if 'score' in k and k != 'grand_total_score') == details.get('total_score_from_telegram'):
             print("  PONTO DE ATENÇÃO: A maior fonte de pontos vem da atividade do Telegram.")
        elif max(details.get(k, 0) for k in details if 'score' in k and k != 'grand_total_score') == details.get('total_score_from_tweets'):
             print("  PONTO DE ATENÇÃO: A maior fonte de pontos vem da criação de tweets.")
        
        print("="*80)

    except Exception as e:
        logger.error(f"Ocorreu um erro durante a análise detalhada: {e}", exc_info=True)

async def main():
    from dotenv import load_dotenv
    load_dotenv()
    # Focando a análise no usuário suspeito
    await get_user_score_details(user_name="Victor Yellowian")

if __name__ == '__main__':
    asyncio.run(main()) 