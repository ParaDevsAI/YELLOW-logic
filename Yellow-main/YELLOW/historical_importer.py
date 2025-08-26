"""
historical_importer.py

Este script é o coração do processo de backfill de dados. Ele é projetado
para ser executado uma vez para popular o banco de dados Supabase.

Fluxo de Execução:
1.  Lê os autores existentes de 'authors_rows.csv'.
2.  Processa uma lista de novos autores definidos no código.
3.  Combina as duas listas.
4.  Para cada autor, busca os dados de perfil mais recentes da API do Twitter para enriquecer as informações.
5.  Salva a lista final e enriquecida em 'authors_final.csv' para verificação manual.
6.  Aguarda a confirmação do usuário no console para prosseguir.
7.  Faz o 'upsert' de todos os autores na tabela 'authors' do Supabase.
8.  Para cada autor, busca seu histórico de tweets e salva nas tabelas 'tweets' e 'tweet_entities'.
"""

import asyncio
import os
import pandas as pd
import re
import httpx
from dotenv import load_dotenv

# Importando os módulos do nosso projeto
from author_manager import get_supabase_client
from twitter_client import get_twitter_info
from tweet_link_tracker import save_full_tweet_data

# --- Dados dos Novos Usuários ---
# Lista dos novos usuários fornecidos para serem adicionados.
NEW_USERS_DATA = [
    "@https://x.com/hacubismurf esse telegram id hacubi_ai 5818069334,Hacubi | NeuralAI,hacubi_ai",
    "esse id telegram id 6659833825,Carlos,Carloxir esse name no twitter @https://x.com/Carloxir",
    "5943016786,Pascal christ,Pascal_cj e https://x.com/JackGovinda/status/1942319387123589403?t=PyN3EDbLROQjrWub2X8rmA&s=19",
    "7381221094,Rhaegar,rhaegar_A e @https://x.com/takana_0_01?s=21",
    "367966118,Cryptopeet,plscallmeadmin e @https://x.com/Crypto_peet",
    "1708440962,PaintX,PaintXYZ e @https://x.com/martinpistrich?t=2MgxLPQey42SlJs_iwS-cA&s=09",
    "1720637082,sejtron.azero,sejtron e @https://x.com/sejtron18",
    "5034352240,Bells'©,jahzkhid e @https://x.com/Crypt0bells"
]

def parse_new_users(user_data_list: list) -> pd.DataFrame:
    """Processa a lista de strings de novos usuários e a converte em um DataFrame."""
    print("\n--- Processando lista de novos usuários ---")
    parsed_users = []
    for line in user_data_list:
        try:
            # Extrair twitter username da URL
            tw_match = re.search(r"x\.com/(\w+)", line)
            twitter_username = tw_match.group(1) if tw_match else None
            
            # Extrair telegram_id (sequência de 10 ou mais dígitos)
            tg_id_match = re.search(r"\b(\d{10,})\b", line)
            telegram_id = tg_id_match.group(1) if tg_id_match else None
            
            # O resto da linha são nomes, separados por vírgula
            parts = re.split(r'\s*,\s*', line)
            telegram_name = parts[1] if len(parts) > 1 else "N/A"
            telegram_username = parts[2] if len(parts) > 2 else "N/A"

            if twitter_username and telegram_id:
                parsed_users.append({
                    'telegram_id': int(telegram_id),
                    'telegram_name': telegram_name.strip(),
                    'telegram_username': telegram_username.strip(),
                    'twitter_username': twitter_username.strip()
                })
                print(f"  - Novo usuário processado: {telegram_name} (@{twitter_username})")
        except Exception as e:
            print(f"  - AVISO: Falha ao processar a linha: '{line[:50]}...'. Erro: {e}")
            
    return pd.DataFrame(parsed_users)

async def fetch_and_enrich_authors() -> pd.DataFrame:
    """
    Combina os autores do CSV com os novos, enriquece com dados do Twitter e retorna um DataFrame.
    """
    print("\n--- Fase 1: Coletando e Enriquecendo Dados dos Autores ---")
    
    # 1. Carregar autores existentes
    df_existing = pd.read_csv('authors_rows.csv')
    print(f"Lidos {len(df_existing)} autores do 'authors_rows.csv'.")
    
    # 2. Processar novos usuários
    df_new = parse_new_users(NEW_USERS_DATA)
    
    # 3. Combinar as listas
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    print(f"Total de autores a processar: {len(df_combined)}.")
    
    # 4. Enriquece cada autor com dados frescos do Twitter
    all_authors_data = []
    for index, row in df_combined.iterrows():
        username = row.get('twitter_username')
        if not username or pd.isna(username):
            print(f"AVISO: Pulando autor com telegram_id {row.get('telegram_id')} por falta de twitter_username.")
            continue
            
        print(f"Buscando dados para @{username}...")
        twitter_data = await get_twitter_info(username)
        
        # Começa com os dados que já temos (do CSV ou da lista)
        author_record = row.to_dict()
        
        if twitter_data:
            # Se a API retornou dados, atualizamos o registro
            author_record.update({
                'twitter_id': twitter_data.get('id'),
                'twitter_name': twitter_data.get('name'),
                'twitter_description': twitter_data.get('description'),
                'twitter_followers': twitter_data.get('followers'),
                'twitter_following': twitter_data.get('following'),
                'twitter_statusescount': twitter_data.get('statusesCount'),
                'twitter_mediacount': twitter_data.get('mediaCount'),
                'twitter_createdat': twitter_data.get('createdAt'),
                'twitter_isblueverified': twitter_data.get('isBlueVerified', False),
                'twitter_profilepicture': twitter_data.get('profilePicture')
            })
        else:
            print(f"AVISO: Não foi possível buscar dados para @{username}. Usando dados existentes.")
        
        all_authors_data.append(author_record)

    return pd.DataFrame(all_authors_data)

async def fetch_historical_tweets(supabase_client, api_key: str):
    """Busca o histórico de tweets para todos os autores no DB."""
    print("\n--- Iniciando busca de tweets históricos para todos os autores ---")
    
    response = await asyncio.to_thread(supabase_client.table('authors').select('telegram_id, twitter_id, twitter_username').execute)
    if not response.data:
        print("Nenhum autor encontrado no Supabase para buscar tweets.")
        return
        
    authors = response.data
    
    async with httpx.AsyncClient(timeout=40.0) as http_client:
        for author in authors:
            username = author.get('twitter_username')
            telegram_id = author.get('telegram_id')
            if not username:
                continue

            print(f"\nBuscando tweets para @{username}...")
            url = "https://api.twitterapi.io/twitter/tweet/advanced_search"
            # Uma consulta simples para buscar tweets de um usuário
            query = f"from:{username}"
            params = {"query": query, "queryType": "Latest"}
            headers = {"X-API-Key": api_key}
            
            try:
                api_response = await http_client.get(url, params=params, headers=headers)
                if api_response.status_code == 200:
                    data = api_response.json()
                    tweets = data.get('tweets', [])
                    print(f"Encontrados {len(tweets)} tweets para @{username}. Salvando no Supabase...")
                    for tweet in tweets:
                        # Reutilizamos a função já testada do tweet_link_tracker
                        await save_full_tweet_data(tweet, telegram_id)
                else:
                    print(f"  - ERRO na API ao buscar tweets para @{username}. Status: {api_response.status_code}")
            except Exception as e:
                print(f"  - ERRO inesperado ao processar @{username}: {e}")

async def main():
    """Função principal que orquestra todo o processo."""
    load_dotenv()
    
    # --- FASE 1: FOI REMOVIDA, VAMOS USAR O ARQUIVO JÁ GERADO ---
    output_csv = 'authors_final.csv'
    if not os.path.exists(output_csv):
        print(f"ERRO: O arquivo '{output_csv}' não foi encontrado.")
        print("Por favor, execute a primeira fase do script (coleta de dados) primeiro.")
        return
        
    print(f"--- Usando o arquivo de verificação existente: '{output_csv}' ---")
    final_authors_df = pd.read_csv(output_csv)

    # --- FASE 2: ENVIAR PARA O BANCO DE DADOS ---
    proceed = input("Deseja continuar e enviar os dados de 'authors_final.csv' para o Supabase? (s/n): ")
    if proceed.lower() != 's':
        print("Operação abortada pelo usuário.")
        return
        
    print("\n--- Iniciando envio para o Supabase ---")
    supabase = await get_supabase_client()
    if not supabase:
        print("ERRO CRÍTICO: Falha ao conectar com o Supabase.")
        return
        
    # 1. Enviar autores
    print("Enviando dados dos autores...")
    # Limpeza final dos dados, convertendo NaN para None, que é compatível com JSON.
    # Esta é a correção definitiva para o erro 'Out of range float values'.
    records_to_upsert = final_authors_df.where(pd.notnull(final_authors_df), None).to_dict(orient='records')
    
    await asyncio.to_thread(
        supabase.table('authors').upsert(records_to_upsert, on_conflict='telegram_id').execute
    )
    print("Dados dos autores enviados com sucesso.")

    # 2. Buscar tweets históricos
    api_key = os.getenv("TWITTER_API_KEY")
    if api_key:
        await fetch_historical_tweets(supabase, api_key)
    else:
        print("AVISO: TWITTER_API_KEY não encontrada. Pulando busca de tweets históricos.")

    print("\n--- PROCESSO CONCLUÍDO ---")

if __name__ == "__main__":
    asyncio.run(main()) 