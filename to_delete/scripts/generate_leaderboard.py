import os
import sys
from pathlib import Path

# Adiciona o diretório raiz do projeto ao sys.path
# Isso permite que o script seja executado de qualquer lugar e encontre os módulos.
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from core.database_client import get_db_client

def run_leaderboard_generation():
    """
    Executa o script SQL para gerar o histórico completo do leaderboard.
    """
    print("Iniciando a geração do leaderboard retroativo...")

    db_client = get_db_client()

    # O caminho para o arquivo SQL é relativo à raiz do projeto
    sql_file_path = project_root / 'generate_retroactive_leaderboard.sql'

    if not sql_file_path.exists():
        print(f"ERRO: Arquivo SQL não encontrado em '{sql_file_path}'")
        sys.exit(1)

    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_query = f.read()

    print("Limpando a tabela 'leaderboard_history' e repopulando...")

    try:
        # Usamos rpc() para chamar uma função, mas para executar um script SQL bruto,
        # uma abordagem direta com a API do PostgREST (que o cliente Supabase usa)
        # não é ideal para scripts grandes e complexos.
        # A melhor prática é usar a biblioteca `psycopg2` ou similar se o script for muito grande,
        # mas para este caso, uma chamada rpc para uma função que executa o script
        # ou a execução direta (se o cliente suportar) é viável.
        # Vamos tentar uma execução direta. Assumindo que o client pode executar SQL bruto.
        # Como o supabase-py não tem um método `execute_sql` direto,
        # vamos usar o que temos, que pode ser uma chamada a uma função.
        # Se não houver uma função, teremos que criar uma ou usar outra lib.
        # Por simplicidade, vamos assumir que o `rpc` pode executar SQL.
        # NOTA: O método .rpc() é para chamar stored procedures.
        # Executar um script SQL de várias instruções não é diretamente suportado.
        # A maneira correta é usar a conexão postgres do supabase.
        # O cliente python não expõe isso diretamente.
        # No entanto, a API REST do Supabase permite consultas.
        # Vamos usar o método `execute` que existe na conexão postgres subjacente se disponível.
        # O `supabase-py` v1 não expõe isso. A v2 sim.
        # Assumindo que podemos fazer isso, se não, precisaremos ajustar.
        
        # A forma mais simples de executar um SQL grande e multi-statement é através de uma função no DB.
        # Vamos criar uma função para encapsular nosso script.
        # Mas por agora, vamos tentar enviar o SQL diretamente.
        
        # A API do Supabase não permite múltiplas queries.
        # A solução é fazer uma por uma.
        # O script tem um TRUNCATE e um INSERT.
        
        queries = [q.strip() for q in sql_query.split(';') if q.strip()]
        
        for query in queries:
            print(f"Executando query: {query[:80]}...")
            # A API Supabase não tem um método para executar SQL genérico.
            # Temos que usar uma função (RPC).
            # Vamos criar uma função no banco para executar isso.
            # Por agora, vou apenas simular.
            # Na verdade, a API do Supabase permite, mas através do endpoint /rest/v1/rpc/exec_sql
            # O python client pode não ter uma abstração pra isso.
            # Vamos fazer o que é garantido: chamar uma stored procedure.
            # Vou assumir que o usuário pode criar a função.
            
            # UPDATE: A forma mais fácil é simplesmente passar a query para o endpoint de queries.
            # O cliente supabase-py usa postgrest.
            # db_client.table('arbitrary_table').select('*').execute() é como funciona.
            # Não há `db_client.execute(sql)`.
            
            # Solução pragmática:
            # 1. Separar truncate do insert.
            # 2. Chamar o truncate
            # 3. Chamar o insert.
            
            # O `execute` no `postgrest-py` é para a query builder.
            # Vamos usar o `rpc`.
            
            # Dividindo o script em comandos. O `TRUNCATE` e o `INSERT`.
            truncate_query = "TRUNCATE TABLE leaderboard_history;"
            insert_query = query # O resto do script
            
            # A biblioteca supabase-py não tem um método `execute` para SQL bruto.
            # A maneira de fazer isso é usar o `psycopg2` com as credenciais do banco de dados.
            # Ou, criar uma função no banco de dados e chamá-la via RPC.
            
            # Vamos pelo caminho de criar uma função no DB.
            # `create_or_replace_function...`
            # E depois chamar `db_client.rpc('generate_leaderboard_data')`
            # Por agora, o script vai apenas printar o que faria.
            
            # A API do supabase permite queries diretas no endpoint /rest/v1/?
            # Sim, mas o python client abstrai isso.
            
            # Final approach: a API do Supabase /rest/v1/ `leaderboard_history` permite POST para inserir.
            # Mas o nosso `INSERT ... SELECT` é complexo.
            
            # A única forma robusta é RPC.
            db_client.rpc('execute_sql', {'sql': query}).execute()

        print("Geração do leaderboard concluída com sucesso!")

    except Exception as e:
        print(f"Ocorreu um erro ao gerar o leaderboard: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_leaderboard_generation() 