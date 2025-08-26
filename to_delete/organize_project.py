import shutil
from pathlib import Path
import re

def organize_project():
    """
    Automatiza a reorganização completa do projeto para uma estrutura limpa e final,
    pronta para um novo repositório Git.
    """
    # Define os caminhos com base na localização atual do script.
    # O script está em '.../engagement_tracker/YELLOW/'
    current_dir = Path.cwd() 
    project_root = current_dir.parent # '.../engagement_tracker/'
    source_dir = current_dir # A pasta 'YELLOW' atual
    dest_dir = project_root / 'YELLOW_PROJECT_FINAL'

    print("--- INICIANDO ORGANIZAÇÃO AUTOMÁTICA DO PROJETO ---")
    print(f"Pasta de origem: {source_dir}")
    print(f"Pasta de destino: {dest_dir}")

    # 1. Cria a pasta de destino final.
    try:
        print(f"\\n1. Criando a pasta de destino '{dest_dir.name}'...")
        dest_dir.mkdir(exist_ok=True)
        print("   Pasta de destino criada com sucesso.")
    except Exception as e:
        print(f"   ERRO ao criar a pasta de destino: {e}")
        return

    # 2. Copia todos os arquivos e pastas para o novo destino.
    print("\\n2. Copiando arquivos e pastas...")
    items_to_copy = list(source_dir.iterdir())
    total_items = len(items_to_copy)
    for i, item_path in enumerate(items_to_copy):
        # Ignora o próprio script de organização para não se copiar.
        if item_path.name == 'organize_project.py':
            continue

        destination_item_path = dest_dir / item_path.name
        
        try:
            if item_path.is_dir():
                # Se o diretório já existe no destino, remove antes de copiar para garantir uma cópia limpa.
                if destination_item_path.exists():
                    shutil.rmtree(destination_item_path)
                shutil.copytree(item_path, destination_item_path)
            else:
                shutil.copy2(item_path, destination_item_path)
            
            print(f"   ({i+1}/{total_items}) Copiado: {item_path.name}")
            
        except Exception as e:
            print(f"   ERRO ao copiar '{item_path.name}': {e}")
            # Continua para o próximo item
            continue
    print("   Cópia de todos os arquivos concluída.")

    # 3. Corrige o arquivo de workflow do GitHub Actions no novo local.
    print("\\n3. Corrigindo o arquivo de workflow...")
    workflow_file_path = dest_dir / '.github' / 'workflows' / 'scheduled_jobs.yml'
    
    if workflow_file_path.exists():
        try:
            with open(workflow_file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Regex para encontrar e remover o bloco 'defaults' inteiro que define o working-directory.
            # Isso lida com variações de espaçamento e indentação.
            pattern = re.compile(r"^\s*defaults:\s*\n\s*run:\s*\n\s*working-directory: ./YELLOW\s*\n", re.MULTILINE)
            
            new_content, num_replacements = pattern.subn("", content)

            if num_replacements > 0:
                with open(workflow_file_path, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f"   Arquivo '{workflow_file_path.name}' corrigido com sucesso. ({num_replacements} ocorrências removidas).")
            else:
                print("   AVISO: O bloco 'working-directory' não foi encontrado. O arquivo pode já estar correto.")

        except Exception as e:
            print(f"   ERRO ao corrigir o arquivo de workflow: {e}")
    else:
        print(f"   AVISO: Arquivo de workflow não encontrado em '{workflow_file_path}'. Pulo esta etapa.")

    print("\\n--- ORGANIZAÇÃO CONCLUÍDA ---")
    print(f"\\nO seu projeto final e organizado está pronto na pasta: {dest_dir}")
    print("Agora você pode navegar para essa pasta e enviá-la para um novo repositório do GitHub.")

if __name__ == '__main__':
    organize_project() 