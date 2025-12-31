# Obtém o diretório onde o script está localizado
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ativa o venv usando caminho absoluto
source "$DIR/venv/bin/activate"

# Executa o main.py usando caminho absoluto
python3 "$DIR/main.py"

# Desativa o venv
deactivate