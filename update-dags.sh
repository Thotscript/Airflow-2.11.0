#!/bin/bash

# Script para atualizar apenas as DAGs sem rebuild
set -e

PROJECT_NAME="airflow-project"
INSTALL_DIR="/opt/$PROJECT_NAME"

echo "🔄 Atualizando DAGs do Airflow..."

# Verificar se o projeto existe
if [ ! -d "$INSTALL_DIR" ]; then
    echo "❌ Projeto não encontrado em $INSTALL_DIR"
    echo "Execute primeiro o deploy.sh"
    exit 1
fi

cd $INSTALL_DIR

# Fazer backup das DAGs atuais
echo "💾 Fazendo backup das DAGs atuais..."
if [ -d "dags" ] && [ "$(ls -A dags)" ]; then
    cp -r dags dags_backup_$(date +%Y%m%d_%H%M%S)
fi

# Atualizar apenas os arquivos do repositório
echo "📥 Baixando atualizações do Git..."
git fetch origin main

# Verificar se há mudanças
if git diff HEAD origin/main --quiet; then
    echo "✅ Nenhuma atualização disponível"
    exit 0
fi

# Mostrar o que será atualizado
echo "📋 Mudanças detectadas:"
git diff --name-only HEAD origin/main

# Atualizar o repositório
git merge origin/main

# Verificar se os serviços estão rodando
if ! docker-compose ps | grep -q "Up"; then
    echo "⚠️ Serviços não estão rodando. Iniciando..."
    docker-compose up -d
else
    echo "🔄 Reiniciando apenas o scheduler para aplicar mudanças..."
    docker-compose restart airflow-scheduler
fi

# Verificar saúde dos serviços
echo "🔍 Verificando saúde dos serviços..."
sleep 10

if docker-compose ps | grep -E "(airflow-webserver|airflow-scheduler)" | grep -q "Up"; then
    echo "✅ DAGs atualizadas com sucesso!"
    echo "🌐 Verifique em: http://localhost:8080"
else
    echo "❌ Erro: Alguns serviços não estão funcionando"
    docker-compose logs airflow-scheduler --tail=20
    exit 1
fi