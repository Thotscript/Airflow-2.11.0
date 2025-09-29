#!/bin/bash
# update-dags-git.sh - Atualiza apenas a pasta dags do Git

echo "Atualizando apenas a pasta dags do Git..."

cd /opt/Airflow-2.11.0

# Salva mudanças locais (stash)
git stash

# Faz pull
git pull origin main

# Restaura stash se necessário (cuidado com conflitos)
# git stash pop

# Reinicia scheduler
docker-compose restart airflow-scheduler

echo "✓ DAGs atualizadas do Git!"