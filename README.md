# Projeto Jira

Script para extrair chamados do Jira e exportar para Excel.

## Configuração de variáveis sensíveis

1. Copie o arquivo de exemplo para criar seu arquivo local:
   - PowerShell:
     ```powershell
     Copy-Item .env.example .env
     ```
2. Edite o `.env` e preencha os valores reais:
   - `JIRA_EMAIL`
   - `JIRA_API_TOKEN`
   - `JIRA_BASE_URL`
   - `PROJECT_KEY`

> O arquivo `.env` está no `.gitignore` e não deve ser versionado.

## Execução

```powershell
python main.py
```
