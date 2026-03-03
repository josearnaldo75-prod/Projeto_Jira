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

## Extração completa de campos (para análise)

Para mapear todos os campos possíveis do Jira e os campos efetivamente usados no projeto:

```powershell
python extrair_campos_jira.py
```

O script gera:
- JSON bruto com todas as issues e campos retornados pela API
- CSV flat com colunas por `field_id`
- Excel com:
   - aba `campos_usados` (campos encontrados nas issues + taxa de preenchimento)
   - aba `catalogo_campos` (catálogo completo de campos do Jira + indicador de uso no projeto)
