import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime
import os
from pathlib import Path


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value

# ==============================
# CONFIGURAÇÕES
# ==============================

load_env_file(Path(__file__).with_name(".env"))

JIRA_EMAIL = os.getenv("JIRA_EMAIL")
JIRA_API_TOKEN = os.getenv("JIRA_API_TOKEN")
JIRA_BASE_URL = os.getenv("JIRA_BASE_URL")
PROJECT_KEY = os.getenv("PROJECT_KEY", "TI")

missing_vars = [
    var_name
    for var_name, var_value in {
        "JIRA_EMAIL": JIRA_EMAIL,
        "JIRA_API_TOKEN": JIRA_API_TOKEN,
        "JIRA_BASE_URL": JIRA_BASE_URL,
    }.items()
    if not var_value
]

if missing_vars:
    raise ValueError(
        "Variáveis ausentes no ambiente/.env: " + ", ".join(missing_vars)
    )

# ==============================
# AUTENTICAÇÃO
# ==============================

auth = HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN)

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# ==============================
# ENDPOINT DE BUSCA ENHANCED
# ==============================

search_url = f"{JIRA_BASE_URL}/rest/api/3/search/jql"

# ==============================
# JQL — TODOS OS STATUS
# ==============================

jql = f'project = "{PROJECT_KEY}" ORDER BY created ASC'

max_results = 100
all_issues = []
next_page_token = None

print("🔄 Buscando TODOS os chamados do projeto TI...")

# ==============================
# PAGINAÇÃO
# ==============================

while True:
    payload = {
        "jql": jql,
        "maxResults": max_results,
        "fields": [
            "summary",
            "status",
            "issuetype",
            "priority",
            "assignee",
            "reporter",
            "created",
            "updated",
            "resolutiondate"
        ]
    }

    if next_page_token:
        payload["nextPageToken"] = next_page_token

    r = requests.post(
        search_url,
        headers=headers,
        auth=auth,
        json=payload
    )

    if r.status_code != 200:
        print("❌ Erro na API Jira")
        print("Status:", r.status_code)
        print("Resposta:", r.text)
        raise Exception("Erro ao buscar chamados")

    data = r.json()
    issues = data.get("issues", [])

    if not issues:
        break

    all_issues.extend(issues)
    next_page_token = data.get("nextPageToken")

    print(f"📄 Chamados coletados: {len(all_issues)}")

    if data.get("isLast", True):
        break

print(f"✅ Total final de chamados: {len(all_issues)}")

# ==============================
# NORMALIZAÇÃO
# ==============================

rows = []

for issue in all_issues:
    f = issue["fields"]

    rows.append({
        "issue_key": issue["key"],
        "summary": f.get("summary"),
        "status": f.get("status", {}).get("name"),
        "status_category": f.get("status", {}).get("statusCategory", {}).get("name"),
        "request_type": f.get("issuetype", {}).get("name"),
        "priority": f.get("priority", {}).get("name") if f.get("priority") else None,
        "assignee": f.get("assignee", {}).get("displayName") if f.get("assignee") else None,
        "reporter": f.get("reporter", {}).get("displayName") if f.get("reporter") else None,
        "created": f.get("created"),
        "updated": f.get("updated"),
        "resolution_date": f.get("resolutiondate")
    })

df = pd.DataFrame(rows)

# ==============================
# EXPORTAÇÃO
# ==============================

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
arquivo = f"jira_TI_todos_chamados_{timestamp}.xlsx"

df.to_excel(arquivo, index=False, engine="openpyxl")

print(f"📁 Planilha gerada com sucesso: {arquivo}")