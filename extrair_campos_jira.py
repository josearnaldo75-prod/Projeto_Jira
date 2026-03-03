import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth


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


def validate_env() -> tuple[str, str, str, str]:
    load_env_file(Path(__file__).with_name(".env"))

    jira_email = os.getenv("JIRA_EMAIL")
    jira_api_token = os.getenv("JIRA_API_TOKEN")
    jira_base_url = os.getenv("JIRA_BASE_URL")
    project_key = os.getenv("PROJECT_KEY", "TI")

    missing_vars = [
        var_name
        for var_name, var_value in {
            "JIRA_EMAIL": jira_email,
            "JIRA_API_TOKEN": jira_api_token,
            "JIRA_BASE_URL": jira_base_url,
        }.items()
        if not var_value
    ]

    if missing_vars:
        raise ValueError("Variáveis ausentes no ambiente/.env: " + ", ".join(missing_vars))

    return jira_email, jira_api_token, jira_base_url, project_key


def serialize_field_value(value):
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return json.dumps(value, ensure_ascii=False)


def fetch_fields_catalog(base_url: str, headers: dict, auth: HTTPBasicAuth) -> list[dict]:
    url = f"{base_url}/rest/api/3/field"
    response = requests.get(url, headers=headers, auth=auth)

    if response.status_code != 200:
        raise Exception(f"Erro ao buscar catálogo de campos: {response.status_code} - {response.text}")

    return response.json()


def fetch_project_issues_with_all_fields(
    base_url: str,
    headers: dict,
    auth: HTTPBasicAuth,
    project_key: str,
    max_results: int = 100,
) -> list[dict]:
    jql = f'project = "{project_key}" ORDER BY created ASC'

    all_issues = []
    print(f"🔄 Coletando issues do projeto {project_key} com todos os campos possíveis...")

    search_jql_url = f"{base_url}/rest/api/3/search/jql"
    next_page_token = None
    use_fallback_search = False

    while not use_fallback_search:
        payload = {
            "jql": jql,
            "maxResults": max_results,
            "fields": ["*all"],
        }

        if next_page_token:
            payload["nextPageToken"] = next_page_token

        response = requests.post(search_jql_url, headers=headers, auth=auth, json=payload)

        if response.status_code == 400 and not all_issues:
            print("⚠️ Endpoint /search/jql rejeitou o payload. Tentando fallback para /search...")
            use_fallback_search = True
            break

        if response.status_code != 200:
            raise Exception(f"Erro ao buscar issues: {response.status_code} - {response.text}")

        data = response.json()
        issues = data.get("issues", [])

        if not issues:
            break

        all_issues.extend(issues)
        next_page_token = data.get("nextPageToken")

        print(f"📄 Issues coletadas: {len(all_issues)}")

        if data.get("isLast", True):
            break

    if use_fallback_search:
        search_url = f"{base_url}/rest/api/3/search"
        start_at = 0

        while True:
            params = {
                "jql": jql,
                "startAt": start_at,
                "maxResults": max_results,
                "fields": "*all",
                "expand": "names,schema",
            }

            response = requests.get(search_url, headers=headers, auth=auth, params=params)

            if response.status_code != 200:
                raise Exception(f"Erro ao buscar issues no fallback /search: {response.status_code} - {response.text}")

            data = response.json()
            issues = data.get("issues", [])

            if not issues:
                break

            all_issues.extend(issues)
            print(f"📄 Issues coletadas: {len(all_issues)}")

            start_at += len(issues)
            total = data.get("total", 0)
            if start_at >= total:
                break

    return all_issues


def build_fields_usage_dataframe(all_issues: list[dict], fields_catalog: list[dict]) -> pd.DataFrame:
    used_count = {}
    used_non_null_count = {}

    for issue in all_issues:
        fields = issue.get("fields", {})
        for field_id, field_value in fields.items():
            used_count[field_id] = used_count.get(field_id, 0) + 1
            if field_value is not None:
                used_non_null_count[field_id] = used_non_null_count.get(field_id, 0) + 1

    catalog_map = {field.get("id"): field for field in fields_catalog if field.get("id")}

    rows = []
    for field_id, issue_presence_count in sorted(used_count.items(), key=lambda item: item[0]):
        meta = catalog_map.get(field_id, {})
        schema = meta.get("schema") or {}

        rows.append(
            {
                "field_id": field_id,
                "field_name": meta.get("name"),
                "custom": meta.get("custom"),
                "schema_type": schema.get("type"),
                "schema_system": schema.get("system"),
                "schema_custom": schema.get("custom"),
                "schema_custom_id": schema.get("customId"),
                "issue_presence_count": issue_presence_count,
                "issue_non_null_count": used_non_null_count.get(field_id, 0),
                "issue_non_null_pct": round(
                    (used_non_null_count.get(field_id, 0) / len(all_issues)) * 100, 2
                )
                if all_issues
                else 0,
                "clause_names": ", ".join(meta.get("clauseNames", [])) if meta.get("clauseNames") else None,
            }
        )

    return pd.DataFrame(rows)


def build_catalog_dataframe(fields_catalog: list[dict], used_field_ids: set[str]) -> pd.DataFrame:
    rows = []
    for field in fields_catalog:
        schema = field.get("schema") or {}
        rows.append(
            {
                "field_id": field.get("id"),
                "field_name": field.get("name"),
                "custom": field.get("custom"),
                "navigable": field.get("navigable"),
                "orderable": field.get("orderable"),
                "searchable": field.get("searchable"),
                "schema_type": schema.get("type"),
                "schema_system": schema.get("system"),
                "schema_custom": schema.get("custom"),
                "schema_custom_id": schema.get("customId"),
                "clause_names": ", ".join(field.get("clauseNames", [])) if field.get("clauseNames") else None,
                "used_in_project": field.get("id") in used_field_ids,
            }
        )

    return pd.DataFrame(rows).sort_values(by=["used_in_project", "field_id"], ascending=[False, True])


def build_flat_issues_dataframe(all_issues: list[dict]) -> pd.DataFrame:
    rows = []

    for issue in all_issues:
        fields = issue.get("fields", {})
        row = {
            "issue_id": issue.get("id"),
            "issue_key": issue.get("key"),
            "issue_self": issue.get("self"),
        }

        for field_id, field_value in fields.items():
            row[field_id] = serialize_field_value(field_value)

        rows.append(row)

    return pd.DataFrame(rows)


def save_outputs(
    project_key: str,
    all_issues: list[dict],
    df_catalog: pd.DataFrame,
    df_used_fields: pd.DataFrame,
    df_issues_flat: pd.DataFrame,
) -> None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = Path(f"jira_{project_key}_issues_all_fields_raw_{timestamp}.json")
    excel_path = Path(f"jira_{project_key}_campos_completos_{timestamp}.xlsx")
    csv_path = Path(f"jira_{project_key}_issues_flat_{timestamp}.csv")

    json_path.write_text(
        json.dumps(all_issues, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    df_issues_flat.to_csv(csv_path, index=False, encoding="utf-8-sig")

    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        df_used_fields.to_excel(writer, sheet_name="campos_usados", index=False)
        df_catalog.to_excel(writer, sheet_name="catalogo_campos", index=False)

    print(f"✅ JSON bruto salvo em: {json_path}")
    print(f"✅ CSV flat salvo em: {csv_path}")
    print(f"✅ Excel de catálogo salvo em: {excel_path}")


def main() -> None:
    jira_email, jira_api_token, jira_base_url, project_key = validate_env()

    auth = HTTPBasicAuth(jira_email, jira_api_token)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    fields_catalog = fetch_fields_catalog(jira_base_url, headers, auth)
    print(f"📚 Campos disponíveis no Jira: {len(fields_catalog)}")

    all_issues = fetch_project_issues_with_all_fields(
        base_url=jira_base_url,
        headers=headers,
        auth=auth,
        project_key=project_key,
    )
    print(f"✅ Total de issues coletadas: {len(all_issues)}")

    used_field_ids = {
        field_id
        for issue in all_issues
        for field_id in issue.get("fields", {}).keys()
    }
    print(f"🧩 Campos encontrados nas issues do projeto: {len(used_field_ids)}")

    df_used_fields = build_fields_usage_dataframe(all_issues, fields_catalog)
    df_catalog = build_catalog_dataframe(fields_catalog, used_field_ids)
    df_issues_flat = build_flat_issues_dataframe(all_issues)

    save_outputs(
        project_key=project_key,
        all_issues=all_issues,
        df_catalog=df_catalog,
        df_used_fields=df_used_fields,
        df_issues_flat=df_issues_flat,
    )


if __name__ == "__main__":
    main()