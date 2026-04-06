# Project: Azure Table to Snowflake Sync
A Python Azure Function (v2 programming model) that synchronizes data from Azure Table Storage to Snowflake.

## Professional Standards
- **Naming:** Use snake_case for functions and variables, PascalCase for classes.
- **Type Hinting:** All functions must have complete PEP 484 type hints.
- **Error Handling:** Use specific exception handling; avoid bare `except:`. 
- **Security:** Use `azure-identity` and `DefaultAzureCredential`. No hardcoded connection strings or keys.
- **Cloud Design:** Favor statelessness and idempotent operations.

## Environment & Tooling
- **Virtual Env:** `.venv`
- **Dependency Manager:** `pip` (standard `requirements.txt`)
- **Testing:** `pytest`
- **Azure Tools:** Azure Functions Core Tools (v4+)

## Common Commands
- **Run Locally:** `func start`
- **Run Tests:** `pytest`
- **Install Deps:** `pip install -r requirements.txt`