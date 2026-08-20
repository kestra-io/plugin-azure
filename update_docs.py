import os

DOC_FILE = '/Users/hemantmehta/Documents/kesta-plugin-azure/src/main/resources/doc/io.kestra.plugin.azure.md'

with open(DOC_FILE, 'r') as f:
    content = f.read()

azure_ai_foundry_section = '''
## Azure AI Foundry

The `aifoundry` package provides tasks and a trigger for interacting with Azure AI Foundry:

- `aifoundry.ChatCompletion` - Call a deployed model for chat completions.
- `aifoundry.Embeddings` - Generate vector embeddings from text input.
- `aifoundry.RunAgent` - Create and run an Azure AI Foundry agent, returning the conversation result.
- `aifoundry.CreateEvaluation` - Submit a new evaluation job using a dataset and a set of evaluators.
- `aifoundry.GetDeployment` - Retrieve deployment status and configuration.
- `aifoundry.Trigger` - Poll an Azure AI Foundry agent run and fire when it reaches a terminal state.

### Authentication for Azure AI Foundry

- Tasks like `ChatCompletion` and `Embeddings` support API-key authentication (via the `apiKey` property) or Entra ID (`DefaultAzureCredential`).
- Tasks that use the Azure AI Projects SDK (like `RunAgent`, `CreateEvaluation`, `GetDeployment`, and `Trigger`) **require** Entra ID (`DefaultAzureCredential`) as API keys are not supported by the underlying client. Do not provide the `apiKey` property when using these tasks.
'''

if "## Azure AI Foundry" not in content:
    with open(DOC_FILE, 'a') as f:
        f.write("\n" + azure_ai_foundry_section)
    print("Added Azure AI Foundry section to docs.")
else:
    print("Azure AI Foundry section already exists.")
