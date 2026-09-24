# Local AI Model Support in Antigravity SDK

This project demonstrates how to integrate and use local AI models within the Antigravity SDK, specifically leveraging `litert-lm` for local inference.

## Features

- **Local Model Inference**: Uses `litert-lm` to run models locally, reducing dependency on cloud APIs.
- **Antigravity SDK Integration**: Integrates with `google-antigravity` to provide an agentic interface for the local model.
- **Resource Monitoring**: Includes a CLI tool to monitor system resources during model execution.

## Prerequisites

Before running the project, ensure you have the required local model downloaded. The project is configured to look for the model at:
`~/.litert-lm/models/gemma4-26b/model.litertlm`

## Installation

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

```bash
# Install dependencies and the project
uv sync
```

### Model download

```
uv run litert-lm import --from-huggingface-repo=litert-community/gemma-4-26B-A4B-it-litert-lm gemma-4-26B-A4B-it-gpu.litertlm gemma4-26b
```


## Usage

### Running the Local AI Agent

To run the main demonstration that chats with the local model:

```bash
uv run local-ai-model-support-in-antigravity-sdk
```

### Resource Monitor

To monitor system resources while the model is running:

```bash
uv run resource-monitor
```

## Dependencies

- `google-antigravity`: The core SDK for agentic workflows.
- `litert-lm`: Provides the runtime for local LiteRT model inference.
