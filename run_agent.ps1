# 使用项目虚拟环境启动 Agent CLI
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $Root ".venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    Write-Error "未找到虚拟环境: $Python`n请先创建并安装依赖: python -m venv .venv; .\.venv\Scripts\pip install -r requirements.txt"
}

& $Python (Join-Path $Root "run_agent_cli.py") @args
