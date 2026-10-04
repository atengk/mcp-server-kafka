# ==============================================================================
# mcp-server-kafka 生产级多阶段容器镜像构建文件
#
# @author Ateng
# @since 2026-10-04
# ==============================================================================

# ==============================================================================
# 阶段 1: 构建依赖与虚拟环境 (Builder)
# ==============================================================================
FROM python:3.12-slim AS builder

# 从官方极速镜像复制预编译 uv 工具
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 优化构建过程与缓存行为
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 先行复制项目依赖配置文件，最大化利用 Docker 缓存层
COPY pyproject.toml README.md uv.lock /app/

# 安装项目运行时依赖到虚拟环境 /app/.venv（排除开发依赖）
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev --no-install-project

# 复制工程源码并完成项目自身安装
COPY src/ /app/src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-dev

# ==============================================================================
# 阶段 2: 生产运行阶段 (Runtime)
# ==============================================================================
FROM python:3.12-slim AS runtime

# 运行时安全防护与环境配置
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# 创建专有非 root 运行账号 appuser (UID 10001, GID 10001)，遵循生产最小权限基线
RUN groupadd -g 10001 appuser && \
    useradd -u 10001 -g 10001 -s /usr/sbin/nologin -m appuser

# 从构建阶段复制已就绪的虚拟环境与代码资产并赋予属主权限
COPY --from=builder --chown=appuser:appuser /app /app

# 切换为专有非 root 用户执行
USER appuser

# 暴露 HTTP SSE 网关默认监听端口
EXPOSE 8000

# 默认服务入口：支持通过 CLI 参数直接追加覆盖
ENTRYPOINT ["mcp-server-kafka"]

# 默认常驻参数：以 SSE 传输网关模式监听 0.0.0.0:8000
CMD ["--transport", "sse", "--host", "0.0.0.0", "--port", "8000"]
