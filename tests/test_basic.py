"""基础工程配置与冒烟测试.

@author Ateng
@since 2026-10-04
"""

from mcp_server_kafka import __version__


def test_version() -> None:
    """验证模块版本声明."""
    assert __version__ == "1.2.0"
