"""Execute com: uv run --no-sync python scripts/generate_proto.py."""

from pathlib import Path

from grpc_tools import protoc


ROOT = Path(__file__).resolve().parents[1]


def generate(output: Path = ROOT) -> int:
    # O caminho virtual mantém imports dos stubs dentro do pacote Python.
    return protoc.main([
        "grpc_tools.protoc",
        f"-Icluster_protocol={ROOT / 'proto'}",
        f"--python_out={output}",
        f"--grpc_python_out={output}",
        "cluster_protocol/cluster.proto",
    ])


if __name__ == "__main__":
    raise SystemExit(generate())
