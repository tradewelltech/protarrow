import pathlib
import sys
import warnings
from typing import List

import google.type.date_pb2
import grpc_tools

_ROOT_DIR = pathlib.Path(__file__).parent.parent.absolute()
_GOOGLE_COMMON_PROTOS_ROOT_DIR = pathlib.Path(
    google.type.date_pb2.__file__
).parent.parent.parent.absolute()
_GRPC_PROTOS_INCLUDE = pathlib.Path(grpc_tools.__file__).parent.absolute() / "_proto"
_SRC_DIR = _ROOT_DIR / "protos"
_OUT_DIR = _ROOT_DIR / "protarrow_protos"


def run_protoc(arguments: List[str]):
    try:
        import grpc_tools.protoc

        return_code = grpc_tools.protoc.main(arguments)
        if return_code != 0:
            raise RuntimeError("Could not generate proto")

    except ImportError:
        warnings.warn("Using system version of protoc")
        import subprocess  # nosec B404

        subprocess.run(arguments, check=True)  # nosec B603


def main():
    out_dir = _OUT_DIR if len(sys.argv) < 2 else pathlib.Path(sys.argv[1])
    out_dir.mkdir(parents=True, exist_ok=True)

    proto_files = [x.as_posix() for x in _SRC_DIR.glob("**/*.proto")]
    proto_args = [
        "protoc",
        f"--proto_path={_GOOGLE_COMMON_PROTOS_ROOT_DIR}",
        f"--proto_path={_GRPC_PROTOS_INCLUDE}",
        f"--proto_path={_SRC_DIR}",
        f"--python_out={out_dir}",
    ] + proto_files
    print(" ".join(proto_args))
    run_protoc(proto_args)
    (out_dir / "__init__.py").touch()


if __name__ == "__main__":
    main()
