import pathlib
import sys

import google.type.date_pb2
import grpc_tools
import grpc_tools.protoc

_ROOT_DIR = pathlib.Path(__file__).parent.parent.absolute()
_GOOGLE_COMMON_PROTOS_ROOT_DIR = pathlib.Path(
    google.type.date_pb2.__file__
).parent.parent.parent.absolute()
_GRPC_PROTOS_INCLUDE = pathlib.Path(grpc_tools.__file__).parent.absolute() / "_proto"
_SRC_DIR = _ROOT_DIR / "protos"
_OUT_DIR = _ROOT_DIR / "protarrow_protos"


def main():
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    proto_files = [x.as_posix() for x in _SRC_DIR.glob("**/*.proto")]
    proto_args = [
        "protoc",
        "--proto_path={}".format(_GOOGLE_COMMON_PROTOS_ROOT_DIR),
        "--proto_path={}".format(_GRPC_PROTOS_INCLUDE),
        "--proto_path={}".format(_SRC_DIR),
        "--python_out={}".format(_OUT_DIR),
    ] + proto_files
    print(" ".join(proto_args))
    return_code = grpc_tools.protoc.main(proto_args)
    if return_code != 0:
        sys.exit(return_code)
    else:
        (_OUT_DIR / "__init__.py").touch()


if __name__ == "__main__":
    main()
