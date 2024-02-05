import nox

DEFAULT_PYTHON_VERSION = "3.11"
BLACK_VERSION = "black==22.3.0"
LINT_PATHS = ["system", "noxfile.py"]


@nox.session(python=DEFAULT_PYTHON_VERSION)
def tests(session: nox.sessions.Session) -> None:
    # Install dependencies
    session.install("-r", "requirements.txt")

    # Run your integration tests
    session.run("pytest", "-s", "system/workflow-test.py")


@nox.session(python=DEFAULT_PYTHON_VERSION)
def blacken(session: nox.sessions.Session) -> None:
    """Run black. Format code to uniform standard."""
    session.install(BLACK_VERSION)
    session.run(
        "black",
        *LINT_PATHS,
    )


# Linting with flake8.
#
# We ignore the following rules:
#   E203: whitespace before ‘:’
#   E266: too many leading ‘#’ for block comment
#   E501: line too long
#   I202: Additional newline in a section of imports
#
# We also need to specify the rules which are ignored by default:
# ['E226', 'W504', 'E126', 'E123', 'W503', 'E24', 'E704', 'E121']
FLAKE8_COMMON_ARGS = [
    "--show-source",
    "--builtin=gettext",
    "--max-complexity=20",
    "--exclude=.nox,.cache,env,lib,generated_pb2,*_pb2.py,*_pb2_grpc.py",
    "--ignore=E121,E123,E126,E203,E226,E24,E266,E501,E704,W503,W504,I202",
    "--max-line-length=88",
]


@nox.session(python=DEFAULT_PYTHON_VERSION)
def lint(session: nox.sessions.Session) -> None:
    session.install("flake8", "flake8-annotations")

    args = FLAKE8_COMMON_ARGS + [
        ".",
    ]
    session.run("flake8", *args)
