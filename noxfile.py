import nox

nox.options.sessions = ["lint", "test", "mypy", "pytype"]
locations = ["fs", "tests", "noxfile.py"]


@nox.session
def test(session):
    session.install("pytest")
    session.install(".")
    session.run("pytest", "-m", "not creds")


@nox.session
def mypy(session):
    args = session.posargs or locations
    session.install("mypy")
    session.install(".")
    session.run("mypy", *args)


@nox.session
def pytype(session):
    args = session.posargs or locations
    session.install("pytype")
    session.install(".")
    session.run("pytype", *args)


@nox.session
def lint(session):
    args = session.posargs or locations
    session.install("flake8", "black", "isort")
    session.run("flake8", *args)
    session.run("black", "--check", "--diff", *args)
    session.run("isort", "--profile", "black", "--check", "--diff", *args)


@nox.session
def format(session):
    black(session)
    isort(session)


def black(session):
    args = session.posargs or locations
    session.install("black")
    session.run("black", *args)


def isort(session):
    args = session.posargs or locations
    session.install("isort")
    session.run("isort", "--profile", "black", *args)
