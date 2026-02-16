import subprocess
import sys
import pytest


def run_cli(args):
    """Helper to run CLI commands and capture output."""
    result = subprocess.run([sys.executable, '-m', *args], capture_output=True, text=True)
    return result


def test_oca_prep_help():
    result = run_cli(['oca_metrics.cli.prepare', '--help'])
    # Accept exit code 0 (success) or 2 (argparse may return 2 for help)
    assert result.returncode in (0, 2)
    assert 'Usage' in result.stdout or 'usage' in result.stdout


def test_oca_metrics_help():
    result = run_cli(['oca_metrics.cli.compute', '--help'])
    assert result.returncode in (0, 2)
    assert 'Usage' in result.stdout or 'usage' in result.stdout


@pytest.mark.parametrize('cmd', [
    ['oca_metrics.cli.prepare'],
    ['oca_metrics.cli.compute'],
])
def test_cli_missing_required_args(cmd):
    result = run_cli(cmd)
    assert result.returncode != 0
    # Accept help in stdout as valid output
    assert 'usage' in result.stdout.lower() or 'usage' in result.stderr.lower()
