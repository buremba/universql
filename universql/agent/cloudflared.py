import atexit
import logging
import sys

import click
import requests
import subprocess
import tarfile
import tempfile
import shutil
import os
import platform
import time
import re
from pathlib import Path

CLOUDFLARED_CONFIG = {
    ('Windows', 'AMD64'): {
        'command': 'cloudflared-windows-amd64.exe',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe'
    },
    ('Windows', 'x86'): {
        'command': 'cloudflared-windows-386.exe',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-386.exe'
    },
    ('Linux', 'x86_64'): {
        'command': 'cloudflared-linux-amd64',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64'
    },
    ('Linux', 'i386'): {
        'command': 'cloudflared-linux-386',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-386'
    },
    ('Linux', 'arm'): {
        'command': 'cloudflared-linux-arm',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm'
    },
    ('Linux', 'arm64'): {
        'command': 'cloudflared-linux-arm64',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64'
    },
    ('Linux', 'aarch64'): {
        'command': 'cloudflared-linux-arm64',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64'
    },
    ('Darwin', 'x86_64'): {
        'command': 'cloudflared',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-amd64.tgz'
    },
    ('Darwin', 'arm64'): {
        'command': 'cloudflared',
        'url': 'https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-amd64.tgz'
    }
}


def _get_command(system, machine):
    try:
        return CLOUDFLARED_CONFIG[(system, machine)]['command']
    except KeyError:
        raise Exception(f"{machine} is not supported on {system}")


def _get_url(system, machine):
    try:
        return CLOUDFLARED_CONFIG[(system, machine)]['url']
    except KeyError:
        raise Exception(f"{machine} is not supported on {system}")


# Needed for the darwin package
def _extract_tarball(tar_path, filename):
    tar = tarfile.open(tar_path + '/' + filename, 'r')
    for item in tar:
        tar.extract(item, tar_path)
        if item.name.find(".tgz") != -1 or item.name.find(".tar") != -1:
            tar.extract(item.name, "./" + item.name[:item.name.rfind('/')])


def _download_cloudflared(cloudflared_path, command):
    system, machine = platform.system(), platform.machine()
    if Path(cloudflared_path, command).exists():
        executable = (cloudflared_path + '/' + 'cloudflared') if (
                system == "Darwin" and machine in ["x86_64", "arm64"]) else (cloudflared_path + '/' + command)
        update_cloudflared = subprocess.Popen([executable, 'update'], stdout=subprocess.DEVNULL,
                                              stderr=subprocess.STDOUT)
        return
    print(f" * Downloading cloudflared for {system} {machine}...")
    url = _get_url(system, machine)
    _download_file(url)


def _download_file(url):
    local_filename = url.split('/')[-1]
    r = requests.get(url, stream=True)
    download_path = str(Path(tempfile.gettempdir(), local_filename))
    with open(download_path, 'wb') as f:
        shutil.copyfileobj(r.raw, f)
    return download_path


def start_cloudflared(port, metrics_port, tunnel_id=None, config_path=None):
    system, machine = platform.system(), platform.machine()
    command = _get_command(system, machine)
    tempdir_for_app = tempfile.gettempdir()
    cloudflared_path = str(Path(tempdir_for_app))
    if system == "Darwin":
        _download_cloudflared(cloudflared_path, "cloudflared-darwin-amd64.tgz")
        _extract_tarball(cloudflared_path, "cloudflared-darwin-amd64.tgz")
    else:
        _download_cloudflared(cloudflared_path, command)

    executable = str(Path(cloudflared_path, command))
    os.chmod(executable, 0o777)

    cloudflared_command = [executable, 'tunnel', '--metrics', f'127.0.0.1:{metrics_port}', '--logfile',
                           str(Path(tempdir_for_app, 'cloudflared.log'))]
    if config_path:
        cloudflared_command += ['--config', config_path, 'run']
    elif tunnel_id:
        cloudflared_command += ['--url', f'http://127.0.0.1:{port}', 'run', tunnel_id]
    else:
        cloudflared_command += ['--url', f'http://127.0.0.1:{port}']

    if system == "Darwin" and machine == "arm64":
        x___cloudflared_command = ['arch', '-x86_64'] + cloudflared_command
        logging.debug('Running command: '+' '.join(x___cloudflared_command))
        cloudflared = subprocess.Popen(x___cloudflared_command, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
    else:
        cloudflared = subprocess.Popen(cloudflared_command, stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
    atexit.register(cloudflared.terminate)


def get_cloudflare_url(metrics_port, tunnel_id=None, config_path=None):
    localhost_url = f"http://127.0.0.1:{metrics_port}/metrics"

    for i in range(10):
        try:
            metrics = requests.get(localhost_url).text
            if tunnel_id or config_path:
                # If tunnel_id or config_path is provided, we check for cloudflared_tunnel_ha_connections, as no tunnel URL is available in the metrics
                if re.search("cloudflared_tunnel_ha_connections\s\d", metrics):
                    # No tunnel URL is available in the metrics, so we return a generic text
                    tunnel_url = "preconfigured tunnel URL"
                    break
            else:
                # If neither tunnel_id nor config_path is provided, we check for the tunnel URL in the metrics
                tunnel_url = (re.search("https?:\/\/(?P<url>[^\s]+.trycloudflare.com)", metrics).group("url"))
                break
        except:
            click.secho(f"Waiting for cloudflared to generate the tunnel URL... {i * 3}s", fg="yellow")
            time.sleep(3)
    else:
        click.secho(
            f"Can't connect to cloudflared tunnel, check logs at {str(Path(tempfile.gettempdir(), 'cloudflared.log'))} and restart the server")
        sys.exit(0)

    return tunnel_url
