import subprocess


lines = []
for pkg in ['gtfs-realtime-bindings', 'requests']:
    command = f'python -m pip install {pkg}'
    print(command)
    with subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    ) as proc:
        lines.extend(proc.stdout.readlines())

all_done = True
