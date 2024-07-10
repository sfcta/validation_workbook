import subprocess
from pathlib import Path

time_periods = ["EA", "AM", "MD", "PM", "EV"]


def transit_assignment_filepaths(model_run_dir, time_periods=time_periods):
    model_run_dir = Path(model_run_dir)
    return [model_run_dir / f"SFALLMSA{t}.DBF" for t in time_periods]


def read_transit_assignments(model_run_dir, time_periods=time_periods):
    filepaths = transit_assignment_filepaths(model_run_dir, time_periods=time_periods)
    # TODO consolidate from each subscript to this script & pass as function arg instead
    raise NotImplementedError


if __name__ == "__main__":
    scripts = [
        "BART.py",
        "MUNI.py",
        "Screen.py",
        "Simwrapper_table.py",
        "map_data.py",
        "Obs.py",
        "total_val.py",
    ]

    for script_name in scripts:
        print(f"Running {script_name}")
        result = subprocess.run(["python", script_name], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Successfully ran {script_name}")
        else:
            print(f"Error running {script_name}: {result.stderr}")
