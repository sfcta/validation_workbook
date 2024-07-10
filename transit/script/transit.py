import subprocess

if __name__ == "__main__":
    # required_packages = ['pandas==1.3.4', 'shapefile==0.7.2', 'xlrd==2.0.1', 'numpy==1.21.4', 'geopandas==0.14.2', 'sqlalchemy==1.4.27', 'tables==3.6.1']

    # # Get a list of installed packages
    # installed_packages = subprocess.check_output(['pip', 'freeze']).decode()

    # # Check if each required package is installed
    # missing_packages = []
    # for package in required_packages:
    #     if package not in installed_packages:
    #         missing_packages.append(package)

    # Install missing packages
    # if missing_packages:
    #     subprocess.check_call(['pip', 'install'] + missing_packages)

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
