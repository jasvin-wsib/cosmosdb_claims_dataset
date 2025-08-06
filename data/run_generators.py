import subprocess
import os

# Get the directory of this script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Define the generator script paths
scripts = [
    "claim_data_generator.py",
    "agent_data_generator.py",
    "claimant_data_generator.py"
]

# Run each script one by one
for script in scripts:
    script_path = os.path.join(script_dir, script)
    print(f"Running {script}...")
    result = subprocess.run(["python", script_path], capture_output=True, text=True)

    # Show output or error
    if result.returncode == 0:
        print(f"{script} ran successfully.")
        if result.stdout:
            print(result.stdout)
    else:
        print(f"Error running {script}:")
        print(result.stderr)
        break
