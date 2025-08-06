import json
import os

def split_json_by_field(input_file, output_dir, field_name, prefix=None):
    """
    Splits a JSON file of records into individual files named by a given field.

    Args:
        input_file (str): Path to the JSON file (array or newline-delimited JSON).
        output_dir (str): Directory where split files will be saved.
        field_name (str): The field whose value will be used in file names.
        prefix (str, optional): Prefix for output file names (defaults to field_name).

    Returns:
        list[str]: List of full paths to files created.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Read the JSON file
    with open(input_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
            if not isinstance(data, list):
                raise ValueError("JSON file does not contain a list at the top level.")
        except json.JSONDecodeError:
            f.seek(0)
            data = [json.loads(line) for line in f if line.strip()]

    if prefix is None:
        prefix = field_name

    created_files = []
    for record in data:
        field_value = record.get(field_name)
        if field_value is None:
            raise ValueError(f"Record missing '{field_name}': {record}")

        # sanitize filename component a bit
        filename_value = str(field_value).replace(os.path.sep, "_")
        output_path = os.path.join(output_dir, f"{prefix}_{filename_value}.json")
        with open(output_path, "w", encoding="utf-8") as out_f:
            json.dump(record, out_f, ensure_ascii=False, indent=2)
        created_files.append(os.path.abspath(output_path))

    print(f"Split {len(data)} records into '{os.path.abspath(output_dir)}' using '{field_name}' in filenames.")
    return created_files


if __name__ == "__main__":
    # Make paths relative to the script file, not the current working directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # For agents
    agent_input = os.path.join(script_dir, "agent_data.json")
    agent_output = os.path.join(script_dir, "agent_data")
    split_json_by_field(agent_input, agent_output, "agent_id", prefix="agent")

    # For claims
    claim_input = os.path.join(script_dir, "claim_data.json")
    claim_output = os.path.join(script_dir, "claim_data")
    split_json_by_field(claim_input, claim_output, "claim_id", prefix="claim")

    # For claimants
    claimant_input = os.path.join(script_dir, "claimant_data.json")
    claimant_output = os.path.join(script_dir, "claimant_data")
    split_json_by_field(claimant_input, claimant_output, "claimant_id", prefix="claimant")
