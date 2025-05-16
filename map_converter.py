import json
import os
import argparse # Import the module for command-line arguments

def convert_simulation_data(simulation_config_path, output_path, docker_path_prefix=None, host_path_prefix=None):
    """
    Reads a simulation configuration file, processes the specified node and link files,
    and generates a consolidated GPSMap.json file.
    Allows mapping Docker path prefixes to the host.

    Args:
        simulation_config_path (str): The path to the simulation configuration JSON file.
        output_path (str): The path where the GPSMap.json file will be saved.
        docker_path_prefix (str, optional): The path prefix inside the Docker container. Defaults to None.
        host_path_prefix (str, optional): The corresponding path prefix on the host system. Defaults to None.
    """
    all_vertices = []
    all_edges = []
    node_files_processed = []
    link_files_processed = []
    files_not_found = []
    json_errors = []
    key_errors = []

    print(f"Reading configuration file: {simulation_config_path}")
    try:
        with open(simulation_config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"CRITICAL Error: Configuration file '{simulation_config_path}' not found.")
        return
    except json.JSONDecodeError:
        print(f"CRITICAL Error: Failed to decode JSON in file '{simulation_config_path}'. Check the format.")
        return
    except Exception as e:
        print(f"CRITICAL unexpected error reading configuration file: {e}")
        return

    if "actorsDataSources" not in config:
        print("CRITICAL Error: Key 'actorsDataSources' not found in the configuration file.")
        return

    # --- Process each data source ---
    for data_source_info in config.get("actorsDataSources", []):
        class_type = data_source_info.get("classType", "")
        resource_id = data_source_info.get("id", None) # This will be the shardId
        source_details = data_source_info.get("dataSource", {}).get("info", {})
        original_file_path = source_details.get("path", None) # Path as it is in the JSON

        if not resource_id or not original_file_path:
            print(f"Warning: Skipping entry in actorsDataSources without 'id' or 'path': {data_source_info}")
            continue

        # --- Docker -> Host Path Mapping ---
        file_path = original_file_path
        if docker_path_prefix and host_path_prefix and original_file_path.startswith(docker_path_prefix):
            # Replace the Docker prefix with the Host prefix
            # The '1' ensures only the first occurrence (the prefix) is replaced
            file_path = original_file_path.replace(docker_path_prefix, host_path_prefix, 1)
            print(f"Mapping: '{original_file_path}' -> '{file_path}'")
        else:
            # If no mapping is needed or the path doesn't start with the prefix, use it as is
            print(f"Using path as is: '{file_path}'")
            pass # Use file_path as read from config

        # --- Processing Node Files ---
        if "mobility.actor.Node" in class_type:
            print(f"--> Processing node file: {file_path} (Shard ID: {resource_id})")
            node_files_processed.append(original_file_path) # Log the original path
            try:
                with open(file_path, 'r', encoding='utf-8') as node_file:
                    nodes_data = json.load(node_file)
                    if not isinstance(nodes_data, list):
                       print(f"  Warning: Content of {file_path} is not a JSON list. Skipping.")
                       continue

                    for node in nodes_data:
                        try:
                            vertex = {
                                "id": node["id"],
                                "classType": node["typeActor"],
                                "resourceId": resource_id, # Use the resource ID as shardId
                                "latitude": node["data"]["content"]["latitude"],
                                "longitude": node["data"]["content"]["longitude"]
                            }
                            all_vertices.append(vertex)
                        except KeyError as e:
                            err_msg = f"Key error '{e}' in node {node.get('id', 'unknown ID')} in {file_path}"
                            print(f"  {err_msg}")
                            key_errors.append(err_msg)
                        except Exception as e:
                             print(f"  Unexpected error processing node in {file_path}: {e}. Node: {node}")

            except FileNotFoundError:
                err_msg = f"Node file not found: '{file_path}' (Original: '{original_file_path}')"
                print(f"  Error: {err_msg}")
                files_not_found.append(err_msg)
            except json.JSONDecodeError:
                err_msg = f"Failed to decode JSON in node file: '{file_path}'"
                print(f"  Error: {err_msg}")
                json_errors.append(err_msg)
            except Exception as e:
                print(f"  Unexpected error processing node file '{file_path}': {e}")

        # --- Processing Link Files ---
        elif "mobility.actor.Link" in class_type:
            print(f"--> Processing link file: {file_path} (Shard ID: {resource_id})")
            link_files_processed.append(original_file_path) # Log the original path
            try:
                with open(file_path, 'r', encoding='utf-8') as link_file:
                    links_data = json.load(link_file)
                    if not isinstance(links_data, list):
                       print(f"  Warning: Content of {file_path} is not a JSON list. Skipping.")
                       continue

                    for link in links_data:
                        try:
                            # Extract node IDs from 'dependencies' if 'data.content' doesn't have them
                            # (The example shows them in data.content, but having a fallback is good practice)
                            source_node_id = link.get("data", {}).get("content", {}).get("from_node") \
                                             or link.get("dependencies", {}).get("from_node", {}).get("id")
                            target_node_id = link.get("data", {}).get("content", {}).get("to_node") \
                                             or link.get("dependencies", {}).get("to_node", {}).get("id")
                            link_id = link["id"]
                            length_str = link["data"]["content"]["length"]
                            length_float = float(length_str) # Convert to float

                            if not source_node_id or not target_node_id:
                                print(f"  Error: Could not find source or target node ID in link {link_id} in {file_path}.")
                                continue

                            edge = {
                                "source_id": source_node_id,
                                "target_id": target_node_id,
                                "weight": length_float, # Use float length as weight
                                "label": {
                                    "id": link_id,
                                    "resourceId": resource_id, # Use the resource ID as shardId
                                    "classType": class_type,
                                    "length": length_float # Store float length in label as well
                                }
                            }
                            all_edges.append(edge)
                        except KeyError as e:
                            err_msg = f"Key error '{e}' in link {link.get('id', 'unknown ID')} in {file_path}"
                            print(f"  {err_msg}")
                            key_errors.append(err_msg)
                        except ValueError:
                             err_msg = f"Could not convert 'length' to float in link {link.get('id', 'unknown ID')} in {file_path}. Value: '{length_str}'"
                             print(f"  Error: {err_msg}")
                             key_errors.append(err_msg) # Add to error log
                        except Exception as e:
                             print(f"  Unexpected error processing link in {file_path}: {e}. Link: {link}")

            except FileNotFoundError:
                err_msg = f"Link file not found: '{file_path}' (Original: '{original_file_path}')"
                print(f"  Error: {err_msg}")
                files_not_found.append(err_msg)
            except json.JSONDecodeError:
                err_msg = f"Failed to decode JSON in link file: '{file_path}'"
                print(f"  Error: {err_msg}")
                json_errors.append(err_msg)
            except Exception as e:
                print(f"  Unexpected error processing link file '{file_path}': {e}")
        else:
            # Ignore other class types
            print(f"Skipping unsupported actor type: {class_type} (File: {original_file_path})")


    # --- Error Report ---
    if files_not_found:
        print("\n--- Errors: Files Not Found ---")
        for msg in files_not_found:
            print(msg)
    if json_errors:
        print("\n--- Errors: JSON Read Failure ---")
        for msg in json_errors:
            print(msg)
    if key_errors:
        print("\n--- Errors: Missing or Invalid Keys in Data ---")
        for msg in key_errors:
            print(msg)

    # --- Build the final GPSMap structure ---
    gps_map_data = {
        "nodes": all_vertices,
        "edges": all_edges,
        "directed": False # Set to false as per the example
    }

    # --- Save the output file ---
    print(f"\n--- Summary ---")
    print(f"Node files processed (original paths): {len(node_files_processed)}")
    print(f"Link files processed (original paths): {len(link_files_processed)}")
    print(f"Total vertices generated: {len(all_vertices)}")
    print(f"Total edges generated: {len(all_edges)}")

    if not all_vertices and not all_edges and (files_not_found or json_errors or key_errors):
         print("\nNo data was generated due to previous errors. Output file will not be created.")
         return # Don't create an empty file if there were critical errors

    try:
        # Ensure the output directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Output directory created: {output_dir}")

        with open(output_path, 'w', encoding='utf-8') as outfile:
            # Use indent for pretty printing, ensure_ascii=False for non-ASCII chars if any
            json.dump(gps_map_data, outfile, indent=4, ensure_ascii=False)
        print(f"\nFile '{output_path}' generated successfully!")
    except IOError as e:
        print(f"\nCRITICAL Error: Could not write output file '{output_path}': {e}")
    except Exception as e:
        print(f"\nCRITICAL unexpected error saving output file: {e}")

# --- Command Line Argument Setup ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Converts simulation data (nodes and links) to GPSMap.json format, "
                    "allowing mapping of Docker volume paths to the host.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Shows default values in help message
    )

    parser.add_argument(
        "config_file",
        help="Path to the simulation configuration JSON file."
    )
    parser.add_argument(
        "output_file",
        nargs='?', # Makes the argument optional
        default="GPSMap.json", # Default value if not provided
        help="Path for the output GPSMap.json file."
    )
    parser.add_argument(
        "--docker-prefix",
        dest="docker_prefix", # Variable name to store the value
        metavar="DOCKER_PREFIX",
        help="The path prefix for files *inside* the Docker container to be replaced (e.g., /app/data/)."
    )
    parser.add_argument(
        "--host-prefix",
        dest="host_prefix",
        metavar="HOST_PREFIX",
        help="The corresponding path prefix on the *host* system (e.g., /home/user/my_project/data/)."
    )

    args = parser.parse_args()

    # Simple validation: if one prefix is provided, the other must be too
    if (args.docker_prefix and not args.host_prefix) or (not args.docker_prefix and args.host_prefix):
        parser.error("If you provide --docker-prefix, you must also provide --host-prefix, and vice-versa.")

    # Check if the configuration file exists
    if not os.path.exists(args.config_file):
        print(f"Error: The specified configuration file '{args.config_file}' was not found.")
        exit(1) # Exit the script with an error code

    # Call the main function with the arguments
    convert_simulation_data(
        args.config_file,
        args.output_file,
        args.docker_prefix,
        args.host_prefix
    )