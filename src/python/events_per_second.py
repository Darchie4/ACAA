import pandas as pd

def calculate_events_per_second(file_path):
    """
    Calculate the number of events processed per second from a CSV file
    and find the timestamp with the highest number of events.

    Args:
    - file_path (str): Path to the CSV file.

    Returns:
    - DataFrame: Events per second grouped by timestamp.
    - tuple: Timestamp and the maximum events per second.
    """
    # Load the CSV file
    df = pd.read_csv(file_path)

    # Convert timestamps to datetime with flexible parsing
    try:
        df['Created At'] = pd.to_datetime(df['Created At'], errors='coerce')
    except ValueError as e:
        print(f"Error parsing datetime: {e}")
        return

    # Check for rows with NaT (invalid timestamps)
    if df['Created At'].isna().any():
        print("Warning: Some timestamps could not be parsed. These rows will be excluded.")
        df = df.dropna(subset=['Created At'])

    # Group by each second and count the number of events
    df['Created At (Second)'] = df['Created At'].dt.floor('S')  # Round down to the nearest second
    events_per_second = df.groupby('Created At (Second)').size().reset_index(name='Events Per Second')

    # Find the maximum events per second and the corresponding timestamp
    max_events = events_per_second['Events Per Second'].max()
    max_timestamp = events_per_second.loc[events_per_second['Events Per Second'] == max_events, 'Created At (Second)'].iloc[0]

    # Display the results
    print(events_per_second)
    print(f"Maximum events per second: {max_events} at {max_timestamp}")

    return events_per_second, (max_timestamp, max_events)

# Example usage
file_path = "/home/root/code/logs/sensor_monitoring.csv"  # Replace with your actual CSV file path
events_per_second_df, max_events_info = calculate_events_per_second(file_path)
