import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os


def load_alert_data(file_path):
    """Load alert data into a DataFrame and calculate Time Difference in milliseconds."""
    # Ensure 'Created At' and 'Consumed At' are parsed as datetime
    data = pd.read_csv(file_path)
    data['Created At'] = pd.to_datetime(data['Created At'], errors='coerce')
    data['Consumed At'] = pd.to_datetime(data['Consumed At'], errors='coerce')

    # Drop rows with invalid datetime parsing
    data.dropna(subset=['Created At', 'Consumed At'], inplace=True)

    # Calculate Time Difference in milliseconds
    data['Time Difference (milliseconds)'] = (
        (data['Consumed At'] - data['Created At']).dt.total_seconds() * 1000
    )
    return data

def create_boxplot(data, output_dir="plots"):
    """Create and save boxplots for Time Difference (milliseconds)."""
    os.makedirs(output_dir, exist_ok=True)

    # Overall Boxplot
    plt.figure(figsize=(10, 6))
    plt.boxplot(data['Time Difference (milliseconds)'], vert=False, patch_artist=True)
    plt.title("Boxplot of Time Difference (milliseconds)")
    plt.xlabel("Time Difference (milliseconds)")
    plt.savefig(os.path.join(output_dir, "time_difference_milliseconds_boxplot.png"))
    plt.close()

    # Boxplot Grouped by Sensor ID
    plt.figure(figsize=(10, 6))
    data.boxplot(column='Time Difference (milliseconds)', by='Sensor ID', grid=False, patch_artist=True)
    plt.title("Boxplot of Time Difference by Sensor ID")
    plt.suptitle("")  # Remove default super title
    plt.xlabel("Sensor ID")
    plt.ylabel("Time Difference (milliseconds)")
    plt.savefig(os.path.join(output_dir, "time_difference_by_sensor_boxplot.png"))
    plt.close()


def create_histogram(data, output_dir="plots"):
    """Create and save a histogram for Time Difference (milliseconds)."""
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    plt.hist(data['Time Difference (milliseconds)'], bins=50, edgecolor='black')
    plt.title("Histogram of Time Difference (milliseconds)")
    plt.xlabel("Time Difference (milliseconds)")
    plt.ylabel("Frequency")
    plt.savefig(os.path.join(output_dir, "time_difference_histogram.png"))
    plt.close()


def create_density_plot(data, output_dir="plots"):
    """Create and save a density plot for Time Difference (milliseconds)."""
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    data['Time Difference (milliseconds)'].plot(kind='density')
    plt.title("Density Plot of Time Difference (milliseconds)")
    plt.xlabel("Time Difference (milliseconds)")
    plt.ylabel("Density")
    plt.savefig(os.path.join(output_dir, "time_difference_density_plot.png"))
    plt.close()


def create_scatter_plot(data, output_dir="plots"):
    """Create and save a scatter plot for Sensor ID vs Time Difference."""
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    plt.scatter(data['Sensor ID'], data['Time Difference (milliseconds)'], alpha=0.6)
    plt.title("Scatter Plot of Sensor ID vs Time Difference")
    plt.xlabel("Sensor ID")
    plt.ylabel("Time Difference (milliseconds)")
    plt.savefig(os.path.join(output_dir, "scatter_sensor_time_difference.png"))
    plt.close()


def create_time_series(data, output_dir="plots"):
    """Create and save a time series plot for Time Difference."""
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    data.sort_values('Created At', inplace=True)
    plt.plot(data['Created At'], data['Time Difference (milliseconds)'], alpha=0.8)
    plt.title("Time Series of Time Difference")
    plt.xlabel("Created At")
    plt.ylabel("Time Difference (milliseconds)")
    plt.savefig(os.path.join(output_dir, "time_series_time_difference.png"))
    plt.close()


def create_heatmap(data, output_dir="plots"):
    """Create and save a heatmap for Average Time Difference by Sensor ID."""
    os.makedirs(output_dir, exist_ok=True)
    grouped = data.groupby('Sensor ID')['Time Difference (milliseconds)'].mean().reset_index()
    plt.figure(figsize=(10, 6))
    sns.heatmap(grouped.pivot_table(index='Sensor ID', values='Time Difference (milliseconds)'), annot=True, fmt=".2f")
    plt.title("Heatmap of Average Time Difference by Sensor ID")
    plt.xlabel("Sensor ID")
    plt.ylabel("Average Time Difference (milliseconds)")
    plt.savefig(os.path.join(output_dir, "heatmap_sensor_time_difference.png"))
    plt.close()


def create_violin_plot(data, output_dir="plots"):
    """Create and save a violin plot for Time Difference by Sensor ID."""
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    sns.violinplot(x='Sensor ID', y='Time Difference (milliseconds)', data=data, scale='width')
    plt.title("Violin Plot of Time Difference by Sensor ID")
    plt.xlabel("Sensor ID")
    plt.ylabel("Time Difference (milliseconds)")
    plt.savefig(os.path.join(output_dir, "violin_plot_sensor_time_difference.png"))
    plt.close()


def create_cdf_plot(data, output_dir="plots"):
    """Create and save a cumulative distribution function (CDF) plot."""
    os.makedirs(output_dir, exist_ok=True)
    plt.figure(figsize=(10, 6))
    sorted_data = data['Time Difference (milliseconds)'].sort_values()
    plt.plot(sorted_data, sorted_data.rank(pct=True))
    plt.title("Cumulative Distribution Function (CDF) of Time Difference")
    plt.xlabel("Time Difference (milliseconds)")
    plt.ylabel("Cumulative Probability")
    plt.savefig(os.path.join(output_dir, "time_difference_cdf.png"))
    plt.close()

def remove_outliers(data, column):
    """Remove outliers from the specified column using the IQR method."""
    Q1 = data[column].quantile(0.25)
    Q3 = data[column].quantile(0.75)
    IQR = Q3 - Q1

    # Define the bounds
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Filter the data
    filtered_data = data[(data[column] >= lower_bound) & (data[column] <= upper_bound)]
    print(f"Outliers removed: {len(data) - len(filtered_data)}")
    return filtered_data

def main(file_path, output_dir="plots"):
    """Main function to load data and generate all plots."""
    # Load alert data
    data = load_alert_data(file_path)

    # Print basic statistics
    print("Statistics for Time Difference (milliseconds):")
    print(data['Time Difference (milliseconds)'].describe())
    
        # Remove outliers
    data = remove_outliers(data, 'Time Difference (milliseconds)')
    
        # Print basic statistics after outlier removal
    print("\nStatistics for Time Difference (milliseconds) after outlier removal:")
    print(data['Time Difference (milliseconds)'].describe())

    # Generate all plots
    create_boxplot(data, output_dir)
    create_histogram(data, output_dir)
    create_density_plot(data, output_dir)
    create_scatter_plot(data, output_dir)
    create_time_series(data, output_dir)
    create_heatmap(data, output_dir)
    create_violin_plot(data, output_dir)
    create_cdf_plot(data, output_dir)

    print(f"All plots saved in the directory: {output_dir}")
    
    


# Run the analysis
file_path = "logs/alert.csv"  # Path to your alert data CSV
main(file_path)
