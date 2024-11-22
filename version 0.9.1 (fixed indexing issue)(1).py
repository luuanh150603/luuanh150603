import pandas as pd
import numpy as np
import sqlite3
import os
import zipfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi

# Path of kaggle.json:  /home/intro/.config/kaggle/kaggle.json

# Default arguments for the DAG
default_args = {
    'owner': 'Team_6.1',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
dag = DAG(
    'WeatherHistory_Project',
    default_args=default_args,
    description='ETL pipeline for the weather history',
    schedule_interval='@daily',
)

# File paths
csv_file_path = '/home/quynhanh_intro/airflow/datasets/weatherHistory.csv'
db_path = '/home/quynhanh_intro/airflow/databases/weather_data.db'

# Task 1: Extract data
def extract_data(**kwargs):
    # Set up Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Download the dataset file
    api.dataset_download_file('muthuj7/weather-dataset', file_name='weatherHistory.csv', path='/home/quynhanh_intro/airflow/datasets')

    # Define file paths
    downloaded_file_path = '/home/quynhanh_intro/airflow/datasets/weatherHistory.csv'
    zip_file_path = downloaded_file_path + '.zip'

    # Check if the downloaded file is a ZIP file
    if zipfile.is_zipfile(zip_file_path):
        # If it's a ZIP file, unzip it
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall('/home/quynhanh_intro/airflow/datasets')
        # Update file path to extracted CSV
        os.remove(zip_file_path)  # Optionally delete the ZIP file after extraction
    else:
        print("Downloaded file is not a ZIP archive, skipping extraction.")

    # Push the CSV file path to XCom for use in the next steps
    kwargs['ti'].xcom_push(key='csv_file_path', value=downloaded_file_path)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Transform data
def transform_data(**kwargs):
    # Retrieve file path from XCom
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    df = pd.read_csv(file_path)

    # Convert formatted date to datetime object, converting to utc and remove timezone info
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True)
    df['Formatted Date'] = df['Formatted Date'].dt.tz_localize(None)

    # Drop duplicate dates (if date and time of day are same)
    df.drop_duplicates(subset='Formatted Date', inplace=True)

    # Precip type has two different values, rain and snow
    # Fill NaN values with snow if temperature is 0 or below, rain otherwise
    fill_values = np.where(df['Temperature (C)'] <= 0, 'snow', 'rain')
    df['Precip Type'] = df['Precip Type'].fillna(pd.Series(fill_values, index=df.index))



    """ Replacing possible negative values with '?' in columns where value can't be negative,
     then replacing all '?' values with NaN, then replacing NaN with mode in columns with numerical data """

    # Columns to check for negative values
    columns_to_check = ['Humidity', 'Wind Speed (km/h)', 'Wind Bearing (degrees)', 'Visibility (km)', 'Pressure (millibars)']

    # Replace negative values with '?'
    df[columns_to_check] = df[columns_to_check].where(df[columns_to_check] >= 0, '?')

    # Replace '?' with NaN in the whole dataframe
    df.replace('?', pd.NA, inplace=True)

    # Columns where NaN should be replaced with mode
    columns_for_mode = columns_to_check + ['Temperature (C)', 'Apparent Temperature (C)']

    # Replace NaN with mode
    for column in columns_for_mode:
        mode_value = df[column].mode()[0]
        df[column] = df[column].fillna(mode_value)


    """ Adding new columns for current month and monthly mode """

    # Create new column for month
    df['Month'] = df['Formatted Date'].dt.month

    # Return mode if there's only one mode, NaN if there are multiple
    def calculate_mode(precip):
        modes = precip.mode()
        if len(modes) == 1:
            return modes.iloc[0]
        else:
            return pd.NA

    # Determine mode for each month
    monthly_mode = df.groupby('Month')['Precip Type'].apply(calculate_mode)

    # Create Mode column to original dataframe
    df['Mode'] = df['Month'].map(monthly_mode)


    """ Adding new column for wind strength """

    def determine_wind_strength(wind_speed):
        # Converts km/h to m/s and categorizes the wind strength 
        wind_speed_m_per_s = round(wind_speed / 3.6, 1)

        if wind_speed_m_per_s <= 1.5:
            return 'Calm'
        elif 1.6 <= wind_speed_m_per_s <= 3.3:
            return 'Light Air'
        elif 3.4 <= wind_speed_m_per_s <= 5.4:
            return 'Light Breeze'
        elif 5.5 <= wind_speed_m_per_s <= 7.9:
            return 'Gentle Breeze'
        elif 8.0 <= wind_speed_m_per_s <= 10.7:
            return 'Moderate Breeze'
        elif 10.8 <= wind_speed_m_per_s <= 13.8:
            return 'Fresh Breeze'
        elif 13.9 <= wind_speed_m_per_s <= 17.1:
            return 'Strong Breeze'
        elif 17.2 <= wind_speed_m_per_s <= 20.7:
            return 'Near Gale'
        elif 20.8 <= wind_speed_m_per_s <= 24.4:
            return 'Gale'
        elif 24.5 <= wind_speed_m_per_s <= 28.4:
            return 'Strong Gale'
        elif 28.5 <= wind_speed_m_per_s <= 32.6:
            return 'Storm'
        elif wind_speed_m_per_s >= 32.7:
            return 'Violent Storm'

    df['wind_strength'] = df['Wind Speed (km/h)'].apply(determine_wind_strength)


    """ Daily averages """

    # Create new column for date without time of day
    df['Date'] = df['Formatted Date'].dt.date

    # Create new dataframe for daily averages grouping by date
    daily_averages = df.groupby('Date')[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']].agg('mean').reset_index()

    # Drop columns not needed for daily table
    daily_avg_df = df.drop(['Daily Summary', 'Wind Bearing (degrees)', 'Loud Cover', 'Summary', 'Precip Type', 'Month', 'Mode'], axis=1)
    
    # Merge averages with new dataframe
    daily_avg_df = daily_avg_df.merge(
        daily_averages.rename(
            columns={
                'Temperature (C)': 'avg_temperature_c',
                'Humidity': 'avg_humidity',
                'Wind Speed (km/h)': 'avg_wind_speed_kmh'
            }
        ),
        on='Date',
        how='left'
    )

    # Rename columns for the table
    daily_avg_df = daily_avg_df.rename(
        columns={
            'Formatted Date': 'formatted_date',
            'Temperature (C)': 'temperature_c',
            'Apparent Temperature (C)': 'apparent_temperature_c',
            'Humidity': 'humidity',
            'Wind Speed (km/h)': 'wind_speed_kmh',
            'Visibility (km)': 'visibility_km',
            'Pressure (millibars)': 'pressure_millibars'
        }
    )

    # Drop the date column (not needed anymore)
    daily_avg_df.drop('Date', axis=1, inplace=True)

    # Sort by date
    daily_avg_df = daily_avg_df.sort_values(by='formatted_date').reset_index(drop=True)


    """ Monthly averages """

    # Create the new dataframe
    monthly_avg_df = df.groupby('Month')[['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)']].mean()

    # Extract each month's precip type mode
    mode_precip_type = df.groupby('Month')['Mode'].first()

    # Add the mode column
    monthly_avg_df['mode_precip_type'] = mode_precip_type

    # Reset index (currently the month column)
    monthly_avg_df = monthly_avg_df.reset_index()

    # Rename the columns appropriately
    monthly_avg_df = monthly_avg_df.rename(
        columns={
            'Month': 'month',
            'Temperature (C)': 'avg_temperature_c',
            'Apparent Temperature (C)': 'avg_apparent_temperature_c',
            'Humidity': 'avg_humidity',
            'Wind Speed (km/h)': 'avg_wind_speed_kmh',
            'Visibility (km)': 'avg_visibility_km',
            'Pressure (millibars)': 'avg_pressure_millibars',
        }
    )


    # Save the transformed data to new CSV files and pass paths to XCom
    daily_transformed_file_path = '/tmp/daily_transformed_weatherHistory.csv'
    monthly_transformed_file_path = '/tmp/monthly_transformed_weatherHistory.csv'

    daily_avg_df.to_csv(daily_transformed_file_path, index=False)
    monthly_avg_df.to_csv(monthly_transformed_file_path, index=False)

    kwargs['ti'].xcom_push(key='daily_transformed_file_path', value=daily_transformed_file_path)
    kwargs['ti'].xcom_push(key='monthly_transformed_file_path', value=monthly_transformed_file_path)


transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)


# Task 3: Validate data
def validate_data(**kwargs):
    # Retrieve file paths from XCom
    daily_transformed_file_path = kwargs['ti'].xcom_pull(key='daily_transformed_file_path')
    monthly_transformed_file_path = kwargs['ti'].xcom_pull(key='monthly_transformed_file_path')

    # Read files into dataframes
    daily_avg_df = pd.read_csv(daily_transformed_file_path)
    monthly_avg_df = pd.read_csv(monthly_transformed_file_path)

    # Critical columns to validate
    critical_columns = ['temperature_c', 'humidity', 'wind_speed_kmh']

    # Missing Values Check
    def check_missing_values(df, critical_columns):
        missing = df[critical_columns].isnull().sum()
        if missing.any():
            raise ValueError(f"Missing values found in critical columns: {missing.to_dict()}")
        print("No missing values in critical columns.")

    # Range Check
    def check_value_ranges(df):
        if not df['temperature_c'].between(-50, 50).all():
            raise ValueError("Temperature values are out of range (-50 to 50°C).")
        if not df['humidity'].between(0, 1).all():
            raise ValueError("Humidity values are out of range (0 to 1).")
        if not (df['wind_speed_kmh'] >= 0).all():
            raise ValueError("Wind Speed values must be non-negative.")
        print("All values are within the expected ranges.")

    # Outlier Detection
    def detect_outliers(df, column, threshold=3):
        mean = df[column].mean()
        std_dev = df[column].std()
        df['z_score'] = (df[column] - mean) / std_dev
        outliers = df[df['z_score'].abs() > threshold]

        if not outliers.empty:
            print(f"Outliers detected in {column}:")
            print(outliers[[column, 'z_score']])
        else:
            print(f"No outliers detected in {column}.")
        df.drop(columns=['z_score'], inplace=True)

    # Perform validation for daily averages
    print("Validating daily averages data...")
    check_missing_values(daily_avg_df, critical_columns)
    check_value_ranges(daily_avg_df)
    detect_outliers(daily_avg_df, 'temperature_c')
    detect_outliers(daily_avg_df, 'wind_speed_kmh')

    # Perform validation for monthly averages
    print("Validating monthly averages data...")
    check_missing_values(monthly_avg_df, critical_columns)
    check_value_ranges(monthly_avg_df)
    detect_outliers(monthly_avg_df, 'temperature_c')
    detect_outliers(monthly_avg_df, 'wind_speed_kmh')

    # Save validated data to files
    daily_validated_file_path = '/tmp/daily_validated_weatherHistory.csv'
    monthly_validated_file_path = '/tmp/monthly_validated_weatherHistory.csv'

    daily_avg_df.to_csv(daily_validated_file_path, index=False)
    monthly_avg_df.to_csv(monthly_validated_file_path, index=False)

    # Pass the validated file paths to XCom for the next task
    kwargs['ti'].xcom_push(key='daily_validated_file_path', value=daily_validated_file_path)
    kwargs['ti'].xcom_push(key='monthly_validated_file_path', value=monthly_validated_file_path)

    print("Validation completed successfully for both daily and monthly data.")

validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)


# Task 4: Load data
def load_data(**kwargs):
    # Retrieve validated file paths from XCom
    daily_validated_file_path = kwargs['ti'].xcom_pull(key='daily_validated_file_path')
    monthly_validated_file_path = kwargs['ti'].xcom_pull(key='monthly_validated_file_path')

    daily_avg_df = pd.read_csv(daily_validated_file_path)
    monthly_avg_df = pd.read_csv(monthly_validated_file_path)

    # Define conn and cursor
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Insert data into the database
    daily_avg_df.to_sql('daily_weather', conn, if_exists='replace', index=True, index_label='id')
    monthly_avg_df.to_sql('monthly_weather', conn, if_exists='replace', index=True, index_label='id')

    conn.commit()
    conn.cursor()
    conn.close()

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    trigger_rule='all_success',  # Ensures load_task only runs if validate_task is successful
    dag=dag,
)

# Set task dependencies with trigger rules
extract_task >> transform_task >> validate_task >> load_task
