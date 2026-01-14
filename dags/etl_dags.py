from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Định nghĩa các tham số mặc định
default_args = {
    'owner': 'Huy_BigData',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Khởi tạo DAG
with DAG(
    'zillow_daily_batch_etl',
    default_args=default_args,
    description='Pipeline tự động chạy Spark Batch xử lý dữ liệu từ HDFS vào Cassandra',
    # Chạy vào lúc 00:05 mỗi ngày để đảm bảo dữ liệu ngày hôm trước đã được ghi đủ vào HDFS
    schedule_interval='5 0 * * *', 
    start_date=datetime(2025, 1, 1), # Bắt đầu từ đầu năm 2025
    catchup=False, # Không chạy bù các ngày trong quá khứ
    tags=['big_data', 'spark', 'real_time_btl']
) as dag:

    # 3. Định nghĩa Task chạy Spark Job
    # ds: là biến logic date của Airflow (YYYY-MM-DD). 
    # Nó sẽ được truyền vào sys.argv[1] của file python.
    
    # ĐƯỜNG DẪN CẦN KIỂM TRA KỸ TRÊN MÁY BẠN:
    PYTHON_ENV = "D:/BTL_Big_Data/Big_Data_20251/venv/Scripts/python.exe"
    SPARK_SCRIPT = "D:/BTL_Big_Data/Big_Data_20251/spark/batch_hdfs_to_cassandra.py"

    run_spark_job = BashOperator(
        task_id='spark_hdfs_to_cassandra',
        # Chạy file python bằng python trong venv và truyền ngày hiện tại của hệ thống Airflow
        bash_command=f'{PYTHON_ENV} {SPARK_SCRIPT} {{{{ ds }}}}'
    )

    # 4. Thiết lập luồng chạy (Trong bài này chỉ có 1 task nên không cần nối >>)
    run_spark_job