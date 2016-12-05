from datetime import datetime, timedelta
import json


from airflow.hooks import HttpHook, PostgresHook
from airflow.operators import PythonOperator
from airflow.models import DAG


def get_rates(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='clover')


    # These are the only valid pairs the DB supports at the moment. Anything
    # else that turns up will be ignored.
    valid_pairs = (
        'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS',
        'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN',
        'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BRL', 'BSD',
        'BTC', 'BTN', 'BWP', 'BYN', 'BYR', 'BZD', 'CAD',
        'CDF', 'CHF', 'CLF', 'CLP', 'CNY', 'COP', 'CRC',
        'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP',
        'DZD', 'EEK', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD',
        'FKP', 'GBP', 'GEL', 'GGP', 'GHS', 'GIP', 'GMD',
        'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG',
        'HUF', 'IDR', 'ILS', 'IMP', 'INR', 'IQD', 'IRR',
        'ISK', 'JEP', 'JMD', 'JOD', 'JPY', 'KES', 'KGS',
        'KHR', 'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT',
        'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL',
        'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT',
        'MOP', 'MRO', 'MTL', 'MUR', 'MVR', 'MWK', 'MXN',
        'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR',
        'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR',
        'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF',
        'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP',
        'SLL', 'SOS', 'SRD', 'STD', 'SVC', 'SYP', 'SZL',
        'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD',
        'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'UYU', 'UZS',
        'VEF', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU',
        'XCD', 'XDR', 'XOF', 'XPD', 'XPF', 'XPT', 'YER',
        'ZAR', 'ZMK', 'ZMW', 'ZWL')

    rates_insert = """INSERT INTO rates (pair, valid_until, rate)
                      VALUES ('ZAR', '2016-11-12', '99');"""

    pg_hook.run(rates_insert)


args = {
    'owner': 'mars',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Run at the top of the hour Monday to Friday.
# Note: This doesn't line up with the market hours of
# 10PM Sunday till 10PM Friday GMT.
dag = DAG(dag_id='rates',
          default_args=args,
          schedule_interval='0 * * * 1,2,3,4,5',
          dagrun_timeout=timedelta(seconds=30))

get_rates_task = PythonOperator(task_id='get_rates',
                                provide_context=True,
                                python_callable=get_rates,
                                dag=dag)
