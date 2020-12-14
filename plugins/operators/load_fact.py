from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Loads data into the fact tables.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 load_sql,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql

    def execute(self, context):
        
        self.log.info('Start loading data..')
        
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        sql = f"""
            INSERT INTO {self.table}
            ({self.load_sql});
        """
        redshift.run(sql)