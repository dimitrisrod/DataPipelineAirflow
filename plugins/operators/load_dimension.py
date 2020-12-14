from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Loads data into the dimension tables.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id,
                 load_sql,
                 delete_load=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql
        self.delete_load: bool = delete_load

    def execute(self, context):
        self.log.info(f'Loading table {self.table} in Redshift.')
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        if self.delete_load == True:
            self.log.info('Truncate dimension table.')
            redshift.run("DELETE FROM {}".format(self.table))
        
        sql = f"""
            INSERT INTO {self.table}
            {self.load_sql};
        """

        redshift.run(sql)