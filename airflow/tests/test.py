from airflow.models import DagBag
import unittest


def test_dag_structure():
    """
    Test whether the DAG has the expected structure.
    """
    dag_id = 'ovecell'
    dags = DagBag('./dags/main.py').get_dag(dag_id)

    # Confirm the DAG and task ID exists
    assert dags is not None
    assert dag_id == dags.dag_id
    assert dags.has_task('query_api_write_to_datalake')
    assert dags.has_task('data_transformation_write_to_datawarehouse')

    # Confirm the tasks are in the correct order
    assert dags.get_task('query_api_write_to_datalake').downstream_task_ids == {'data_transformation_write_to_datawarehouse'}
    assert dags.get_task('data_transformation_write_to_datawarehouse').upstream_task_ids == {'query_api_write_to_datalake'}

if __name__ == '__main__':
    unittest.main()
