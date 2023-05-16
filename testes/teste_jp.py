from prefect import flow
from prefect_jupyter import notebook
from prefect.deployments import Deployment


@flow(log_prints=True)
def example_execute_notebook():
    path = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\testes\teste_ingestao.ipynb"
    
    nb = notebook.execute_notebook(path)
    return nb


@flow
def deploy():
    deployment = Deployment.build_from_flow(
        flow=example_execute_notebook,
        name="prefect-example-deployment-teste"
    )
    deployment.apply()


if __name__ == "__main__":
    deploy()