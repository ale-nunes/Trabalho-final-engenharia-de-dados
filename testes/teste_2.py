from prefect_jupyter import notebook
from datetime import datetime, timedelta
from prefect import task, flow

# @flow
# def example_execute_notebook():
#     path = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\testes\teste_ingestao.ipynb"
    
#     nb = notebook.execute_notebook(path)
#     return nb

# @task
# def coleta_dados_players():
#     path = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\testes\teste_coleta_players.ipynb"
    
#     nb = notebook.execute_notebook(path)
#     return nb


# @task
# def coleta_dados_teams():
#     path = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\testes\Dados_NBA_Coleta_Team.ipynb"
    
#     nb = notebook.execute_notebook(path)
#     return nb


# @task
# def ingestao_dados_players():
#     path = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\testes\teste_ingestao.ipynb"
    
#     nb = notebook.execute_notebook(path)
#     return nb


# def ingestao_dados_teams():
#     path = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\testes\Ingestao_dados_NBA_team.ipynb"
    
#     nb = notebook.execute_notebook(path)
#     return nb



@flow
def ingestao_dados_players():
    path_1 = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\testes\teste_coleta_players.ipynb"
    path = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\testes\teste_ingestao.ipynb"
    
    nb_1 = notebook.execute_notebook(path_1)
    nb = notebook.execute_notebook(path)
    return nb_1, nb




if __name__ == "__main__":
    ingestao_dados_players()
