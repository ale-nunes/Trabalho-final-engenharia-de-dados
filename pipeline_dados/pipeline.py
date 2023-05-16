from prefect_jupyter import notebook
from prefect import task, flow


@flow
def coleta_ingestao_dados():
    coleta_players = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\coleta de dados\Dados_NBA_Coleta_Player.ipynb"
    coleta_teams = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\coleta de dados\Dados_NBA_Coleta_Team.ipynb"
    
    ingestao_players = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\ingestao de dados\Ingestao_dados_NBA_player.ipynb"
    ingestao_team = r"C:\Users\aleci\Downloads\Files\Projeto Aplicado\Projeto\ingestao de dados\Ingestao_dados_NBA_team.ipynb"

    nb_coleta_players = notebook.execute_notebook(coleta_players)
    nb_coleta_teams = notebook.execute_notebook(coleta_teams)
    nb_ingestao_players = notebook.execute_notebook(ingestao_players)
    nb_ingestao_team = notebook.execute_notebook(ingestao_team)
    return nb_coleta_players, nb_coleta_teams, nb_ingestao_players, nb_ingestao_team


if __name__ == "__main__":
    coleta_ingestao_dados()
