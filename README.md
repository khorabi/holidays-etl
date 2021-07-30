# holidays-etl
workflow ETL em airflow com a API Calendarific

* Apache Airflow e Docker
    O workflow foi desenvolvido utilizando o apache airflow em um ambiente Docker.

    minha recomendação pessoal de início rápido em desenvolvimento airflow com docker é esse tutorial em inglês: https://www.youtube.com/watch?v=aTaytcxy2Ck (Running Airflow 2.0 with Docker in 5 mins - Marc Lamberti)

* pandas-gbq
    O pandas-gbq é uma biblioteca para execução de consultas e upload de DataFrames do pandas para o BigQuery.

    acesse a documentação da biblioteca: https://pandas-gbq.readthedocs.io/en/latest/
    

* Calendarific API
    Uma API pública com direito a 1000 requisições gratuitas, que tem como resposta os feriados ocorrendo na data e país especificado.

    acesse a documentação da API: https://calendarific.com/api-documentation

* Google Cloud Platform
    * BigQuery Dataset e Table
        O BigQuery foi o serviço de data warehousing utilizado para este projeto.

        acesse a documentação do google bigquery sobre criação de tabelas: https://cloud.google.com/bigquery/docs/tables

    * Conta de Serviço
        Foi utilizada nesse projeto uma conta de serviço da google cloud, com a conexão sendo realizada através de um arquivo .json com as devidas credenciais. 

        acesse a documentação google cloud sobre contas de serviço: https://cloud.google.com/iam/docs/service-accounts
    
