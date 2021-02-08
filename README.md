### Readme

- Crie o ambiente do conda e instale as dependências com o comando abaixo:
```
conda env create -f environment.yml
```

- Altere as credenciais do banco de dados conforme suas configurações no arquivo "database.py" 
```
    database = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = ""
    )
```

- Altere as credenciais do banco de dados conforme suas configurações no arquivo "etl.py"
```
    database = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = ""
    )
```
- Obs.: Em razão do tamanho do arquivo, o "logs.txt" não está no repositório.
- Execute o arquivo "etl.py"
- O script criará o banco de dados, tabelas, inserir os dados e gerar os relatórios em ".csv"
