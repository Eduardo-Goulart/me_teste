### Readme

- Crie o ambiente do conda e instale as dependências com o comando abaixo:
```
conda env create -f environment.yml
```

- Altere as credenciais do banco de dados conforme suas configurações
```
    database = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = "",
        charset='utf8mb4'
    )
```

- Execute o arquivo "etl.py"
- O script criará o banco de dados, tabelas, inserir os dados e gerar os relatórios em ".csv"
