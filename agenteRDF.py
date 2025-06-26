import pandas as pd
import subprocess
from rdflib import Graph
from io import StringIO
import tempfile

def gerar_rdf_from_csvs(csv_path1: str) -> Graph:

    def ensure_csv(path: str) -> str:
        if path.lower().endswith(('.xls', '.xlsx')):
           
            df = pd.read_excel(path)
            temp_csv = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", newline="", encoding="utf-8")
            df.to_csv(temp_csv.name, index=False)
            return temp_csv.name
        return path
    
    csv_path = ensure_csv(csv_path1)
    
    try:
        df1 = pd.read_csv(csv_path, sep=';')
    except pd.errors.ParserError:
        df1 = pd.read_csv(csv_path, sep=',')
    
    csv_text = df1.to_csv(index=False)

    prompt = f"""
    Você é um agente semântico inteligente, especializado em transformar dados tabulares (como arquivos CSV) em grafos RDF conforme os padrões da Web Semântica.

    ### SUA TAREFA:

    Gere um arquivo no formato RDF/XML a partir de dados CSV disponibilizados, seguindo as instruções abaixo .

    ---

    ## INSTRUÇÕES GERAIS

    1. **Entidades e URIs**
    - A entidade principal é a **Biblioteca**.
    - Gere URIs únicas, legíveis e consistentes para cada biblioteca, preferencialmente no formato:
    `http://exemplo.com/biblioteca/id-nome-sem-espacos`
    - Ex: `http://exemplo.com/biblioteca/012345-Biblioteca-Central-UFBA`

    2. **Propriedades e Relacionamentos**
    - Mapeie os atributos do CSV como propriedades RDF adequadas.
    - Ex:
        - `ex:temNome` → nome da biblioteca
        - `ex:estaNoMunicipio` → cidade
        - `ex:localizadaEmUF` → estado
        - `ex:temCEP` → CEP
        - `ex:temNatureza` → natureza administrativa
        - `ex:temTipo` → tipo de biblioteca
        - `ex:temTelefone` → telefone
        - `ex:temEndereco` → logradouro ou endereço completo

    3. **Tipos e Literais**
    - Utilize os datatypes apropriados com `xsd`, como:
    - `xsd:string` para nomes, locais, endereços, naturezas etc.
    - `xsd:integer` para códigos numéricos
    - `xsd:decimal` para coordenadas, se existirem

    4. **Classificação (rdf:type)**
    - Toda biblioteca deve ser instanciada como:
    ```xml
    <rdf:type rdf:resource="http://exemplo.com/Biblioteca"/>

    5. **Namespaces obrigatórios** 
    - No início do arquivo RDF/XML, os seguintes prefixos devem estar declarados:
    xmlns:ex="http://exemplo.com/"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema#"
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"

    6. **Coerência**
    - Mantenha consistência nas URIs e propriedades.
    - Normalize nomes em URIs: remova acentos, use hífens no lugar de espaços e elimine caracteres especiais.

    ## ENTRADA (TABELA CSV)
    Arquivo: {csv_path}
    Conteúdo:
    {csv_text}

    ---

    ## SAÍDA 

    ### IMPORTANTE:
    - Responda apenas com o conteúdo RDF no formato RDF/XML válido.
    - Não use markdown (```) ou escritas fora da sintaxe, como "```xml```" .
    - Certifique-se de que o conteúdo gerado possa ser interpretado por um parser RDF/XML padrão.
    - Gere **todas** as entidades necessárias, ou seja, cada linha vai ser uma entidade .
    - O número de instâncias geradas tem de ser o mesmo número de linhas do arquivo csv .
    - Não gere instâncias repetidas .

    ### EXEMPLO DE SAÍDA ESPERADA
    <?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns:ex="http://exemplo.com/"
    xmlns:xsd="http://www.w3.org/2001/XMLSchema#">

    <!-- Biblioteca Pública Municipal de Abaíra -->
    <rdf:Description rdf:about="http://exemplo.com/biblioteca/1428-Biblioteca-Publica-Municipal-de-Abaira">
        <rdf:type rdf:resource="http://exemplo.com/Biblioteca"/>
        <ex:temNome>Biblioteca Pública Municipal de Abaíra</ex:temNome>
        <ex:temTipo>Municipal</ex:temTipo>
        <ex:estaNoMunicipio>Abaíra</ex:estaNoMunicipio>
        <ex:localizadaEmUF>BA</ex:localizadaEmUF>
        <ex:temRegiao>Nordeste</ex:temRegiao>
        <ex:temCEP rdf:datatype="xsd:string">46690-000</ex:temCEP>
        <ex:temTelefone rdf:datatype="xsd:string">(77)99142-3015</ex:temTelefone>
        <ex:temEmail>seceduabaira@yahoo.com.br</ex:temEmail>
        <ex:temEndereco>Rua Francisco Cardoso, s/n, Centro</ex:temEndereco>
        <ex:temLatitude rdf:datatype="xsd:decimal">-13.250075</ex:temLatitude>
        <ex:temLongitude rdf:datatype="xsd:decimal">-41.664868</ex:temLongitude>
    </rdf:Description>

    <!-- Biblioteca Pública Municipal Professora Maria das Mercês Alexandre -->
    <rdf:Description rdf:about="http://exemplo.com/biblioteca/1429-Biblioteca-Publica-Municipal-Professora-Maria-das-Merces-Alexandre">
        <rdf:type rdf:resource="http://exemplo.com/Biblioteca"/>
        <ex:temNome>Biblioteca Pública Municipal Professora Maria das Mercês Alexandre</ex:temNome>
        <ex:temTipo>Municipal</ex:temTipo>
        <ex:estaNoMunicipio>Abaré</ex:estaNoMunicipio>
        <ex:localizadaEmUF>BA</ex:localizadaEmUF>
        <ex:temRegiao>Nordeste</ex:temRegiao>
        <ex:temTelefone rdf:datatype="xsd:string">(75)3287-2470/(75)32872222/(75)999796833</ex:temTelefone>
        <ex:temEmail>bibliotecaabare@hotmail.com/cheino@hotmail.com</ex:temEmail>
        <ex:temEndereco>Rua José Amancio Filho, s/n, Centro</ex:temEndereco>
        <ex:temLatitude rdf:datatype="xsd:decimal">-8.722557</ex:temLatitude>
        <ex:temLongitude rdf:datatype="xsd:decimal">-39.115018</ex:temLongitude>
    </rdf:Description>

    <!-- Biblioteca Pública Municipal Machado de Assis -->
    <rdf:Description rdf:about="http://exemplo.com/biblioteca/1430-Biblioteca-Publica-Municipal-Machado-de-Assis">
        <rdf:type rdf:resource="http://exemplo.com/Biblioteca"/>
        <ex:temNome>Biblioteca Pública Municipal Machado de Assis</ex:temNome>
        <ex:temTipo>Municipal</ex:temTipo>
        <ex:estaNoMunicipio>Acajutiba</ex:estaNoMunicipio>
        <ex:localizadaEmUF>BA</ex:localizadaEmUF>
        <ex:temRegiao>Nordeste</ex:temRegiao>
        <ex:temTelefone rdf:datatype="xsd:string">(75)99857-5578</ex:temTelefone>
        <ex:temEmail>seduc.acajutiba@outlook.com</ex:temEmail>
        <ex:temEndereco>Praça Antônio da Costa Brito, s/n, Centro</ex:temEndereco>
        <ex:temLatitude rdf:datatype="xsd:decimal">-11.657425</ex:temLatitude>
        <ex:temLongitude rdf:datatype="xsd:decimal">-38.018257</ex:temLongitude>
    </rdf:Description>

    <!-- Biblioteca Pública Municipal Dejanira Virgens -->
    <rdf:Description rdf:about="http://exemplo.com/biblioteca/1431-Biblioteca-Publica-Municipal-Dejanira-Virgens">
        <rdf:type rdf:resource="http://exemplo.com/Biblioteca"/>
        <ex:temNome>Biblioteca Pública Municipal Dejanira Virgens</ex:temNome>
        <ex:temTipo>Municipal</ex:temTipo>
        <ex:estaNoMunicipio>Adustina</ex:estaNoMunicipio>
        <ex:localizadaEmUF>BA</ex:localizadaEmUF>
        <ex:temRegiao>Nordeste</ex:temRegiao>
        <ex:temTelefone rdf:datatype="xsd:string">(75)3496-2148/(75)3496-2140</ex:temTelefone>
        <ex:temEmail>bibliotecamunicipal@gmail.com</ex:temEmail>
        <ex:temEndereco>Rua Maria José Rabelo de Jesus, 167, Centro</ex:temEndereco>
        <ex:temLatitude rdf:datatype="xsd:decimal">-10.544119</ex:temLatitude>
        <ex:temLongitude rdf:datatype="xsd:decimal">-38.111617</ex:temLongitude>
    </rdf:Description>

</rdf:RDF>

    """
    # roda o modelo LLaMA 3 localmente com o prompt (ajustado para Ollama)
    result = subprocess.run(
        ["ollama", "run", "llama3"],
        input=prompt.encode("utf-8"),
        stdout=subprocess.PIPE
    )

    rdf_output = result.stdout.decode("utf-8")

    # cria um grafo RDF usando rdflib
    g = Graph()
    try:
        g.parse(data=rdf_output, format="application/rdf+xml")
    except Exception as e:
        print("Erro ao processar RDF:", e)
        print("Resposta da LLM:\n", rdf_output)
        return "retornou nada"

    return g

def main():
    print(gerar_rdf_from_csvs(r'C:\Users\cassi\projeto_ic\Agente\Data\bibliotecas_br_2023.csv'))

if __name__ == "__main__":
    main()
