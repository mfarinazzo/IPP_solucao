# IPP_solucao

Introdução
O processamento de grandes volumes de dados é uma tarefa comum em diversas áreas acadêmicas e profissionais. No contexto deste projeto, trabalhamos com arquivos CSV do CAGED (Cadastro Geral de Empregados e Desempregados), cuja análise requer o processamento eficiente de múltiplos arquivos para extrair informações relevantes, como médias salariais e faixas etárias. Inicialmente, o código responsável por essa tarefa era executado de forma sequencial, o que resultava em tempos de processamento elevados, especialmente quando o número de arquivos aumentava. Para superar essa limitação, decidimos implementar uma abordagem paralela utilizando a biblioteca Dask. Este relatório descreve as modificações realizadas no código original, a estratégia adotada para a paralelização com Dask e compara essa abordagem com o uso de MPI (Message Passing Interface), uma tecnologia conhecida na área de computação paralela.

# Abordagem Utilizada para Paralelizar o Código com Dask
A motivação principal para a paralelização do código foi reduzir significativamente o tempo de processamento dos arquivos CSV. O código original realizava as seguintes etapas de maneira sequencial: leitura dos arquivos, filtragem dos dados, cálculo de médias salariais e de idade, escrita dos resultados e execução de scripts adicionais para ajustes finais. Com o aumento do número de arquivos, esse processo tornava-se ineficiente.

Para implementar a paralelização, utilizamos a biblioteca Dask, que é uma ferramenta poderosa para computação paralela em Python. A principal estratégia adotada foi dividir o processamento de cada arquivo CSV em tarefas independentes que poderiam ser executadas simultaneamente. Isso foi feito decorando a função de processamento de arquivos com @delayed, transformando-a em uma tarefa que Dask pode gerenciar e executar em paralelo.

Primeiramente, configuramos um cluster local utilizando LocalCluster e Client do Dask, definindo o número de workers e threads por worker de acordo com os recursos disponíveis no sistema. Em seguida, criamos uma lista de tarefas onde cada tarefa corresponde ao processamento de um arquivo CSV específico. Essas tarefas foram então submetidas ao Dask para execução paralela usando a função compute, que coordena a distribuição das tarefas entre os diferentes workers do cluster.

Após a conclusão das tarefas, os resultados foram consolidados e escritos em arquivos de saída. Essa abordagem permitiu que múltiplos arquivos fossem processados simultaneamente, aproveitando melhor os recursos do processador e reduzindo significativamente o tempo total de execução.

# Vantagens da Abordagem com Dask
A implementação do Dask trouxe várias vantagens para o processamento dos arquivos CSV. Primeiramente, houve uma redução significativa no tempo total de execução, já que múltiplos arquivos eram processados simultaneamente. Além disso, a utilização de Dask permitiu uma melhor utilização dos recursos do sistema, aproveitando todos os núcleos disponíveis do processador sem a necessidade de configurações complexas.

Outra vantagem importante foi a facilidade de implementação. Diferentemente do MPI, que exige um conhecimento mais aprofundado sobre comunicação entre processos, o Dask permitiu que a paralelização fosse implementada com modificações mínimas no código original. Isso foi especialmente benéfico no contexto acadêmico, onde o foco é mais na análise dos dados do que na complexidade da infraestrutura de paralelização.

Além disso, Dask ofereceu uma maior flexibilidade e escalabilidade. Caso o número de arquivos aumentasse ou os recursos de hardware fossem ampliados, a configuração do cluster poderia ser facilmente ajustada para aproveitar os novos recursos sem a necessidade de reescrever o código.

# SPEED UP
Código comum:
Tempo de execução: 748.59 segundos

Código paralelizado:
Tempo de execução: 391.47 segundos

Um speedup de aproximadamente 1,91 indica que a versão paralelizada do código executou a tarefa quase 1,91 vezes mais rápido do que a versão sequencial. Em outras palavras, a paralelização proporcionou uma redução de tempo de cerca de 48,7%.

Estes dois tempos foram feitos com uma base de apenas 3 anos de arquivos.
