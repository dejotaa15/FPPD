# T1: Problemas Clássicos de Concorrência

FPPD — Fundamentos de Processamento Paralelo e Distribuído (98713-04)

Grupo: João Pedro Bianchi, Gustavo Both, Antônio Costa

## Estrutura

```
filosofos/              Problema 1: Jantar dos Filósofos
  main.go               parâmetros, goroutine do filósofo, vigia de deadlock, estatísticas
  mesa.go               garfos (channels) e estado compartilhado
  estrategias.go        base, hierarquia e limite
  garcom.go             arbitragem centralizada (processo garçom)
produtor-consumidor/    Problema 2: Produtor/Consumidor com buffer limitado
  main.go               parâmetros, produtores, consumidores, monitor, verificação
  buffer.go             interface Buffer e versão com channel
  semaforo.go           semáforo de contagem e versão com buffer circular
relatorio/relatorio.pdf Relatório
experimentos.ps1        Roda os experimentos do relatório
resultados/             Saída dos experimentos usados no relatório
```

## Requisitos

Go 1.21 ou mais novo. Não há dependências externas.

No Windows, o detector de data race (`-race`) precisa de um compilador C (gcc do MinGW-w64) no `PATH`.
No Linux e no macOS basta o Go.

## Problema 1: Jantar dos Filósofos

```
go run -race ./filosofos -n 5 -r 100 -estrategia base
go run -race ./filosofos -n 5 -r 100 -estrategia hierarquia
go run -race ./filosofos -n 5 -r 100 -estrategia limite
go run -race ./filosofos -n 5 -r 100 -estrategia garcom
```

| Parâmetro     | Padrão | Significado |
|---------------|--------|-------------|
| `-n`          | 5      | número de filósofos (N) |
| `-r`          | 100    | número de iterações (R) |
| `-estrategia` | base   | `base` (deadlock), `hierarquia`, `limite` ou `garcom` |
| `-disputa`    | false  | as N·R refeições são disputadas entre todos, em vez de R para cada um |
| `-pensar`     | 1ms    | tempo máximo pensando |
| `-comer`      | 1ms    | tempo máximo comendo |
| `-pausa`      | 1ms    | pausa entre o primeiro e o segundo garfo |
| `-vigia`      | 2s     | tempo sem refeições para declarar deadlock |
| `-csv`        | false  | imprime só uma linha de resumo |

A versão `base` entra em deadlock; o programa detecta, mostra o ciclo de espera e encerra.

## Problema 2: Produtor/Consumidor

```
go run -race ./produtor-consumidor -p 4 -c 4 -k 10 -itens 10000 -versao channel
go run -race ./produtor-consumidor -p 4 -c 4 -k 10 -itens 10000 -versao semaforo
```

Para ver o consumo com timeout em ação (produtores lentos):

```
go run -race ./produtor-consumidor -p 2 -c 3 -k 4 -itens 50 -versao semaforo -tprod 40ms -timeout 10ms -v
```

| Parâmetro  | Padrão  | Significado |
|------------|---------|-------------|
| `-p`       | 4       | número de produtores (P) |
| `-c`       | 4       | número de consumidores (C) |
| `-k`       | 10      | capacidade do buffer (K) |
| `-itens`   | 1000    | itens produzidos por cada produtor |
| `-versao`  | channel | `channel` ou `semaforo` |
| `-tprod`   | 0       | tempo máximo para produzir um item |
| `-tcons`   | 0       | tempo máximo para consumir um item |
| `-timeout` | 50ms    | espera do consumidor antes do comportamento alternativo |
| `-amostra` | 200µs   | intervalo de amostragem da ocupação |
| `-v`       | false   | mostra cada timeout |
| `-csv`     | false   | imprime só uma linha de resumo |

Ao final o programa verifica que o total consumido é igual ao produzido e que nenhum item foi perdido ou duplicado.

## Reproduzir os experimentos do relatório

```
powershell -ExecutionPolicy Bypass -File .\experimentos.ps1
```
