# Roda os experimentos usados no relatório e grava os resultados em resultados/.
# Uso (PowerShell, na raiz do repositório):  .\experimentos.ps1

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force bin, resultados | Out-Null

go build -o bin\filosofos.exe .\filosofos
go build -o bin\pc.exe .\produtor-consumidor

# ---------- Problema 1: Jantar dos Filósofos ----------
$detalhe = "resultados\filosofos-detalhe.txt"
"" | Out-File $detalhe -Encoding utf8

# Versão base: com e sem a pausa entre os garfos.
.\bin\filosofos.exe -n 5 -r 1000 -estrategia base | Out-File $detalhe -Append -Encoding utf8
.\bin\filosofos.exe -n 5 -r 1000 -estrategia base -pausa 0 | Out-File $detalhe -Append -Encoding utf8

# Estratégias: R refeições para cada um (mede a espera) e N*R em disputa (mede a distribuição).
foreach ($e in "hierarquia", "limite", "garcom") {
    .\bin\filosofos.exe -n 5 -r 1000 -estrategia $e | Out-File $detalhe -Append -Encoding utf8
    .\bin\filosofos.exe -n 5 -r 1000 -estrategia $e -disputa | Out-File $detalhe -Append -Encoding utf8
}

# ---------- Problema 2: Produtor/Consumidor ----------
$csv = "resultados\produtor-consumidor.csv"
"cenario,versao,p,c,k,itens,tempo_ms,vazao_itens_s,ocup_media,ocup_max,inserir_us,retirar_us,timeouts,cons_min,cons_max,ok" |
    Out-File $csv -Encoding utf8

foreach ($v in "channel", "semaforo") {
    foreach ($k in 1, 10, 100) {
        foreach ($rep in 1..5) {
            # Cenário A: sem trabalho simulado, mede só o custo da sincronização.
            "A," + (.\bin\pc.exe -p 4 -c 4 -k $k -itens 250000 -versao $v -csv) | Out-File $csv -Append -Encoding utf8
            # Cenário B: produção e consumo com tempos aleatórios (rajadas).
            "B," + (.\bin\pc.exe -p 4 -c 4 -k $k -itens 500 -versao $v -tprod 2ms -tcons 2ms -csv) | Out-File $csv -Append -Encoding utf8
        }
    }
}

Write-Host "Resultados em resultados\"
