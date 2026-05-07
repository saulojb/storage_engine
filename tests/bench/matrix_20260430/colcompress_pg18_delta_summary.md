# PG18 colcompress release note

Comparacao da implementacao atual contra os arquivos baseline `copy`, focada apenas em `colcompress`. Valores negativos indicam melhoria de tempo.

## Texto pronto para changelog

O rerun do benchmark sem `citus` em PG18 mostrou que a implementacao atual de agregacao vetorizada trouxe ganho claro para `colcompress` em execucao paralela, com reducoes expressivas de tempo em consultas analiticas de agregacao, filtros com `GIN` e scans textuais. O melhor resultado apareceu em `Q2 SUM/AVG numeric+double`, que caiu de `143.981 ms` para `33.642 ms` (`-76.63%`), seguido por `Q7 JSONB key + GROUP BY` (`316.957 ms -> 77.908 ms`, `-75.42%`), `Q6 JSONB @> GIN` (`198.173 ms -> 53.563 ms`, `-72.97%`), `Q8 array @> GIN` (`146.534 ms -> 41.573 ms`, `-71.63%`), `Q9 LIKE text scan` (`120.920 ms -> 34.586 ms`, `-71.40%`) e `Q10 heavy multi-agg` (`1929.479 ms -> 706.758 ms`, `-63.37%`).

No caminho serial, o efeito ficou misto: houve melhora relevante em `Q2 SUM/AVG numeric+double` (`126.623 ms -> 92.916 ms`, `-26.62%`) e ganhos menores em `Q9 LIKE text scan` (`-4.62%`) e `Q6 JSONB @> GIN` (`-1.62%`), enquanto as demais consultas oscilaram levemente para cima, com pior caso em `Q1 count(*)` (`+6.72%`). Em paralelo, a unica regressao observada foi `Q3 GROUP BY country (10)` (`154.945 ms -> 164.079 ms`, `+5.89%`), enquanto todo o restante da matriz paralela melhorou.

## Tabela completa

### Serial

| Query | Baseline (ms) | Atual (ms) | Delta |
|---|---:|---:|---:|
| Q1 count(*) | 5.058 | 5.398 | +6.72% |
| Q2 SUM/AVG numeric+double | 126.623 | 92.916 | -26.62% |
| Q3 GROUP BY country (10) | 154.970 | 158.461 | +2.25% |
| Q4 GROUP BY event_type+p95 | 447.776 | 476.388 | +6.39% |
| Q5 date range 1 month | 23.380 | 24.154 | +3.31% |
| Q6 JSONB @> GIN | 194.282 | 191.135 | -1.62% |
| Q7 JSONB key + GROUP BY | 325.158 | 334.826 | +2.97% |
| Q8 array @> GIN | 137.916 | 139.113 | +0.87% |
| Q9 LIKE text scan | 117.131 | 111.717 | -4.62% |
| Q10 heavy multi-agg | 1888.054 | 1965.014 | +4.08% |

### Paralelo

| Query | Baseline (ms) | Atual (ms) | Delta |
|---|---:|---:|---:|
| Q1 count(*) | 21.245 | 14.018 | -34.02% |
| Q2 SUM/AVG numeric+double | 143.981 | 33.642 | -76.63% |
| Q3 GROUP BY country (10) | 154.945 | 164.079 | +5.89% |
| Q4 GROUP BY event_type+p95 | 459.492 | 326.657 | -28.91% |
| Q5 date range 1 month | 39.525 | 31.832 | -19.46% |
| Q6 JSONB @> GIN | 198.173 | 53.563 | -72.97% |
| Q7 JSONB key + GROUP BY | 316.957 | 77.908 | -75.42% |
| Q8 array @> GIN | 146.534 | 41.573 | -71.63% |
| Q9 LIKE text scan | 120.920 | 34.586 | -71.40% |
| Q10 heavy multi-agg | 1929.479 | 706.758 | -63.37% |
