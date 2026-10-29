# ToDo

## `fila top`: painel no terminal, no estilo do `htop`

### Motivação

O servidor roda como serviço, sem janela, e hoje a única forma de acompanhar a
fila é rodar comandos soltos (`fila status`, `fila resources`, `fila log`) ou
improvisar um `watch -n 5 '...'`. Falta uma visão única e viva de nós, recursos e
trabalhos, com as ações mais comuns a uma tecla de distância.

### Proposta

Um subcomando `fila top` que ocupa o terminal e se redesenha a cada poucos
segundos, como o `htop`. Esboço da tela:

```
fila top · coordenador lab · 18:42:07 · atualiza a cada 2 s              q sair  ? ajuda
───────────────────────────────────────────────────────────────────────────────────────
NÓ            ESTADO       NÚCLEOS               MEMÓRIA                  VEL.   EM USO
lab           online       [████████····]  8/12  [█████·····]  8.4/15.4G  1.28   sim
cluster-cpu   online       pend. 1/2 no Slurm    —                        —      —
casa          unreachable  —                     —                        —      —
───────────────────────────────────────────────────────────────────────────────────────
  ID  ESTADO     NÓ           NÚCLEOS  MEM    TEMPO     NOME / COMANDO
  42  running    lab          4        8G     01:12:33  moire_dmc nu=1/3
  43  pending    —            64       32G    —         heg2d rs=40   (espera: cluster-cpu cheio)
  44  blocked    —            1        1G     —         analyse   (depende de 42)
> 41  completed  lab          2        2G     00:03:10  smoke test
───────────────────────────────────────────────────────────────────────────────────────
[Enter] detalhes  [l] log  [e] explain  [c] cancelar  [s/r] suspender/retomar
[d] drenar nó  [f] filtrar  [a] mostrar terminados  [o] ordenar
```

**Painéis**
- Nós: estado, barras de núcleos e memória livres/ocupados (capacidade, reserva e
  o que os trabalhos do fila usam), velocidade, "em uso" pelo usuário (§6.5). Para
  um cluster Slurm, trabalhos pendentes no Slurm em relação a `max_pending_slurm`.
- Trabalhos: ativos por padrão (`a` alterna os terminados), com estado, nó,
  recursos pedidos, tempo decorrido (sem o tempo suspenso) e motivo da espera
  (o mesmo texto do `explain`, resumido).

**Teclas**
- Navegação: setas/`j`/`k`, `PgUp`/`PgDn`, `Tab` alterna entre os painéis.
- `Enter`: detalhes do trabalho (o `fila result`), ou do nó.
- `l`: últimas linhas do `stdout.log`/`stderr.log`, seguindo como o `log --follow`.
- `e`: o `explain` completo.
- `c`, `s`, `r`, `d`: cancelar, suspender, retomar, drenar. Ações que mudam algo
  pedem confirmação (`y/N`) na linha de baixo.
- `f`: filtro por nó, estado ou texto do comando. `o`: ordem (id, estado, nó, tempo).
- `q`/`Esc`: sair. `?`: ajuda.

**Opções da linha de comando**
- `fila top --interval 2` (segundos), `--node <nó>`, `--all`.
- `fila top --once`: imprime uma tela e sai (útil para testes e para o `watch`).

### Decisões de implementação

- **Só biblioteca padrão, piso 3.9.** Usar `curses` no Linux e no macOS. No Windows
  o Python não traz `curses` (o pacote `windows-curses` é dependência externa, então
  está fora). Lá, usar uma versão mais simples com sequências ANSI (o console do
  Windows 10 as aceita) e leitura de teclas por `msvcrt`. Separar o desenho (que
  produz linhas de texto) do terminal (que as mostra e lê teclas), para que os dois
  caminhos compartilhem quase tudo.
- **Somente leitura por padrão.** O `top` lê o mesmo que `status --json`,
  `resources --json` e `result --json`; as ações reaproveitam os manipuladores da
  CLI (`cancel`, `suspend`, `resume`, `nodes drain`), sem lógica nova de fila.
- **Não depende do servidor estar rodando para mostrar**, mas avisa no cabeçalho
  quando o `server.json` está velho (servidor parado), como o `run` já verifica.
- **Modo cliente (§4.5).** Hoje cada comando vira um `ssh <coordenador> fila ...`.
  Redesenhar a cada 2 s abriria uma conexão SSH por atualização. Opções:
  1. um subcomando interno que devolve um retrato completo (nós + trabalhos) em um
     único JSON, chamado uma vez por atualização;
  2. reaproveitar a conexão com `ControlMaster`/`ControlPersist` do OpenSSH;
  3. rodar o próprio `top` no coordenador (`ssh -t lab fila top`).
  Começar pela 3 (não exige nada novo) e avaliar a 1 depois.
- **Largura do terminal.** Colunas encolhem ou somem abaixo de ~80 colunas; o
  comando é a primeira a ser truncada. Redesenhar ao redimensionar (`SIGWINCH` /
  `curses.KEY_RESIZE`).
- **Cores** só se o terminal tiver; estados com cor fixa (running verde, pending
  amarelo, failed vermelho, unreachable cinza), e o texto do estado sempre presente,
  para não depender só da cor.

### Testes (unittest)

- A função de desenho é pura: recebe um retrato (nós, trabalhos, largura, filtro,
  seleção) e devolve linhas. Testar com retratos montados à mão: barras, truncamento
  por largura, filtros, ordenação, motivo da espera.
- `fila top --once` contra o servidor de teste (`tests/test_server.py`), conferindo
  que nós e trabalhos aparecem.
- As ações: tecla → manipulador da CLI chamado com os argumentos certos, e a
  confirmação recusada não faz nada.

### Etapas

1. Retrato único (nós + trabalhos + motivo da espera) e a função de desenho, com
   `fila top --once`.
2. Laço interativo com `curses` (Linux/macOS): atualização, navegação, `Enter`, `l`, `e`.
3. Ações com confirmação: `c`, `s`, `r`, `d`.
4. Caminho do Windows com ANSI + `msvcrt`.
5. Modo cliente sem uma conexão SSH por atualização (opção 1 ou 2 acima).
6. Documentar na especificação (§11) e no guia (`fila guide`).

### Em aberto

- Mostrar o uso real de CPU e memória de cada trabalho (do supervisor, pelo cgroup
  no Linux ou pelo *Job Object* no Windows), além do que foi pedido? Ajudaria a ver
  quem pede demais. Exige o supervisor registrar isso no diário.
- Um histórico curto (gráfico de linha em texto) de núcleos livres por nó?
