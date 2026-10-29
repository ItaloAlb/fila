# Especificação: `fila`, um gerenciador de fila e recursos para nós não-locais

## 1. Objetivo e escopo

O `fila` junta máquinas que **não compartilham rede, computador nem cluster** num único conjunto de recursos. Ele gerencia uma fila de trabalhos e aloca núcleos e memória nesses nós. Faz o papel de um cluster caseiro, formado por máquinas em lugares diferentes.

Como o Slurm, o `fila` é **alheio aos comandos que executa**. Ele não sabe o que é git, compilação, Monte Carlo ou qualquer programa específico. Recebe um comando opaco e os recursos declarados, decide onde e quando rodar, impõe os limites, acompanha o trabalho até o fim e registra o resultado.

O `fila` **não** faz:
- gerenciar repositórios, sincronizar versões ou compilar;
- mover arquivos de entrada ou de saída (os resultados ficam no nó, como no Slurm);
- interpretar o conteúdo dos comandos.

Essas tarefas ficam com quem usa o `fila` (você ou o Claude Code), com comandos comuns executados por ele.

Outras características:
- **Um único usuário.** Não há contas, cotas nem divisão justa de recursos entre pessoas. O campo `created_by` só distingue você do Claude Code.
- **Idioma.** O programa (código, comentários, subcomandos, opções, chaves de configuração, estados, campos JSON e mensagens) é escrito em inglês. A documentação para o usuário fica em português.
- **Uso pelo Claude Code.** Primeiro pela linha de comando; depois como ferramenta registrada (Model Context Protocol).

O HTCondor foi descartado porque exige configuração pesada e ajuda do administrador do cluster.

## 2. Conceitos

| Termo | Significado |
|---|---|
| **Coordenador** | Papel, não máquina. É o processo `fila server`, que mantém a fila, decide e lança. Roda em qualquer computador (Linux, Windows ou macOS), escolhido pelo usuário. |
| **Nó** | Um recurso cadastrado no coordenador. É um **computador** ou um **cluster Slurm**. |
| **Nó do Slurm** | Uma máquina de cálculo dentro de um cluster (por exemplo, `apollo05`). Não confundir com nó do `fila`. |
| **Trabalho** | Algo que o `fila` executa. Pode ser imediato (`run`) ou enfileirado (`submit`). |
| **Tentativa** | Uma execução de um trabalho. Um trabalho pode ter várias tentativas, todas com o mesmo `id`. |
| **Agente** | O `fila` instalado num computador, que sonda recursos, lança e supervisiona trabalhos. |

A máquina do coordenador **só entra na soma de recursos se for cadastrada** como nó. Se não for, ela só coordena.

O usuário decide onde fica cada coisa: qual máquina coordena, quais são nós e como estão ligadas. O programa não impõe nenhum arranjo.

## 3. Nós

### 3.1 Computadores

Um computador tem o agente do `fila` instalado e aceita os dois modos de execução (`run` e `submit`). O campo `access` diz como **este** coordenador chega até ele:

| `access` | Como funciona |
|---|---|
| `local` | É a própria máquina do coordenador. |
| `ssh` | O coordenador alcança o computador por SSH. |
| `pull` | O coordenador **não** alcança o computador. É o computador quem abre e mantém um canal com o coordenador (§4.3). Serve para máquinas atrás de NAT ou de firewall. |

### 3.2 Clusters Slurm

Um cluster **não tem agente**: nenhum processo do `fila` roda nele, nem Python. Isso evita depender de permissão do administrador para processos permanentes no nó de login.

- **Vínculo a um computador.** O cluster é sempre ligado a um computador (`via`), que pode ser qualquer computador cadastrado ou a própria máquina do coordenador. O agente desse computador faz tudo no cluster por SSH, com comandos comuns:
  - `sinfo`, `scontrol` e `sacctmgr` para sondar e ler limites;
  - envio do script e `sbatch --parsable` para submeter;
  - `squeue` e `sacct` para acompanhar;
  - `scancel` para cancelar;
  - `tail` para ler logs.
- **Só aceita `submit`.** Um `run --on <cluster>` é recusado. Para mexer no nó de login (por exemplo, `git pull`), use o shell do computador vinculado: `fila run --on <via> "ssh <alias> '...'"`.
- **Partição.** Cada nó cluster é um par (cluster, partição). Para usar duas partições do mesmo cluster, cadastre dois nós com o mesmo alias.
- **Um trabalho ocupa um único nó do Slurm** (`--nodes=1`). Rodar MPI em vários nós do Slurm fica de fora.

### 3.3 Regra de alcance e rede

Para cadastrar um computador, é preciso que o coordenador o alcance (`ssh`) ou que ele alcance o coordenador (`pull`). Se o coordenador estiver numa máquina sem portas abertas, nenhum computador pode ser `pull` em relação a ela.

**Toda a parte de rede fica com o OpenSSH.** O cadastro informa só um alias. VPN, `ProxyJump`, portas e chaves ficam no `~/.ssh/config` da máquina que faz a conexão: a do coordenador para computadores `ssh`, e a do computador vinculado para clusters. Exemplo de passagem por outra máquina:

```
Host cluster
    ProxyJump lab
```

### 3.4 Cadastro

O cadastro fica num arquivo TOML na máquina do coordenador. Ele pode ser editado à mão e depois conferido com `fila nodes validate`.

| Campo | Computador | Cluster | Descrição |
|---|---|---|---|
| `type` | `computer` | `slurm` | Tipo do nó |
| `access` | obrigatório | não se aplica | `local`, `ssh` ou `pull` |
| `host` | se `ssh` | obrigatório | Alias SSH (para cluster, como o computador `via` o enxerga) |
| `via` | não se aplica | obrigatório | Computador que controla o cluster |
| `os` | obrigatório | não se aplica | `linux`, `windows` ou `macos` |
| `shell` | **obrigatório** | **obrigatório** | Shell que interpreta os comandos, sem valor padrão (§5.1) |
| `python` | opcional | não se aplica | Caminho do interpretador, se o `python3` do sistema for antigo |
| `jobs_dir` | obrigatório | obrigatório | Diretório dos trabalhos no nó (§10) |
| `min_free_disk` | opcional | opcional | Abaixo disso, o nó não recebe trabalhos novos |
| `limits` | opcional | opcional | Teto de recursos que o `fila` usa no nó (`cores`, `mem`). Sem ele, vale o hardware ou o que o Slurm informa |
| `reserve.always` | opcional | não se aplica | Margem que o `fila` nunca ocupa |
| `reserve.in_use` | opcional | não se aplica | Margem enquanto alguém usa a máquina (§6.5) |
| `on_use` | opcional | não se aplica | `keep` (padrão) ou `suspend` |
| `idle_minutes` | opcional | não se aplica | Minutos sem teclado ou mouse para considerar a máquina livre (padrão 5) |
| `default_hint` | opcional | opcional | `multithread` (padrão) ou `nomultithread` |
| `speed` | medido ou manual | medido ou manual | Fator de velocidade por núcleo (§7.4) |
| `partition`, `account`, `qos` | não se aplica | `partition` obrigatório | Opções padrão do Slurm |
| `exclude` | não se aplica | opcional | Nós do Slurm a evitar por padrão |
| `max_pending_slurm` | não se aplica | opcional | K da política do Slurm (§7.5), padrão 2 |
| `max_time` | opcional | opcional | Tempo máximo de um trabalho no nó |

Valores gerais, válidos para todo o coordenador:

```toml
[general]
probe_interval_s  = 30
starvation_after  = "1h"     # T da proteção contra fome (§7.2)
run_timeout       = "10m"    # tempo máximo padrão de um run
json_tail_kb      = 200      # fim da saída devolvido no --json
```

### 3.5 Registro

```
fila nodes register <nome> --type computer --access local|ssh|pull --os ... --shell "..." [opções]
fila nodes register <nome> --type slurm --via <computador> --host <alias> --partition <p> --shell "..." [opções]
fila nodes list | remove <nome> | drain <nome> | resume <nome> | validate [<nome>]
fila nodes benchmark <nome>|--all [--command "..."]
fila nodes update <nome>|--all
```

O `register` **valida antes de aceitar**. Se qualquer verificação falhar, o nó não entra.

- **Computador `ssh` ou `local`:**
  1. testa a conexão;
  2. confere a versão do Python;
  3. copia o agente;
  4. roda `<shell> 'echo ok'`;
  5. sonda o hardware (núcleos físicos e lógicos, memória, disco);
  6. roda o teste de velocidade embutido.
- **Computador `pull`:** o cadastro é um **pareamento**.
  1. `fila nodes register` imprime a chave e as instruções.
  2. A chave pública do computador entra no `authorized_keys` do coordenador, restrita ao canal daquele nó:
     ```
     restrict,command="fila internal channel --node <nome>" ssh-ed25519 AAAA... <nome>
     ```
  3. No computador, `fila worker install --coordinator <usuario@host> --node <nome>` registra o serviço e grava a impressão digital do SSH do coordenador.
  4. O cadastro termina quando o canal abre pela primeira vez e as verificações acima rodam por ele.

  **Como ficou, na ordem em que se faz:**
  1. **No nó:** `fila worker install --coordinator <usuario@host> --node <nome>`. Ele cria uma chave própria (`~/.fila/id_ed25519`), copia o agente (`~/.fila/fila.pyz`), grava `~/.fila/worker.toml` e instala o serviço `fila-worker`. O comando também imprime a linha que deve ser rodada no coordenador.
     - O SSH do worker usa `StrictHostKeyChecking=accept-new` com um `known_hosts` próprio: a chave do coordenador é gravada na primeira conexão, e uma chave diferente depois é recusada.
  2. **No coordenador:** `fila nodes register <nome> --access pull --key "<chave pública>" --shell ... --jobs-dir ...`. Ele acrescenta ao `~/.ssh/authorized_keys` a linha `restrict,command="<python> <home>/fila_launcher.py internal channel --node <nome>" <chave>` e espera até `--wait` segundos (30 por padrão) o canal abrir, para rodar as verificações. Detalhes:
     - Se o nó ainda não conectou, o cadastro fica salvo, e as verificações rodam depois com `fila nodes validate <nome>`.
     - Cadastrar de novo substitui a linha da mesma chave, sem duplicar.
     - O `restrict` exige OpenSSH 7.2 ou mais novo.
- **Cluster:**
  1. testa `ssh <host>` a partir do computador `via`;
  2. lê `scontrol show partition` (`MaxCPUsPerNode`, `MaxNodes`, `MaxTime`) e `sacctmgr show qos` (`MaxTRES`, `MaxTRESPerUser`);
  3. lê `sinfo -N` (CPUs e memória de cada nó do Slurm da partição);
  4. confere o `shell`.

  Os limites lidos podem ser sobrescritos por `limits` e `max_time` no cadastro.

`drain` funciona como no Slurm: o nó não recebe trabalhos novos, e os que estão rodando continuam. `resume` volta ao normal.

### 3.6 Estados do nó

| Estado | Significado |
|---|---|
| `online` | Alcançável e aceitando trabalhos. |
| `unreachable` | Sem contato. No `pull`, o sinal de vida falhou por mais de 45 s. No cluster, o computador `via` está sem contato ou o SSH falha. |
| `drained` | Pausado por `nodes drain`. |
| `outdated` | A versão do protocolo do agente não confere com a do coordenador. Não recebe trabalhos até `nodes update`. |
| `low_disk` | O espaço livre em `jobs_dir` está abaixo de `min_free_disk`. Os trabalhos em execução continuam; novos não entram. |

## 4. Arquitetura

```
                       [ coordenador: fila server + SQLite + cadastro ]
                         │ local        │ SSH            ↑ canal SSH aberto pelo nó
                         ▼              ▼                │
              [ computador local ]  [ computador ssh ]  [ computador pull ]
                                        │ SSH (sem agente)
                                        ▼
                                  [ cluster Slurm ]
```

### 4.1 Servidor

- O `fila server` é o **único processo que decide e lança**. Os comandos da CLI (`submit`, `cancel`, `run`...) gravam o pedido no SQLite e avisam o servidor por um mecanismo local, sem rede. Isso evita que dois processos lancem o mesmo trabalho.
- **Ciclo.** A cada ciclo, o servidor:
  1. sonda os nós;
  2. confere quem terminou (diários, `squeue`/`sacct`);
  3. libera dependentes;
  4. aplica o agendamento (§7).
- **Serviço permanente do sistema:**
  - `systemd --user`, com `loginctl enable-linger`, no Linux;
  - `launchd` no macOS;
  - Agendador de Tarefas no Windows.

  `fila server --install` (e `--uninstall`, com `--dry-run` para só mostrar os passos) monta o serviço. Todos rodam `<home>/server_launcher.py`, que sabe onde estão o código e o `FILA_HOME` e manda a saída para `<home>/server.log`. Detalhes por sistema:
  - Linux: unidade em `~/.config/systemd/user/fila-server.service` com `Restart=on-failure`, `systemctl --user enable --now` e `loginctl enable-linger`.
  - macOS: *LaunchAgent* em `~/Library/LaunchAgents/org.fila.fila-server.plist`, com `KeepAlive`.
  - Windows: tarefa agendada definida por XML, que inicia no logon na sessão do usuário (`InteractiveToken`), com `pythonw.exe` (sem janela), **sem limite de execução** (`PT0S`; o padrão do Windows derrubaria o servidor depois de 72 h) e reinício em caso de falha.
- **Um ciclo com erro não para o servidor.** O erro vai para `server.log`, e o ciclo seguinte tenta de novo.
- **Reconciliação ao iniciar.** Para cada trabalho marcado como `running`, o servidor confere o diário do computador ou o `sacct` do cluster. Nada é relançado por engano.
- **Relógio.** Os horários registrados vêm do relógio do coordenador.
- **Banco.** SQLite em modo WAL, com permissão só do usuário. O histórico é mantido para sempre.
- **Acordar.** A CLI (`submit`, `run`, `cancel`, `retry`, `drain`...) e o supervisor, ao terminar uma tentativa, tocam o arquivo `<home>/wake`. O servidor faz um ciclo na hora, sem esperar o intervalo. Assim, um trabalho curto termina e libera os dependentes em segundos.
- **Sinal de vida.** A cada ciclo, o servidor grava `<home>/server.json` (PID e horário). O `run` só aceita o pedido se esse sinal for recente; senão, sai com o código 6.
- **Servidor parado.** O `submit` continua gravando, e o trabalho fica `pending` até o servidor voltar. O `run` falha com uma mensagem clara.

### 4.2 Agente

- O mesmo programa (`fila.pyz`, um `zipapp`) roda em todos os computadores. Não precisa de `pip` nem de ambiente virtual: basta Python 3.9 ou mais novo.
- Usa **só a biblioteca padrão**. Um leitor TOML escrito só em Python (`tomli`) vai embutido, porque o `tomllib` só existe a partir do 3.11.
- **Sondagem por sistema:**
  - `/proc` no Linux;
  - `sysctl`/`vm_stat` no macOS;
  - `ctypes` no Windows (`GlobalMemoryStatusEx`, `GetSystemTimes`, `GetLogicalProcessorInformation`).
- **Versão de protocolo.** Ao conectar, o coordenador e o agente comparam a versão. Se não conferir, o nó fica `outdated`. O `fila nodes update` copia o agente novo para os computadores `ssh`, e os computadores `pull` o baixam pelo canal.
- **Operações.** Tudo o que acontece dentro de um computador é uma operação do agente: `version`, `probe`, `cycle` (sondagem mais os diários pedidos), `launch`, `request`, `read_log`, `clean`, `check_shell`, `benchmark`, `time_command` e `selftest`. Num nó `local`, o coordenador as chama diretamente. Num nó `ssh`, chama `fila internal agent <operação>`, com os argumentos em JSON na entrada padrão e a resposta `{"ok", "result" | "error"}` na saída.
- **No nó `ssh`**, o agente fica em `~/.fila/fila.pyz`. A cópia passa pelo próprio SSH (`cat > ...`), sem `scp`. Cada operação roda como `cd && <python> .fila/fila.pyz internal agent <op>`. O `python` do cadastro é uma palavra do shell remoto (`python3`, `$HOME/venvs/x/bin/python`), e o shell de login do nó precisa ser POSIX.
- **Supervisor por trabalho.** Cada trabalho num computador é lançado por um processo supervisor próprio (`fila internal supervise`), **separado da sessão SSH e do canal**. O supervisor:
  - aplica afinidade e limites (§6.3);
  - vigia memória e tempo;
  - executa suspensão e retomada;
  - grava o diário.

  Por isso, a queda do canal, do SSH ou do próprio agente não mata o trabalho.

### 4.3 Transporte

- **`local`:** chamadas diretas.
- **`ssh`:** o coordenador roda `fila internal agent <operação>` por SSH, com **uma chamada por nó e por ciclo** (`cycle`: sondagem e diários juntos), mais as chamadas de lançamento e de pedidos quando houver. O comando SSH fica em `ssh_command`, em `[general]`. O padrão é `ssh -o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=3`, que nunca pede senha e desiste de conexões mortas. O tempo máximo de cada chamada é `ssh_timeout_s`, 60 s por padrão. Recomenda-se `ControlMaster`/`ControlPersist` no `~/.ssh/config`, para não abrir uma conexão a cada comando.
  - **Código 255** do SSH, ou falta de resposta no prazo, significa nó `unreachable`: o nó e os trabalhos em execução nele ficam `unreachable`. Quando o nó volta a responder, um trabalho cujo diário ainda diz "rodando" é reatado (`running`), e um que terminou nesse meio-tempo é fechado com o código real.
  - **Resposta sem o protocolo esperado** deixa o nó `outdated` até o `nodes update`.
- **`pull`:** **canal persistente.**
  - O computador roda `ssh <coordenador> fila internal channel --node <nome>` e mantém a conexão aberta.
  - Os pedidos, as respostas, a saída em tempo real e os relatórios de recursos correm por ela como **uma mensagem JSON por linha**, cada mensagem com o `id` do pedido. Assim, vários comandos rodam ao mesmo tempo.
  - **Sinal de vida** a cada 15 s nos dois sentidos, mais `ServerAliveInterval` no SSH. Sem sinal por 45 s, o nó fica `unreachable`.
  - **Reconexão** automática com espera crescente: 5 s, 10 s, 30 s… até 5 min.
  - **Um nó só fala de si mesmo.** O coordenador rejeita mensagens sobre trabalhos de outros nós e nunca executa nada pedido por um nó.
  - Nenhuma porta é aberta no computador `pull`, e ele não precisa de servidor SSH.
  - **Como ficou:**
    - **O servidor e o canal se encontram no banco.** O processo `internal channel` não é o servidor: ele é iniciado pelo `sshd` a cada conexão. Os dois conversam pelo SQLite (tabelas `channels` e `channel_requests`): o servidor grava um pedido, o canal o envia ao nó em até 50 ms e grava a resposta.
    - **Cada nó só fala de si mesmo.** Uma resposta só é aceita para um pedido daquele mesmo nó.
    - **O nó está conectado** quando o processo do canal vive e ouviu o nó nos últimos 45 s. Uma conexão nova do mesmo nó substitui a antiga.
    - **Mensagens:**
      - do coordenador para o nó: `request` (com `id`, `operation` e `args`) e `ping`;
      - do nó para o coordenador: `hello`, `response` (com `id` e `answer`), `ping` e `event`.
    - **Um trabalho que termina no nó** toca `~/.fila/wake`. O worker manda o evento `wake`, e o canal toca o `wake` do coordenador, para que o fim chegue em segundos.
    - **`nodes update`** manda o agente novo pelo próprio canal. O worker o grava e sai com o código 75, e o serviço o inicia de novo a partir do agente novo. No Windows, esse reinício leva cerca de 1 min, que é o intervalo de reinício da tarefa.

### 4.4 Diário local

Cada computador mantém, em `<jobs_dir>`, um diário dos seus trabalhos: PID, início, CPUs atribuídas, código de saída, fim e motivo. O diário é a fonte da verdade para a reconciliação depois de quedas e reinícios (§9).

### 4.5 Modo cliente

A CLI pode falar com um coordenador em outra máquina. Com `coordinator = "<usuario@host>"` na configuração local do cliente, um comando como `fila status` passa a rodar como `ssh <host> fila status`, de forma transparente. Assim, o Claude Code pode trabalhar em qualquer máquina que alcance o coordenador.

A configuração fica em `<home>/client.toml` (ou na variável `FILA_COORDINATOR`):

```toml
coordinator    = "italo@lab"      # destino SSH
remote_command = "fila"           # como chamar o fila no coordenador, ex.: "python3 -m fila"
ssh            = ["ssh"]          # programa e opções do SSH
```

- Cada argumento vai com aspas no padrão POSIX, então o shell remoto do coordenador precisa ser POSIX (bash ou sh), como num Linux ou macOS.
- A saída é transmitida aos poucos (o `log --follow` funciona), e o código de saída do comando remoto volta inteiro. Uma falha do próprio SSH (código 255) vira o código 6.
- Um `--script` local viaja pela entrada padrão: o coordenador o recebe com `--script -`.
- As quebras de linha de qualquer script enviado viram `\n`, para que um script salvo no Windows rode no bash.

## 5. Trabalhos

### 5.1 Shell

Todo comando é interpretado pelo `shell` do cadastro do nó, chamado como `<shell...> "<comando>"`. Exemplos de cadastro:

```toml
shell = ["/bin/bash", "-lc"]                          # Linux, macOS, cluster
shell = ['C:\msys64\usr\bin\bash.exe', '-lc']         # Windows com MSYS2
```

O `-l` (shell de login) carrega o perfil do usuário, então `PATH` e `module` ficam como numa sessão SSH. Os caminhos e a sintaxe dos comandos são responsabilidade de quem os escreve.

### 5.2 `run`: terminal imediato

```
fila run "<comando>" --on <nós|all|online> [--dir D] [--time T] [--cores N --mem M [--strict]] [--wait] [--dry-run] [--json]
```

- Roda **na hora**, nos computadores indicados, **em paralelo**. Nunca espera na fila.
- **Comandos independentes.** Cada execução abre um shell novo. Ela começa no diretório pessoal do nó, ou em `--dir`. Nada de `cd` ou variáveis passa de um comando para o outro.
- **Sem entrada padrão.** Ela é fechada, então um comando que pede resposta falha na hora em vez de travar. Programas interativos ficam de fora: para eles, use SSH direto.
- **Tempo máximo** `run_timeout` (padrão 10 min), ajustável com `--time`. Ao estourar, o processo é morto e o resultado é `failed`, com motivo `timeout`.
- **Recursos opcionais.** Sem `--cores`/`--mem`, nada é reservado, mas a sondagem detecta o uso e impede que trabalhos novos entrem no nó (§6.1). Com eles, a reserva entra na contabilidade enquanto o comando roda. Se o pedido não couber, o `fila` avisa e roda mesmo assim, ou recusa com `--strict`.
- **Alvos.**
  - `all` são todos os computadores cadastrados. Os clusters não entram, porque não aceitam `run`.
  - `online` são só os que estão alcançáveis.
  - Um alvo `unreachable`, `drained` ou `outdated` recebe `not_run`, com o motivo, sem espera.
- **Um registro por nó**, com código de saída, duração, saída completa em log e o fim da saída no `--json`.

### 5.3 `submit`: fila

```
fila submit [--name X] --cores N --mem M [--time T] [--on <nós>] [--dir D] [--script F]
            [--after IDS] [--even-if-failed] [--retry-after T] [--on-use keep|suspend]
            [--hint multithread|nomultithread]
            [--slurm-nodelist L] [--slurm-exclude L] [--slurm-extra "..."]
            [--dry-run] [--wait] [--json] -- <comando>
```

- **`--cores` e `--mem` são obrigatórios.** Sem eles, o trabalho é recusado.
- **`--time` é opcional.** Sem ele, vale o `max_time` do nó.
- **Cabe num único nó.** Se o pedido for maior que a **capacidade** de todos os nós candidatos, ele é recusado na hora, com o motivo (por exemplo: "25 cores: largest candidate is casa with 6"). A soma de vários nós nunca conta. Se o pedido cabe na capacidade de algum nó, mas não no que está livre agora, ou se esse nó está fora do ar, o trabalho fica `pending`.
- **`--on`** restringe os candidatos. Sem ele, o `fila` escolhe entre todos os nós (§7.3).
- **`--script F`** envia o arquivo local `F` para o diretório do trabalho no nó, e o comando passa a ser executar esse script. É a única cópia de arquivo que o `fila` faz, porque o script é a definição do trabalho, e não dados. Num cluster, linhas `#SBATCH` dentro do script geram um **aviso** e são ignoradas: os recursos são sempre os declarados ao `fila`.
- **`--dry-run`** mostra o nó escolhido e por quê, as CPUs e, no Slurm, o script gerado, sem executar nada.
- **`--wait`** envia e espera, como o `fila wait`.

### 5.4 Dependências

- `--after 12,13,14`: o trabalho só começa depois que **todas** as dependências terminarem, **mesmo que haja recursos livres**.
- **Só `completed` conta como sucesso.** É o equivalente ao `afterok` do Slurm.
- **Se uma dependência falha ou é cancelada**, o dependente fica `blocked`. Ele não ocupa recursos e aparece em destaque no `status`. O bloqueio se propaga para os dependentes dele. Há duas saídas:
  - `fila retry <id da dependência>`: uma nova tentativa. Se ela der certo, os dependentes voltam a andar sozinhos.
  - `fila cancel <id da dependência> --cascade`: cancela a cadeia inteira.
- **`--even-if-failed`**: o dependente roda depois que as dependências chegam a qualquer estado final. Serve para limpeza ou relatórios.
- As dependências olham **só o estado**. Se B precisa de arquivos de A em outro nó, quem envia os trabalhos encadeia uma cópia explícita, por exemplo um trabalho com `scp` entre os dois.
- Trabalhos sem dependências são independentes e são alocados enquanto houver recurso.

### 5.5 Estados

Estados de um trabalho do `submit`:

| Estado | Quando |
|---|---|
| `waiting_dependency` | Tem dependências ainda não terminadas. |
| `pending` | Pronto, esperando recurso. |
| `slurm_queued` | Submetido ao Slurm, que o mantém `PENDING`. |
| `running` | Em execução. A marca `suspended` indica que está congelado (§6.5). |
| `unreachable` | O nó perdeu contato durante a execução (§9). |
| `completed` | Terminou com código de saída 0. |
| `failed` | Terminou sem sucesso. O campo `reason` traz o motivo. |
| `blocked` | Uma dependência falhou ou foi cancelada. |
| `cancelled` | Cancelado pelo usuário ou em cascata. |

Motivos de `failed`:

| Motivo | Quando |
|---|---|
| `exit_code` | Código de saída diferente de 0. |
| `out_of_memory` | Passou da memória declarada. |
| `timeout` | Passou do tempo. |
| `lost` | O processo sumiu, por exemplo porque o computador reiniciou. |
| `start_failed` | O comando nem chegou a começar: diretório inválido, limite recusado pelo sistema ou shell que não abre. |
| `node_failure` | O `NODE_FAIL` do Slurm. |

Resultados de um `run`, por nó: `completed`, `failed` (motivos `exit_code` e `timeout`, mais `out_of_memory` se houver memória declarada) e `not_run` (motivos `unreachable`, `drained` e `outdated`).

### 5.6 Tentativas

- Cada trabalho tem uma ou mais tentativas. Cada tentativa guarda o próprio nó, as CPUs, o início, o fim, o estado, o motivo, o código de saída e os logs.
- `fila retry <id> [--on <nó>]` cria uma nova tentativa. Vale para trabalhos `failed`, `cancelled` ou `unreachable`.
- **Tentativa abandonada.** Quando o trabalho é reenviado enquanto o nó está sem contato, a tentativa antiga vira `abandoned`. Se ela reaparecer depois, o resultado fica guardado, mas não conta para os dependentes.
- `--retry-after T`: se o nó ficar sem contato por mais de T, o `fila` cria a nova tentativa em outro nó sozinho. Isso é opcional, por trabalho. Por padrão, nada é relançado automaticamente.

## 6. Recursos

### 6.1 Contabilidade

Para cada nó e cada recurso (núcleos e memória):

```
livre = min( capacidade − reservado_pelo_fila ,  sondagem − reserva )
```

- `capacidade`: o `limits` do cadastro. Sem ele, o hardware do computador, ou o que o Slurm informa para o cluster.
- `reservado_pelo_fila`: a soma do que foi **pedido** pelos trabalhos do `fila` no nó. Num computador, contam os trabalhos em execução e os `run` com recursos declarados. Num cluster, contam os trabalhos em execução **e** os pendentes no Slurm.
- `sondagem`: o que o sistema informa como livre agora. Num computador, núcleos menos a carga, e a memória disponível. Num cluster, as CPUs ociosas e a memória livre do `sinfo` por nó do Slurm.
- `reserva`: `reserve.always`, ou `reserve.in_use` quando a máquina está em uso (§6.5).

Usar o mínimo protege contra os dois erros possíveis:
- confiar só na sondagem, que demora a refletir um trabalho recém-lançado;
- confiar só na contabilidade, que não enxerga o uso de fora do `fila`.

### 6.2 Unidade de núcleo e *hyperthreading*

- **A unidade é a CPU lógica**, como no sistema operacional e no Slurm. O cadastro registra os núcleos físicos e os lógicos de cada computador.
- **Núcleos físicos inteiros.** Nos computadores, sempre que possível, um trabalho recebe núcleos físicos inteiros, e dois trabalhos nunca dividem um núcleo físico.
- **`--hint nomultithread`** (ou o `default_hint` do nó), com o mesmo sentido do Slurm:
  - **Num computador:** `--cores N` vira N núcleos físicos, com uma CPU lógica usada em cada. As CPUs irmãs ficam reservadas e paradas, então a contabilidade conta 2N CPUs lógicas num nó com *hyperthreading*.
  - **Num cluster:** o `fila` repassa `--hint=nomultithread` ao `sbatch`. Isso só tem efeito se o Slurm do cluster tiver afinidade configurada (`TaskPlugin`).

### 6.3 Imposição dos limites

Os recursos declarados não são uma promessa. O `fila` os impõe da mesma forma que o Slurm com cgroup.

**Núcleos**
- O supervisor prende o processo, e todos os seus filhos, às CPUs atribuídas:
  - `sched_setaffinity` no Linux;
  - máscara de afinidade dentro de um *Job Object* no Windows.
- Threads a mais apenas dividem essas CPUs, sem que o trabalho seja morto.
- O macOS não tem afinidade de processo. Lá, o limite de núcleos não é imposto (limitação conhecida).

**Memória**
- Limite rígido onde o sistema permite:
  - cgroup v2 via `systemd-run --user --scope -p MemoryMax=...` no Linux;
  - limite de memória do *Job Object* no Windows.
- Onde não permite (macOS, ou Linux sem cgroup delegado), o supervisor mede a memória do grupo de processos a cada poucos segundos e mata o trabalho se ela passar do declarado.
- Nos dois casos, o resultado é `failed` com motivo `out_of_memory`.
- **Estado atual da implementação:** o Windows já usa o limite rígido e é avisado pelo *Job Object* quando um processo tenta passar dele. O Linux e o macOS ainda só medem e matam; o cgroup no Linux entra quando houver um Linux para testar.

**Tempo**
- Ao passar do `--time`, o supervisor envia `SIGTERM`, espera 30 s e envia `SIGKILL`. O resultado é `failed` com motivo `timeout`.
- No Windows não há `SIGTERM` para programas comuns, então lá o encerramento é imediato, pelo *Job Object* (limitação conhecida).
- No cluster, quem impõe é o próprio Slurm, com `--time`, `--mem` e `--cpus-per-task`.

### 6.4 Variáveis exportadas ao trabalho

`FILA_JOB_ID`, `FILA_ATTEMPT`, `FILA_CORES`, `FILA_MEM` e `FILA_NODE`.

O `fila` **não** define `OMP_NUM_THREADS` nem variáveis de outros programas, como o Slurm também não faz. O OpenMP do GCC (libgomp) conta as CPUs da máscara de afinidade para decidir quantos *threads* abrir, então programas que usam todos os núcleos disponíveis já ficam com N *threads*. No Windows/MinGW isso foi conferido (Apêndice A); no Linux, falta conferir.

### 6.5 Uso da máquina pelo usuário

**Detecção.** A máquina está "em uso" se houve entrada de teclado ou mouse nos últimos `idle_minutes`:
- `GetLastInputInfo` no Windows;
- `IdleHint` da sessão (`loginctl`) no Linux;
- `HIDIdleTime` (`ioreg`) no macOS.

No Windows, isso exige que o trabalhador rode na sessão do usuário, como tarefa agendada que inicia no logon.

**Efeitos.**
- Enquanto a máquina está em uso, vale `reserve.in_use` para os trabalhos **novos**.
- Sobre os que **já rodam**, a escolha é do usuário, em três níveis, e vale o mais específico:
  1. `on_use` do cadastro do nó: `keep` (padrão) ou `suspend`;
  2. `--on-use` do trabalho;
  3. manualmente, com `fila suspend <id>|--node <nó>` e `fila resume <id>|--node <nó>`.
- **`suspend`** congela os processos: `SIGSTOP`/`SIGCONT` no Linux e no macOS, suspensão dos processos do *Job Object* no Windows. A retomada é automática quando a máquina volta a ficar livre.
  - Libera a CPU, mas **não a memória**.
  - O tempo congelado não conta para o `--time`.

### 6.6 Disco

A sondagem mede o espaço livre em `jobs_dir`. Abaixo de `min_free_disk`, o nó não recebe trabalhos novos, e o `explain` informa o motivo.

### 6.7 Recursos genéricos (futuro)

Por enquanto, só núcleos e memória. Os recursos de nós e trabalhos são guardados como **dicionário**, e não como campos fixos. Assim, recursos contáveis como GPU podem entrar depois, como o GRES do Slurm:
- `gres = { gpu = 1 }` no cadastro;
- `--gres gpu:1` no trabalho;
- repasse de `--gres` ao `sbatch`;
- `FILA_GPUS` nos computadores.

Nada disso exige mudar o banco nem o agendador.

## 7. Agendamento

### 7.1 Candidatos

Um nó é candidato para um trabalho pronto se:
- está `online` e não está separado para outro trabalho (§7.2);
- está dentro de `--on`, se houver;
- o trabalho cabe na capacidade dele **e** no livre de agora (§6.1), recurso por recurso.

Para um cluster, "cabe agora" significa que o `sinfo` mostra um nó do Slurm da partição, respeitando `nodelist` e `exclude`, com CPUs ociosas e memória livre suficientes.

### 7.2 Ordem e proteção contra fome

- **Preenchimento.** A cada ciclo, os trabalhos prontos são percorridos na ordem de envio, e cada um que tiver candidato é lançado. Um trabalho que não cabe não segura os que vêm depois.
- **Proteção contra fome.** Se o trabalho mais antigo da fila espera há mais de `starvation_after` (padrão 1 h), o `fila` **separa para ele** o nó, entre os que comportam o pedido, que deve liberar recursos mais cedo. Nesse nó não entra nenhum trabalho novo até ele começar.
- O *backfill* do Slurm (deixar entrar trabalhos curtos que comprovadamente terminam antes) e as prioridades manuais ficam para os refinamentos.

### 7.3 Escolha do nó

Entre os candidatos, vence o de **maior fator de velocidade por núcleo**. No empate, vence o que **sobra menos** depois da alocação, para manter espaços grandes livres para trabalhos grandes.

O `fila` não espera um nó mais rápido ficar livre: escolhe o mais rápido entre os que podem começar agora.

### 7.4 Fator de velocidade

O fator mede a rapidez **de um núcleo**, porque o trabalho recebe os núcleos que declarou em qualquer nó. A frequência nominal e o número de núcleos do nó não servem para isso.

- **Teste embutido.** Roda num núcleo, com código em C da biblioteca padrão: 24 compressões `zlib` (nível 6) de um bloco fixo de cerca de 2,3 MB. Esse trabalho em 2 s vale 1.0. É executado no cadastro dos computadores (a menos que `--speed` seja dado) e com `fila nodes benchmark`.
- **Teste do usuário (escala).** Um comando que leva 100 s vale 1.0. O cadastro guarda de onde veio o fator (`speed_source`: `builtin`, `command`, `manual` ou `default`), e o `nodes benchmark` avisa quando se misturam fatores de testes diferentes.
- **Teste do usuário.** `fila nodes benchmark --all --command "..."` roda um comando representativo em cada nó e mede só o tempo. Nos clusters, ele é submetido por `sbatch` e roda num nó de cálculo, nunca no nó de login.
- **Ajuste manual.** `speed` no cadastro.
- **Comparação.** Os fatores só são comparáveis entre nós medidos com o mesmo teste. Um cluster só pode ser medido com o teste do usuário ou ajustado à mão. Um nó sem medição vale 1.0.

### 7.5 Política do Slurm

O `fila` mantém o trabalho na própria fila e só faz `sbatch` quando o cluster "cabe agora" (§7.1). Há uma exceção: se **nenhum outro candidato** puder começar o trabalho agora, ele pode ser submetido mesmo assim, desde que o `fila` tenha menos de `max_pending_slurm` (K, padrão 2) trabalhos seus pendentes naquele cluster. Isso garante um lugar na fila do Slurm sem enchê-la.

Num refinamento futuro, um trabalho parado no Slurm por mais de X horas poderá ser cancelado (`scancel`) e redirecionado, se ainda não tiver começado e outro nó puder rodá-lo.

## 8. Clusters Slurm: detalhes

**Script gerado.** O `fila` gera o script a partir dos recursos declarados e do cadastro, envia para `<jobs_dir>/<id>/job.sbatch` e submete. Exemplo:

```bash
#!/bin/bash -l
#SBATCH --job-name=fila-42
#SBATCH --partition=cpuq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=32G
#SBATCH --time=3-00:00:00
#SBATCH --chdir=/home/usuario/projeto
#SBATCH --output=/home/usuario/fila/jobs/42/stdout.log
#SBATCH --error=/home/usuario/fila/jobs/42/stderr.log
#SBATCH --exclude=apollo01
export FILA_JOB_ID=42 FILA_ATTEMPT=1 FILA_CORES=64 FILA_MEM=32G FILA_NODE=cluster-cpu
./build/programa entrada.yaml
```

**Opções.**
- O `fila` entende `--slurm-nodelist` e `--slurm-exclude`: ele as repassa ao `sbatch` e filtra o `sinfo` por elas.
- `--slurm-extra "..."` passa direto, como linhas `#SBATCH`, sem interpretação.
- São **recusadas** as opções que sobrescrevem o que é do `fila`: `--cpus-per-task`, `--mem`, `--mem-per-cpu`, `--time`, `--nodes`, `--ntasks`, `--output`, `--error` e `--chdir`.

**Acompanhamento.**
- O estado e o motivo vêm do `squeue` e do `sacct`:

  | Slurm | `fila` |
  |---|---|
  | `PENDING` | `slurm_queued` |
  | `RUNNING` | `running` |
  | `COMPLETED` | `completed` |
  | `FAILED` | `failed` / `exit_code` |
  | `OUT_OF_MEMORY` | `failed` / `out_of_memory` |
  | `TIMEOUT` | `failed` / `timeout` |
  | `NODE_FAIL` | `failed` / `node_failure` |
  | `CANCELLED` | `cancelled` |

- O código de saída vem do `ExitCode` do `sacct`.
- O `fila log --follow` usa `tail` por SSH a partir do computador `via`.
- O `fila cancel` chama `scancel`.

**Migração (refinamento).** `fila submit --from-sbatch arquivo.slurm` leria as diretivas comuns (`--cpus-per-task`, `--mem`, `--time`, `--partition`, `--nodelist`, `--exclude`, `--job-name`) e recusaria as que não souber contar.

**Como foi implementado**
- **Comandos.** O agente do computador `via` faz SSH até o nó de login (com o `ssh_command` geral) e roda só:
  - `sinfo -h -N -p <partição> -o %N`: as máquinas da partição;
  - `scontrol show node -o`: `CPUTot`, `CPUAlloc`, `RealMemory`, `AllocMem` e `State`. Máquinas em `DOWN`, `DRAIN`, `FAIL`, `MAINT` e estados parecidos contam como sem nada livre;
  - `scontrol show partition <p> -o` e `sacctmgr -n -P show qos`: `MaxCPUsPerNode`, o `cpu=` do `MaxTRES` da QoS e o `MaxTime`;
  - `sbatch --parsable`, `squeue -h -u "$USER"`, `sacct -n -P -X`, `scancel`, `tail`, `df`, `du` e `rm` nos arquivos do próprio `fila`.
- **Cadastro.** O `nodes register --type slurm --via <computador> --host <alias> --partition <p>` aceita também `--account`, `--qos`, `--exclude`, `--max-pending`, `--max-cores-per-job`, `--cores`/`--mem` (a cota de cortesia) e `--max-time`. Ele confere a partição, as máquinas e o shell, e grava o `jobs_dir` **como caminho absoluto**, porque o Slurm não expande `~` nas linhas `#SBATCH`. Sem `--max-time`, vale o `MaxTime` da partição.
- **`--script`** vai embutido no próprio script do `sbatch` (um *heredoc*), sem cópia à parte.
- **`--slurm-extra "--constraint=avx512"`**: o valor que vem logo depois da opção é sempre o valor, mesmo começando com `--`.
- **Estado.** O `sacct` decide quando conhece o job, e o `squeue` cobre um `sacct` lento ou ausente. Um job em execução tem o início real registrado (`Start` do `sacct`).
- **`fila log`** lê com `tail` no cluster, e o `--follow` termina quando `squeue -n fila-<id>` não lista mais o job.
- **`fila suspend`** é recusado em jobs de cluster, porque `scontrol suspend` exige o administrador.
- **`nodes benchmark <cluster> --command ...`** submete o comando com `sbatch --wait` e usa o `ElapsedRaw` do `sacct`.
- **Cluster inalcançável** (o `via` caiu, ou o SSH até o login falhou): o nó fica `unreachable`, e os jobs em execução lá também. Os jobs `slurm_queued` continuam como estão, porque o Slurm cuida deles. Tudo se reconcilia quando o cluster volta.

## 9. Falhas e reconciliação

- **Perder o canal ou o SSH não mata o trabalho** (supervisor separado, §4.2).
- **Sem contato.** Quando o nó fica `unreachable`, os trabalhos dele ficam `unreachable`, e não `failed`. **Nada é relançado automaticamente**, a menos que o trabalho tenha `--retry-after` (§5.6).
- **Quando o nó volta**, o `fila` reconcilia cada trabalho com o diário (ou com o `sacct`, no cluster):
  - ainda rodando: volta a `running`;
  - terminou durante a ausência: vira `completed` ou `failed`, com o código de saída verdadeiro;
  - sumiu: vira `failed` com motivo `lost`, e os dependentes ficam `blocked` (§5.4).
- **Para não esperar:** `fila retry <id> --on <outro nó>` cria uma nova tentativa, e a antiga vira `abandoned`.
- **Cluster.** Se o computador `via` cai, o cluster fica `unreachable`. Os trabalhos continuam no Slurm, e a reconciliação pelo `sacct` acontece quando o computador volta.

## 10. Arquivos

- Cada trabalho tem um diretório próprio, `<jobs_dir>/<id>/`, no nó onde roda, compartilhado por todas as tentativas. Por padrão, o comando de um `submit` roda lá. Um `run` começa no diretório pessoal do usuário. Com `--dir`, os dois rodam em outro diretório do nó, interpretado no próprio nó.
- Os arquivos do `fila` ficam **sempre** em `<jobs_dir>/<id>/attempt-<n>/`, um diretório por tentativa, mesmo com `--dir`:
  - `stdout.log` e `stderr.log`;
  - `job.json`: comando, shell, CPUs atribuídas, limites e variáveis;
  - `status.json`: o diário da tentativa (estado, PIDs, horários, código de saída, motivo, tempo suspenso);
  - `supervisor.log`: erros do próprio supervisor;
  - `cancel` e `suspend`: pedidos ao supervisor; `suspend` presente significa "manter congelado";
  - `job.sbatch`, nos clusters.
- O script do `--script` é gravado em `<jobs_dir>/<id>/`.
- O programa escreve os resultados onde escreveria se rodasse no terminal. O `fila` só define o diretório inicial.
- **Nenhum arquivo é movido automaticamente.** Quem quiser os resultados baixa por conta própria, com um comando comum, na direção que funcionar:
  - de um computador `pull`: `fila run "scp saida.h5 <coordenador>:~/resultados/" --on <nó>`;
  - de um computador `ssh` ou de um cluster: `scp` a partir da máquina do coordenador ou do computador `via`.
- **Nada é apagado automaticamente.** O comando `fila clean --older-than 30d [--node X] [--logs-only]` só apaga diretórios `<jobs_dir>/<id>/` de trabalhos terminados, e nunca caminhos passados com `--dir`.

## 11. Linha de comando

```
fila run "<comando>" --on <nós|all|online> [...]         # terminal imediato (§5.2)
fila submit [...] -- <comando>                            # fila (§5.3)
fila status [--state S] [--node N] [--all]                # fila, execução e resumo dos nós
fila resources                                            # o que cada nó tem livre agora
fila guide                                                # o guia de uso para o CLAUDE.md
fila mcp [--register]                                     # o servidor MCP (stdio), ou o registro no Claude Code
fila result <id>                                          # estado, motivo, código, nó, tentativas, diretório, fim do log
fila log <id> [--attempt K] [--follow]
fila explain <id>                                         # por que o trabalho está nesse estado
fila wait <ids...> [--timeout T]
fila cancel <id> [--cascade]
fila retry <id> [--on <nó>]
fila suspend <id>|--node <nó>
fila resume  <id>|--node <nó>
fila clean --older-than T [--node N] [--logs-only]
fila nodes register|list|remove|drain|resume|validate|benchmark|update ...
fila selftest --on <nós>
fila server [--once] [--install|--uninstall [--dry-run]]  # o coordenador, ou o serviço permanente
fila worker install|run ...                               # o agente de um computador pull
fila internal ...                                         # uso interno (canal, supervisor, sondagem)
```

**`explain`.** Diz em linguagem direta por que um trabalho está parado. Exemplo: "pending: needs 6 cores; casa has 4 free (2 used outside fila); cluster-cpu excluded by --on; waiting since 14:02; node reservation in 38 min (starvation protection)".

**`wait`.** Bloqueia até todos os trabalhos indicados chegarem a um estado final: `completed`, `failed`, `cancelled` ou `blocked`. Ele só lê o banco local, mais ou menos uma vez por segundo, e não fala com os nós.

**Saída.**
- Texto legível por padrão, e `--json` em todos os subcomandos.
- No `--json`, a saída de um comando traz o fim dela (`json_tail_kb`) e o caminho do log completo.

**Códigos de saída**, os mesmos para todos os subcomandos:

| Código | Significado |
|---|---|
| 0 | Sucesso, em todos os alvos ou trabalhos. |
| 1 | Algum alvo ou trabalho terminou sem sucesso. |
| 2 | Algum alvo não rodou (`not_run`). |
| 3 | O tempo de espera acabou (`wait --timeout`, `--wait`). |
| 4 | Pedido recusado: recursos ausentes, maior que qualquer nó, `--strict` sem recurso livre, opção do Slurm proibida, `run` em cluster. |
| 5 | Erro de uso: opção inválida, nó ou trabalho inexistente. |
| 6 | Coordenador indisponível. |

## 12. Integração com o Claude Code

### 12.1 Etapa inicial: linha de comando

O `fila` vem com um **guia de uso genérico**, para ser incluído no `CLAUDE.md` de qualquer projeto. O guia explica:

- **Comandos.** Os subcomandos e quando usar cada um: `run` para comandos curtos nos terminais dos nós e `submit` para cálculos.
- **`--json`.** Sempre ao ler resultados.
- **Esperar sem consultar.** Depois de um `submit`, chamar `fila wait <ids>` em segundo plano e continuar trabalhando. O Claude Code é avisado quando os trabalhos terminam.
- **`explain`** quando um trabalho não começa.
- **Cálculos pesados vão sempre por `submit`**, nunca por `run`.

Orientações que dependem do projeto, para o `CLAUDE.md` de cada um, e não para o `fila`:

- **Caminhos relativos.** Um trabalho começa em `<jobs_dir>/<id>/`. Para rodar como no terminal, a partir da raiz do repositório, use `--dir`.
- **Saídas únicas.** Trabalhos simultâneos no mesmo `--dir` não devem escrever no mesmo arquivo. Use `$FILA_JOB_ID` no nome das saídas.
- **Binários em uso.** Não recompile um executável usado por trabalhos em execução. O Windows trava o arquivo, e no Linux os trabalhos que começarem depois pegam o binário novo. Copie o binário por campanha.
- **Sincronização e compilação** são comandos comuns, por exemplo `fila run "git pull && cmake --build build" --on all --wait`. No cluster, passam pelo computador vinculado (`fila run --on <via> "ssh <alias> '...'"`). Uma compilação pesada no cluster deve ir por `submit`.

### 12.2 Etapa posterior: ferramenta registrada (Model Context Protocol)

- Uma camada fina em Python que expõe ferramentas com parâmetros tipados: `run`, `submit`, `status`, `resources`, `result`, `log`, `explain`, `wait`, `cancel` e `retry`.
- **Sem o kit oficial, de propósito.** O pacote `mcp` exige Python 3.10 ou mais novo e instalação com `pip`, o que quebraria duas regras do projeto: só biblioteca padrão e um único `.pyz`. O protocolo sobre stdio (JSON-RPC 2.0, uma mensagem por linha) é implementado com a biblioteca padrão, e só na parte que um servidor de ferramentas precisa: `initialize` (com negociação da versão: 2025-06-18, 2025-03-26 ou 2024-11-05), `tools/list`, `tools/call` e `ping`.
- **Cada ferramenta roda o comando correspondente da CLI com `--json`.** Assim, o MCP faz exatamente o que o terminal faz, inclusive o modo cliente: num cliente, o servidor MCP roda localmente e cada chamada segue por SSH. A resposta traz `exit_code`, `result` e `stderr`. Só os códigos 4, 5 e 6 (recusa, uso errado, coordenador fora) viram erro da ferramenta; um trabalho que falhou é um resultado normal.
- **Registro:** `fila mcp --register`, que roda `claude mcp add --scope user fila -- <python> <home>/fila_launcher.py mcp`. Para remover, `claude mcp remove fila -s user`.
- **Permissões sugeridas** (o `--register` imprime este trecho para o `settings.json`):
  - livres: `status`, `resources`, `result`, `log`, `explain` e `wait`;
  - pedem confirmação: `run` e `cancel`;
  - `submit` e `retry` seguem o padrão do Claude Code.
- As chamadas de ferramenta têm tempo máximo curto. Por isso, o `wait` da ferramenta aceita no máximo 10 min, e as esperas longas continuam com `fila wait` em segundo plano.
- **Guia para o `CLAUDE.md`:** `fila guide` imprime o guia de uso (em inglês, como o programa), e `fila guide >> CLAUDE.md` o acrescenta a um projeto.

## 13. Segurança

1. **A criptografia vem do SSH.** Todo o tráfego entre coordenador, computadores e clusters passa por SSH, que criptografa e autentica os dois lados. O `fila` não usa criptografia própria.
2. **Só chaves `ed25519`**, nunca senhas. A chave de cada computador `pull` fica restrita no `authorized_keys` do coordenador: com ela, só se abre o canal daquele nó. Ela não dá shell nem permite se passar por outro nó.
3. **Nenhuma porta nova** em nenhuma máquina. O `fila server` não escuta na rede.
4. **O coordenador também se identifica.** No pareamento, o computador `pull` grava a impressão digital do SSH do coordenador e recusa conexões que não confiram.
5. **Um nó só fala de si mesmo** (§4.3).
6. **Permissões.** O cadastro, o banco e os logs têm permissão só do usuário: `0600` no Linux e no macOS, e ACL equivalente no Windows. O agente roda como usuário comum, nunca como administrador.
7. **Tudo é registrado.** Todo `run` e todo `submit` ficam no banco, com origem (`created_by`), horário, nó e resultado.
8. **O ponto crítico é o coordenador.** Quem controla o coordenador pode executar qualquer coisa em todos os nós, porque esse é o papel dele. A conta da máquina do coordenador deve ser protegida.

## 14. Testes

1. **Agendador como função pura.** Ele recebe o estado dos nós, os trabalhos e o horário, e devolve as decisões. Não usa rede nem disco, e o relógio é simulado. Cada regra das seções 5 a 7 tem testes próprios, incluindo:
   - fome (um trabalho de 6 núcleos atrás de vários de 2);
   - bloqueio em cascata e `retry`;
   - recusa por capacidade;
   - política do Slurm com K;
   - escolha por velocidade e desempate.
2. **Nós e Slurm de mentira**, para testar o fluxo completo (servidor, canal, supervisor, diário, reconciliação) no CI:
   - nós executados na própria máquina, com diretórios e limites artificiais;
   - um Slurm falso, com scripts `sbatch`, `squeue`, `sacct`, `sinfo` e `scancel` que respondem como o real;
   - um canal que pode ser derrubado de propósito.
3. **`fila selftest --on <nós>`** nos nós reais. Roda **dentro do nó**, pelo agente e com o Python do nó, sem depender do servidor. Verifica:
   - `shell`: o shell cadastrado responde;
   - `start` e `exit_code`;
   - `time_limit`: um trabalho que estoura o tempo;
   - `memory_limit`: um que estoura a memória de propósito;
   - `cpu_affinity`: no macOS aparece como "n/a";
   - `suspend_resume`: suspensão e retomada;
   - `leftovers`: processos deixados para trás são mortos.

   A queda e a volta do canal entram com os nós `pull`. A conferência do que o OpenMP enxerga continua manual (Apêndice A).
4. **`--dry-run`** no `run` e no `submit`.

## 15. Etapas de desenvolvimento

1. **Núcleo e computador `local`.**
   - Banco, agendador puro com testes e CLI completa.
   - `server` como serviço nos três sistemas, `--dry-run`, `explain` e modo cliente.
   - Execução direta com supervisor, afinidade, limite de memória, tempo, diário, suspensão e detecção de uso, nos três sistemas.
   - Já é útil sozinho: transforma qualquer máquina numa fila.
2. **Computadores `ssh`.** Agente `.pyz`, `nodes register/validate/benchmark/update`, `run --on all` e `selftest`.
3. **Clusters Slurm**, vinculados a qualquer computador (inclusive o `local`). Script gerado, política K, limites lidos do Slurm, `squeue`/`sacct`, `--hint`, `nodelist`/`exclude`/`extra` e teste de velocidade por `sbatch`.
4. **Computadores `pull`.** Canal persistente, pareamento com chave restrita, reconexão, reconciliação e trabalhador do Windows como tarefa agendada na sessão do usuário.
5. **Integração com o Claude Code.** Guia para o `CLAUDE.md` e camada MCP.
6. **Refinamentos.** Redirecionamento de trabalhos parados no Slurm, *backfill*, prioridades, `--from-sbatch`, recursos genéricos e notificações (celular, e-mail) quando trabalhos terminam ou falham.

Dependências entre as etapas:
- A etapa 3 depende só da 1, porque o computador vinculado pode ser o `local`.
- A etapa 4 depende da 2, porque reaproveita o agente.
- As etapas 3 e 4 podem trocar de lugar.

**Sistemas.** Linux e Windows são testados de verdade desde a etapa 1. O macOS é implementado na mesma interface, mas fica marcado como **experimental** até haver um Mac para rodar o `selftest`.

## 16. Limitações conhecidas

- **macOS:** sem afinidade de CPU (o limite de núcleos não é imposto), e a memória é vigiada em vez de limitada.
- **Windows:**
  - não há tolerância para encerramento no tempo esgotado: o processo é morto na hora, sem `SIGTERM`;
  - a detecção de uso exige que o trabalhador rode na sessão do usuário. Se o computador reiniciar e ninguém entrar na conta, o nó fica fora do ar até o logon.
- **Slurm:** o `--hint` e os limites rígidos dependem da configuração do cluster (cgroup e `TaskPlugin`).
- **Trabalhos perdidos não são relançados sozinhos** sem `--retry-after`, por decisão de projeto.
- **Um trabalho nunca ocupa mais de um nó**, nem mais de um nó do Slurm.

---

## Apêndice A: recursos atuais do usuário (exemplo de arranjo)

Os nós abaixo são os recursos disponíveis hoje. Servem de exemplo e não são suposições do programa.

| Nó | Tipo | O que se sabe |
|---|---|---|
| `lab` | computador, Linux (Ubuntu) | i5, cerca de 8 GB. Fica sempre ligado. É o candidato natural a coordenador e a computador `via` do cluster. |
| `cluster-cpu` | cluster Slurm, `via = "lab"` | Partição `cpuq`, nós `apollo01`, `apollo05`… Um script existente pede `--cpus-per-task=254 --mem=64G --time=72:00:00` em `apollo05`, então os "cerca de 20 núcleos por trabalho" não se confirmam na `cpuq`. Módulos: `cmake/3.30.5-p5ngu77` e `ninja/1.12.1-36rc3kw`. O administrador não permite processos permanentes e derruba trabalhos muito longos à mão, então se sugere `max_time = "7-00:00:00"`. |
| `casa` | computador, Windows 10, `pull` | Xeon E5-2680 v4, 14 núcleos físicos e 28 lógicos, 15,9 GB (a memória é o recurso escasso, então convém reservar uma margem), AMD Radeon RX 6600. MSYS2 UCRT64 com GCC 15.2. Não é alcançável de fora. |

Exemplo de cadastro, com o coordenador no `lab`:

```toml
[nodes.lab]
type      = "computer"
access    = "local"
os        = "linux"
shell     = ["/bin/bash", "-lc"]
jobs_dir  = "/home/italo/fila/jobs"
reserve   = { always = { mem = "1G" }, in_use = { cores = 2, mem = "3G" } }

[nodes.cluster-cpu]
type      = "slurm"
via       = "lab"
host      = "cluster"
shell     = ["/bin/bash", "-l"]
partition = "cpuq"
jobs_dir  = "/home/italo/fila/jobs"
max_time  = "7-00:00:00"
max_pending_slurm = 2

[nodes.casa]
type      = "computer"
access    = "pull"
os        = "windows"
shell     = ['C:\msys64\usr\bin\bash.exe', '-lc']
jobs_dir  = 'C:\Users\italo\fila\jobs'
min_free_disk = "5G"
reserve   = { always = { cores = 1, mem = "4G" } }
on_use    = "keep"
```

### Pendências deste arranjo

- [ ] Com a VPN ligada, o `casa` alcança o `lab` por SSH? Isso é necessário para o canal `pull`.
- [ ] Configuração do Slurm do cluster, que decide se `--hint` e os limites rígidos funcionam:
  ```
  scontrol show config | grep -iE 'TaskPlugin|ProctrackType|JobAcctGather'
  grep -i Constrain /etc/slurm/cgroup.conf
  ```
- [x] O OpenMP do MinGW (GCC 15.2, MSYS2 UCRT64) respeita a máscara de afinidade no `casa`: com 2, 3 e 6 CPUs atribuídas, `omp_get_max_threads()` devolveu 2, 3 e 6. Conferido em 2026-10-29.
- [ ] Chave SSH do `lab` autorizada no cluster, e `~/.ssh/config` do `lab` com o alias `cluster`.
