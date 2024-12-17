Compilação:
 Simplesmente executar make.
 Caso quiser o modo debug, rode make debug.

Execução:
 Modo normal:
   ./server <NÚMERO DA PORTA>
   ./client <NÚMERO DA PORTA>
  O servidor roda passivamente, enquanto o client deve receber dados pelo stdin.

 Modo debug:
  ./server <NÚMERO DA PORTA>
  ./client <NÚMERO DA PORTA> <NÚMERO ID>


Explicação do modo debug:
 O modo debug compila com as variáveis de debug e imprime mensagens de debug durante a execução.
 Além disso ele utilizada o ID como identificador dos clientes, ao invés do IP, e portanto pode ser rodado em uma máquina só.
 Note que o sistema não funciona com vários clientes com o mesmo ID, portanto deve-se tomar cuidado para sempre se utilizar
um ID diferente para cada instância de cliente.

