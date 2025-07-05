# shell.nix
{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  name = "studio-env";

  buildInputs = with pkgs; [
    # Node.js runtime
    nodejs

    # Python 3.11 completo (inclui ctypes, ssl, etc.)
    python311Full

    # Ferramentas auxiliares
    unzip git
    pkgs.busybox

    # Pacotes Python de projeto
    python311Packages.pandas
    python311Packages.beautifulsoup4
    python311Packages.lxml
    python311Packages.openpyxl

    # Driver sqlanydb 1.0.11 – empacotado manualmente
    (python311Packages.buildPythonPackage rec {
      pname   = "sqlanydb";
      version = "1.0.11";
      src = pkgs.fetchPypi {
        inherit pname version;
        sha256 = "XmDhgIucEPOvrJspTaqsHhia0v/bm7f/msZTQnpMXHs=";
      };
    })
  ];

  shellHook = ''
    # 1) Carrega variáveis locais do .env (se existir)
    set -a
    [ -f .env ] && . ./.env
    set +a

    # 2) Define SQLANY_BASE com o caminho correto
    if [ -z "$SQLANY_BASE" ]; then
      export SQLANY_BASE="/home/user/studio/client17011"
    fi

    # 3) Caminho explícito para a DBCAPI
    export SQLANY_API_DLL="$SQLANY_BASE/lib64/libdbcapi_r.so"

    # 4) Garante que o loader encontre as dependências
    export LD_LIBRARY_PATH="$SQLANY_BASE/lib64:$LD_LIBRARY_PATH"

    # 5) Adiciona o PATH para binários
    export PATH="$SQLANY_BASE/bin64:$PATH"

    # 6) Define variável padrão do SQL Anywhere
    export SQLANY17="$SQLANY_BASE"

    # 7) Disponibiliza o Python para o Node.js
    export NIX_PYTHON="$(which python3)"

    # 8) Mensagem de status
    echo "✅ Nix shell pronto – DBCAPI em: $SQLANY_API_DLL"
    echo "📁 Caminho das libs: $LD_LIBRARY_PATH"
    echo "🐍 Python: $NIX_PYTHON"
  '';
}