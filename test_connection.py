import socket
import os

def test_tcp_connection():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex(("172.16.20.10", 2638))
        sock.close()
        if result == 0:
            print("✅ TCP conecta OK")
            return True
        else:
            print(f"❌ TCP falha: {result}")
            return False
    except Exception as e:
        print(f"❌ Erro TCP: {e}")
        return False

def test_sqlany_env():
    print(f"SQLANY_API_DLL: {os.environ.get('SQLANY_API_DLL', 'NOT SET')}")
    print(f"LD_LIBRARY_PATH: {os.environ.get('LD_LIBRARY_PATH', 'NOT SET')}")
    
    # Teste se a lib existe
    lib_path = "/home/user/studio/client17011/lib64/libdbcapi_r.so"
    if os.path.exists(lib_path):
        print(f"✅ Lib existe: {lib_path}")
    else:
        print(f"❌ Lib não encontrada: {lib_path}")

if __name__ == "__main__":
    print("=== Teste de Conectividade ===")
    test_tcp_connection()
    print("\n=== Teste de Ambiente ===")
    test_sqlany_env()