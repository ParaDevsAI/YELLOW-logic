"""
test_system_time.py
Teste para verificar sincronização de tempo
"""
import time
import datetime
import ntplib
import requests

def test_system_time():
    """Teste de sincronização de tempo."""
    print("🕐 TESTE DE SINCRONIZAÇÃO DE TEMPO")
    print("="*50)
    
    # 1. Tempo local
    local_time = datetime.datetime.now()
    print(f"1️⃣ Tempo local: {local_time}")
    
    # 2. Tempo UTC
    utc_time = datetime.datetime.utcnow()
    print(f"2️⃣ Tempo UTC: {utc_time}")
    
    # 3. Diferença
    diff = local_time - utc_time
    print(f"3️⃣ Diferença local-UTC: {diff}")
    
    # 4. Teste com NTP (se disponível)
    try:
        ntp_client = ntplib.NTPClient()
        response = ntp_client.request('pool.ntp.org')
        ntp_time = datetime.datetime.fromtimestamp(response.tx_time)
        print(f"4️⃣ Tempo NTP: {ntp_time}")
        
        ntp_diff = local_time - ntp_time
        print(f"5️⃣ Diferença local-NTP: {ntp_diff}")
        
        if abs(ntp_diff.total_seconds()) > 60:
            print("⚠️  ATENÇÃO: Tempo pode estar dessincronizado!")
        else:
            print("✅ Tempo parece sincronizado")
            
    except Exception as e:
        print(f"❌ Erro NTP: {e}")
    
    # 5. Teste com API online
    try:
        response = requests.get('http://worldtimeapi.org/api/timezone/Etc/UTC', timeout=5)
        if response.status_code == 200:
            data = response.json()
            api_time = datetime.datetime.fromisoformat(data['datetime'].replace('Z', '+00:00'))
            print(f"6️⃣ Tempo API: {api_time}")
            
            api_diff = local_time - api_time.replace(tzinfo=None)
            print(f"7️⃣ Diferença local-API: {api_diff}")
            
            if abs(api_diff.total_seconds()) > 60:
                print("⚠️  ATENÇÃO: Tempo pode estar dessincronizado!")
            else:
                print("✅ Tempo parece sincronizado")
        else:
            print("❌ Erro na API de tempo")
            
    except Exception as e:
        print(f"❌ Erro API: {e}")
    
    print("🏁 Teste completo!")

if __name__ == "__main__":
    test_system_time() 