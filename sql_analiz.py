import duckdb
import pandas as pd

csv_yolu = 'c:/Users/Lenovo/Desktop/e-ticaret-analizi/satislar.csv'

sql_sorgusu = f"""
SELECT 
    musteri_id, 
    tarih, 
    tutar,
    RANK() OVER (PARTITION BY musteri_id ORDER BY tutar DESC) as harcama_sirasi,
    SUM(tutar) OVER (PARTITION BY musteri_id ORDER BY tarih) as kumulatif_toplam
FROM '{csv_yolu}'
ORDER BY musteri_id, tarih;
"""

print("--- SQL Analizi Başlatılıyor ---")

try:
    result = duckdb.query(sql_sorgusu).df()
    
    print("\nAnaliz Sonuçları (İlk 10 Satır):")
    print(result)
    
    result.to_csv('c:/Users/Lenovo/Desktop/e-ticaret-analizi/analiz_sonuclari.csv', index=False)
    print("\n✅ Başarılı! Sonuçlar 'analiz_sonuclari.csv' dosyasına kaydedildi.")

except Exception as e:
    print(f"\n❌ Bir hata oluştu: {e}")