"""
DART 전체 상장사 고유번호(corp_code) 매핑을 받아서 dart_corp_code_map.json으로 저장.

로컬(정상 네트워크)에서 딱 한 번 실행해서 나온 dart_corp_code_map.json 파일을
inventory_manager.py와 같은 폴더에 두고 git에 커밋하면, Streamlit Cloud에서는
이 파일을 읽기만 해서 corpCode.xml(3.5MB) 실시간 다운로드가 필요 없어진다.

사용법:
  1) 아래 API_KEY에 secrets.toml에 있는 DART_API_KEY 값을 그대로 붙여넣기
  2) python generate_dart_corp_map.py 실행
  3) 생성된 dart_corp_code_map.json을 inventory_manager.py와 같은 폴더로 이동
  4) git add / commit / push

⚠️ 상장/상장폐지로 종목이 바뀌면 매핑이 오래된 정보가 될 수 있으니,
   가끔(예: 한 달에 한 번) 재실행해서 파일을 갱신해주는 게 좋다.
"""
import requests
import zipfile
import json
from io import BytesIO
import xml.etree.ElementTree as ET
import re

API_KEY = "fa3d35b617cf073f3d282d325c9271bc53d4a913"

def normalize_kr_code(code):
    return re.sub(r"\D", "", str(code)).zfill(6)[:6]

print("DART corpCode.xml 다운로드 중...")
res = requests.get(
    "https://opendart.fss.or.kr/api/corpCode.xml",
    params={"crtfc_key": API_KEY},
    timeout=15,
)
res.raise_for_status()

with zipfile.ZipFile(BytesIO(res.content)) as zf:
    xml_bytes = zf.read(zf.namelist()[0])

root = ET.fromstring(xml_bytes)
result = {}
for item in root.findall("list"):
    stock_code = (item.findtext("stock_code") or "").strip()
    corp_code = (item.findtext("corp_code") or "").strip()
    corp_name = (item.findtext("corp_name") or "").strip()
    if stock_code:
        result[normalize_kr_code(stock_code)] = {
            "corp_code": corp_code,
            "corp_name": corp_name,
        }

with open("dart_corp_code_map.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=1)

print(f"✅ 완료: 총 {len(result)}개 상장사 매핑을 dart_corp_code_map.json에 저장했습니다.")
print("   삼성전자(005930) 확인:", result.get("005930"))
print("\n다음 단계: dart_corp_code_map.json 파일을 inventory_manager.py와 같은 폴더로 옮기고 git commit/push 하세요.")
