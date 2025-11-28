# NoSQL-Studio

**NoSQL-Studio**는 NoSQL 데이터베이스(예: MongoDB, redis )를 위한 쿼리 및 관리 도구입니다. 파이썬으로 개발되었으며, 데이터베이스 구성 관리 및 다양한 쿼리 기능(기본/고급)을 제공합니다.

---

## 주요 파일

- `config_manager.py` : 데이터베이스 연결 설정 및 관리를 담당합니다.
- `db_query_tool.py` : 기본적인 NoSQL 쿼리 및 조작 기능을 제공합니다.
- `db_query_tool_advanced.py` : 고급 쿼리 및 데이터 처리 기능을 지원합니다.
- `requirements.txt` : 필요한 파이썬 패키지 목록입니다.
- `setup.bat` : 환경 설정 및 초기화 스크립트입니다.
- `run_basic.bat` : 기본 쿼리 툴 실행 스크립트입니다.
- `run.bat` : 고급 쿼리 툴 실행 스크립트입니다.

---

## 설치 및 실행 방법

1. **레포지토리 클론**
    ```bash
    git clone https://github.com/blue-code/NoSQL-Studio.git
    cd NoSQL-Studio
    ```

2. **필수 패키지 설치**
    ```bash
    pip install -r requirements.txt
    ```

3. **설정 및 실행**
    - 환경 설정: `setup.bat` 실행
    - 기본 쿼리 툴 실행: `run_basic.bat` 실행
    - 고급 쿼리 툴 실행: `run.bat` 실행

> **참고:** 사용 전 필요한 NoSQL 데이터베이스가 실행 중이어야 하며, 설정이 올바르게 되어 있어야 합니다.

---

## 기여 방법

- 버그 제보, 개선 사항 제안, Pull Request 환영합니다.

---

## 라이선스

- 현재 별도 명시된 라이선스는 없습니다.

---

**문의:** [https://github.com/blue-code/NoSQL-Studio](https://github.com/blue-code/NoSQL-Studio)