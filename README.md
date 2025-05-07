![image](https://github.com/user-attachments/assets/6acbf4b8-394d-49a3-945d-6c652b3e6987)

## DAG: import_db
![image](https://github.com/user-attachments/assets/b393a788-5e30-49e4-bfc9-c0ebbe94a299)
- 매일 주기로 자동 실행
- 과거 날짜 데이터 처리를 위해 catchup=True 설정
- 에러 발생 시 명확히 구분하여 에러 보고 및 처리
- 각 Task는 독립적이고 명확한 역할 분담
#### 전체 동작 요약
DAG는 매일 히스토리 로그데이터를 확인하고 CSV로 변환한 후, 임시 처리 및 데이터베이스(DB) 로딩을 거쳐 완료 상태를 생성하는 워크플로우
특정 작업(check)이 실패할 경우 에러를 처리하고 보고하는 과정 포함
#### DAG 흐름
```
start
  └── check
        ├── (성공) → to.csv → to.tmp → to.base → make.done ─→ end
        └── (실패) → err.report ─→ end
```
#### CSV 변환
<img src="https://github.com/user-attachments/assets/13034a39-03c9-4fe8-8529-69b702ac4d4d" width="200"/>

