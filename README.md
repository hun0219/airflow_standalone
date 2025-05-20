![image](https://github.com/user-attachments/assets/6acbf4b8-394d-49a3-945d-6c652b3e6987)

## movie 파이프라인
### branch: 240801/mv_summary
### DAG: movie
<img src="https://github.com/user-attachments/assets/88425369-e54e-49af-be0d-9be98a4f1ea5" width="1000"/>
KOBIS(영화진흥위원회) OpenAPI에서 일별 박스오피스 데이터를 가져와 다양한 조건(다양성영화, 국적)으로 저장하고, 최종 집계 데이터를 출력하는 워크플로우입니다.

#### 전체 동작 요약
- 실행 주기	매일 오전 2시 10분
- 데이터 출처	영화진흥위원회 OpenAPI
- 분기 처리	기존 데이터가 있을 경우 삭제 후 진행
- 저장 형식	Parquet, 파티셔닝: load_dt, multiMovieYn, repNationCd
- 환경 격리	모든 주요 작업은 PythonVirtualenvOperator 사용
- Git 연동	mov 패키지를 GitHub에서 불러와 사용 (requirements 지정)
- 키 관리	MOVIE_API_KEY는 .env, systemd, 또는 환경변수로 주입됨

#### DAG 흐름
```
start
  └── branch.op
        ├── (기존 데이터 있음) → rm.dir ─┐
        ├── (기존 데이터 없음) → echo.task ─┤
                                         └── get.start
                                               ├── multi.y
                                               ├── multi.n
                                               ├── nation.k
                                               └── nation.f
                                                    └── get.end
                                                          └── save.data
                                                                └── end
```
#### 주요 작업
1. branch.op (분기 판단)<br>
~/tmp/test_parquet/load_dt={날짜} 경로에 데이터가 존재하는지 판단<br>
존재 시 rm.dir (삭제), 아니면 get.start로 진입

3. 데이터 수집 (PythonVirtualenvOperator)<br>
공통 함수: common_get_data(ds_nodash, arg1)<br>
여러 조건별로 API 호출 후 Parquet 저장:<br>
multi.y: 다양성 영화 (multiMovieYn=Y)<br>
multi.n: 비다양성 영화 (multiMovieYn=N)<br>
nation.k: 한국 영화 (repNationCd=K)<br>
nation.f: 외국 영화 (repNationCd=F)<br>
각 저장 경로는 다음과 같이 파티셔닝됨:<br>
```
~/tmp/test_parquet/load_dt=YYYYMMDD/multiMovieYn=Y/xxx.parquet
~/tmp/test_parquet/load_dt=YYYYMMDD/repNationCd=K/xxx.parquet
```
3. save.data (집계)
apply_type2df()를 통해 parquet 파일 로드 및 숫자형 컬럼 변환<br>
openDt 기준으로 누적 관객 수 audiAcc 집계

#### 모듈 구성 (mov 패키지)
**save2df(load_dt, url_param)** <br>
&nbsp;&nbsp; API 응답 JSON → DataFrame 변환 후 load_dt 컬럼 추가<br>
&nbsp;&nbsp; parquet 저장은 DAG 내에서 수행<br>
**apply_type2df(load_dt)** <br>
&nbsp;&nbsp; 지정된 날짜의 parquet 파일 읽고, 숫자형 컬럼 변환<br>
**req2df, list2df** <br>
&nbsp;&nbsp; KOBIS API 호출 → JSON → 리스트 → DataFrame<br>

#### .parquet
<img src="https://github.com/user-attachments/assets/a0eff9a5-7a2a-4699-9421-2ebe80c882d9" width="500"/>
<img src="https://github.com/user-attachments/assets/c86fedd7-3f9b-4b13-94ab-d2b433a2c73a" width="1000"/>



----------------------------------------------------------------------------------------------------------------------------------------

## import 파이프라인
### branch: 240719/2CSV
### DAG: import_DB
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

