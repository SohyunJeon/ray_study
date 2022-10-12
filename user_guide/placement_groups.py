"""
사용자는 배치 그룹을 통해 여러개의 노드에 걸쳐 리소스 그롭을 원자단위로 예약할 수 있다.
PACK : task, actor를 지역에 최대한 가깝게 포장
SPREAD : task, actor를 지역서 분산

<Key concept>
- bundle
resource의 집합(i.e., {'GPU':4})
bundle은 ray 클러스터의 단일 노드에 맞아야 함
bundle들은 '배치 그룹 전략'에 따라 위치하게 된다

- Placement group
bundle의 집합
각 bundle은 배치 그룹 내에서 index를 부여받는다.
배치 그룹이 생성되면 task나 actor는 배치 그룹이나 개별 bundle에 따라 스케줄링된다.

"""


import ray
import time
ray.shutdown()
ray.init()


#%% Starting a placement group
