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
# 배치그룹은 ray.util.placement_group이나 PlacementGroups.createPlacementGroup으로 생성할 수 있다

from ray.util.placement_group import(
placement_group,
placement_group_table,
remove_placement_group
)

import ray
ray.init(num_gpus=2, resources={'extra_resource':2})
bundle1 = {'GPU':2}
bundle2 = {"extra_resource": 2}

pg = placement_group([bundle1, bundle2], strategy="STRICT_PACK")


# placement group이 생성될 때까지 대기
ray.get(pg.ready())

# 또는 ray.wait 사용 가능
ready, unready = ray.wait([pg.ready()], timeout=0)

# placement groupg의 상태 확인 가능
print(placement_group_table(pg))


#%% Strategy types
# -STRICT_PACK : 모든 bundle은 클러스터의 단일 노드에 있어야 한다
# -PACK : 모든 bundle은 가장 효율적인 단일 노드에 묶여 있어야 한다. 만약 strict packing이 가능한 경우가 아니라면
# (i.e., 특정 bundle이 특정 node에 맞지 않다면) 다른 노드에 위치되어야 한다.
# -STRICT_SPREAD : 각 bundle은 분리된 노드에 할당
# -SPREAD : 각 bundle은 가장 효율적인 개별 노드들에 할당됨. 만약 strick spread가 가능하지 않다면 bundle은 노드에 겹쳐서 배치됨

from pprint import pprint

from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
ray.shutdown()
ray.init(num_cpus=4, num_gpus=2, resources={"extra_resources": 2})

# bundle을 특정하고 싶다면..
# - "CPU": ray.remote의 num_cpus
# - "GPU": ray.remote의 num_gpus
# 다른 리소스는 ray.remote의 resources

ray.shutdown()
ray.init(num_cpus=2)

# placement group생성
pg = placement_group([{"CPU": 2}])
ray.get(pg.ready())

# 여기서 2 cpu는 더 이상 사용가능하지 않음. placement group에 의해 사전 예약됨
@ray.remote(num_cpus=2)
def f():
    return True

# 2 cpu가 없기 때문에 스케쥴링이 안됨
f.remote()

# 스케쥴 가능, 왜냐면 placement group이 2 cpu를 예약했기 때문
f.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(placement_group=pg)
).remote()

#%% Note
# placement group을 사용할 때 그것들이 준비되면 특정하는 것이 권장된다 (ray.get(pg.reday()) 호출)
# ray는 placement group이 적절히 생성됐을 것이라 생각하고 불가능한 task에 대해서 경고를 출력하지 않기 때

#%% GPU 사용

gpu_bundle = {"CPU": 2, "GPU": 2}
extra_resource_bundle = {"CPU":2, "extra_resource": 2}

pg = placement_group([gpu_bundle, extra_resource_bundle], strategy="STRICT_PACK")
ray.get(pg.ready())

@ray.remote(num_gpus=1)
class GPUActor:
    def __init__(self):
        pass

@ ray.remote(resources={"extra_resource": 1})
def extra_resource_task():
    import time
    time.sleep(10)

# gpu bundle에 gpu actor 생성
gpu_actors = [
    GPUActor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
            placement_group_bundle_index=0
        )
    ).remote() for _ in range(2)
]

# extra_resource bundle에 extra_resource actor 생성
extra_resource_actors = [
    extra_resource_task.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
            placement_group_bundle_index=1
        )
    ).remote() for _ in range(2)
]